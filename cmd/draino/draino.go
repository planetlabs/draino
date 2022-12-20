/*
Copyright 2018 Planet Labs Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing permissions
and limitations under the License.
*/

package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sort"
	"time"

	"github.com/DataDog/compute-go/kubeclient"
	"github.com/DataDog/compute-go/version"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	"github.com/spf13/cobra"

	"github.com/DataDog/compute-go/controllerruntime"
	"github.com/DataDog/compute-go/infraparameters"
	"k8s.io/apimachinery/pkg/runtime"

	"k8s.io/client-go/kubernetes/scheme"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"

	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
	core "k8s.io/api/core/v1"
	client "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	httptrace "gopkg.in/DataDog/dd-trace-go.v1/contrib/net/http"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	clientgoscheme "k8s.io/client-go/kubernetes/scheme"

	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/analyser"
	"github.com/planetlabs/draino/internal/kubernetes/drain"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	drainoklog "github.com/planetlabs/draino/internal/kubernetes/klog"
)

// Default leader election settings.
const (
	DefaultLeaderElectionLeaseDuration = 15 * time.Second
	DefaultLeaderElectionRenewDeadline = 10 * time.Second
	DefaultLeaderElectionRetryPeriod   = 2 * time.Second
)

func main() {
	tracer.Start(
		tracer.WithService("draino"),
	)
	defer tracer.Stop()
	mux := httptrace.NewServeMux()
	go http.ListenAndServe("localhost:8085", mux) // for go profiler

	// Read application flags
	options, fs := optionsFromFlags()
	config := &kubeclient.Config{}
	kubeclient.BindConfigToFlags(config, fs)

	root := &cobra.Command{
		Short:        "disruption-budget-manager",
		Long:         "disruption-budget-manager",
		SilenceUsage: true,
	}
	root.PersistentFlags().AddFlagSet(fs)
	root.AddCommand(version.NewCommand())

	root.RunE = func(cmd *cobra.Command, args []string) error {

		if errOptions := options.Validate(); errOptions != nil {
			return errOptions
		}

		log, err := zap.NewProduction()
		if err != nil {
			return err
		}
		if options.debug {
			log, err = zap.NewDevelopment()
		}

		drainoklog.InitializeKlog(options.klogVerbosity)
		drainoklog.RedirectToLogger(log)

		defer log.Sync() // nolint:errcheck // no check required on program exit

		DrainoLegacyMetrics(options, log)

		err = k8sclient.DecorateWithRateLimiter(config, "default")
		c, err := kubeclient.NewKubeConfig(config)
		if err != nil {
			return fmt.Errorf("failed create Kubernetes client configuration: %v", err)
		}

		cs, err := client.NewForConfig(c)
		if err != nil {
			return fmt.Errorf("failed create Kubernetes client: %v", err)
		}

		// use a Go context so we can tell the leaderelection and other pieces when we want to step down
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pods := kubernetes.NewPodWatch(ctx, cs)
		statefulSets := kubernetes.NewStatefulsetWatch(ctx, cs)
		persistentVolumes := kubernetes.NewPersistentVolumeWatch(ctx, cs)
		persistentVolumeClaims := kubernetes.NewPersistentVolumeClaimWatch(ctx, cs)
		runtimeObjectStoreImpl := &kubernetes.RuntimeObjectStoreImpl{
			StatefulSetsStore:          statefulSets,
			PodsStore:                  pods,
			PersistentVolumeStore:      persistentVolumes,
			PersistentVolumeClaimStore: persistentVolumeClaims,
		}

		// Sanitize user input
		sort.Strings(options.conditions)

		// Eviction Filtering
		pf := []kubernetes.PodFilterFunc{kubernetes.MirrorPodFilter}
		if !options.evictLocalStoragePods {
			pf = append(pf, kubernetes.LocalStoragePodFilter)
		}

		apiResources, err := kubernetes.GetAPIResourcesForGVK(cs, options.doNotEvictPodControlledBy, log)
		if err != nil {
			return fmt.Errorf("failed to get resources for controlby filtering for eviction: %v", err)
		}
		if len(apiResources) > 0 {
			for _, apiResource := range apiResources {
				if apiResource == nil {
					log.Info("Filtering pod that are uncontrolled for eviction")
				} else {
					log.Info("Filtering pods controlled by apiresource for eviction", zap.Any("apiresource", *apiResource))
				}
			}
			pf = append(pf, kubernetes.NewPodControlledByFilter(apiResources))
		}
		systemKnownAnnotations := []string{
			// https://github.com/kubernetes/autoscaler/blob/master/cluster-autoscaler/FAQ.md#what-types-of-pods-can-prevent-ca-from-removing-a-node
			"cluster-autoscaler.kubernetes.io/safe-to-evict=false",
		}
		pf = append(pf, kubernetes.UnprotectedPodFilter(runtimeObjectStoreImpl, false, append(systemKnownAnnotations, options.protectedPodAnnotations...)...))

		// Cordon Filtering
		podFilterCordon := []kubernetes.PodFilterFunc{}
		if !options.cordonLocalStoragePods {
			podFilterCordon = append(podFilterCordon, kubernetes.LocalStoragePodFilter)
		}
		apiResourcesCordon, err := kubernetes.GetAPIResourcesForGVK(cs, options.doNotCordonPodControlledBy, log)
		if err != nil {
			return fmt.Errorf("failed to get resources for 'controlledBy' filtering for cordon: %v", err)
		}
		if len(apiResourcesCordon) > 0 {
			for _, apiResource := range apiResourcesCordon {
				if apiResource == nil {
					log.Info("Filtering pods that are uncontrolled for cordon")
				} else {
					log.Info("Filtering pods controlled by apiresource for cordon", zap.Any("apiresource", *apiResource))
				}
			}
			podFilterCordon = append(podFilterCordon, kubernetes.NewPodControlledByFilter(apiResourcesCordon))
		}
		podFilterCordon = append(podFilterCordon, kubernetes.UnprotectedPodFilter(runtimeObjectStoreImpl, true, options.cordonProtectedPodAnnotations...))

		// Cordon limiter
		cordonLimiter := kubernetes.NewCordonLimiter(log)
		cordonLimiter.SetSkipLimiterSelector(options.skipCordonLimiterNodeAnnotationSelector)
		for p, f := range options.maxSimultaneousCordonFunctions {
			cordonLimiter.AddLimiter("MaxSimultaneousCordon:"+p, f)
		}
		for p, f := range options.maxSimultaneousCordonForLabelsFunctions {
			cordonLimiter.AddLimiter("MaxSimultaneousCordonLimiterForLabels:"+p, f)
		}
		for p, f := range options.maxSimultaneousCordonForTaintsFunctions {
			cordonLimiter.AddLimiter("MaxSimultaneousCordonLimiterForTaints:"+p, f)
		}
		globalLocker := kubernetes.NewGlobalBlocker(log)
		for p, f := range options.maxNotReadyNodesFunctions {
			globalLocker.AddBlocker("MaxNotReadyNodes:"+p, f(runtimeObjectStoreImpl, log), options.maxNotReadyNodesPeriod)
		}
		for p, f := range options.maxPendingPodsFunctions {
			globalLocker.AddBlocker("MaxPendingPods:"+p, f(runtimeObjectStoreImpl, log), options.maxPendingPodsPeriod)
		}

		for name, blockStateFunc := range globalLocker.GetBlockStateCacheAccessor() {
			localFunc := blockStateFunc
			cordonLimiter.AddLimiter(name, func(_ *core.Node, _, _ []*core.Node) (bool, error) { return !localFunc(), nil })
		}

		nodeReplacementLimiter := kubernetes.NewNodeReplacementLimiter(options.maxNodeReplacementPerHour, time.Now())

		b := record.NewBroadcaster()
		b.StartRecordingToSink(&typedcore.EventSinkImpl{Interface: typedcore.New(cs.CoreV1().RESTClient()).Events("")})
		k8sEventRecorder := b.NewRecorder(scheme.Scheme, core.EventSource{Component: kubernetes.Component})
		eventRecorder := kubernetes.NewEventRecorder(k8sEventRecorder)

		consolidatedOptInAnnotations := append(options.optInPodAnnotations, options.shortLivedPodAnnotations...)

		globalConfig := kubernetes.GlobalConfig{
			Context:                            ctx,
			ConfigName:                         options.configName,
			SuppliedConditions:                 options.suppliedConditions,
			PVCManagementEnableIfNoEvictionUrl: options.pvcManagementByDefault,
		}

		cordonDrainer := kubernetes.NewAPICordonDrainer(cs,
			eventRecorder,
			kubernetes.MaxGracePeriod(options.minEvictionTimeout),
			kubernetes.EvictionHeadroom(options.evictionHeadroom),
			kubernetes.WithSkipDrain(options.skipDrain),
			kubernetes.WithPodFilter(
				kubernetes.NewPodFiltersIgnoreCompletedPods(
					kubernetes.NewPodFiltersIgnoreShortLivedPods(
						kubernetes.NewPodFiltersWithOptInFirst(kubernetes.PodOrControllerHasAnyOfTheAnnotations(runtimeObjectStoreImpl, consolidatedOptInAnnotations...), kubernetes.NewPodFilters(pf...)),
						runtimeObjectStoreImpl, options.shortLivedPodAnnotations...))),
			kubernetes.WithCordonLimiter(cordonLimiter),
			kubernetes.WithNodeReplacementLimiter(nodeReplacementLimiter),
			kubernetes.WithStorageClassesAllowingDeletion(options.storageClassesAllowingVolumeDeletion),
			kubernetes.WithMaxDrainAttemptsBeforeFail(options.maxDrainAttemptsBeforeFail),
			kubernetes.WithGlobalConfig(globalConfig),
			kubernetes.WithAPICordonDrainerLogger(log),
		)

		podFilteringFunc := kubernetes.NewPodFiltersIgnoreCompletedPods(
			kubernetes.NewPodFiltersWithOptInFirst(
				kubernetes.PodOrControllerHasAnyOfTheAnnotations(runtimeObjectStoreImpl, consolidatedOptInAnnotations...), kubernetes.NewPodFilters(podFilterCordon...)))

		var h cache.ResourceEventHandler = kubernetes.NewDrainingResourceEventHandler(
			cs,
			cordonDrainer,
			runtimeObjectStoreImpl,
			eventRecorder,
			kubernetes.WithLogger(log),
			kubernetes.WithDrainBuffer(options.drainBuffer),
			kubernetes.WithSchedulingBackoffDelay(options.schedulingRetryBackoffDelay),
			kubernetes.WithDurationWithCompletedStatusBeforeReplacement(options.durationBeforeReplacement),
			kubernetes.WithDrainGroups(options.drainGroupLabelKey),
			kubernetes.WithGlobalConfigHandler(globalConfig),
			kubernetes.WithCordonPodFilter(podFilteringFunc),
			kubernetes.WithGlobalBlocking(globalLocker),
			kubernetes.WithPreprovisioningConfiguration(kubernetes.NodePreprovisioningConfiguration{Timeout: options.preprovisioningTimeout, CheckPeriod: options.preprovisioningCheckPeriod, AllNodesByDefault: options.preprovisioningActivatedByDefault}))

		if options.dryRun {
			h = cache.FilteringResourceEventHandler{
				FilterFunc: kubernetes.NewNodeProcessed().Filter,
				Handler: kubernetes.NewDrainingResourceEventHandler(
					cs,
					&kubernetes.NoopCordonDrainer{},
					runtimeObjectStoreImpl,
					eventRecorder,
					kubernetes.WithLogger(log),
					kubernetes.WithDrainBuffer(options.drainBuffer),
					kubernetes.WithSchedulingBackoffDelay(options.schedulingRetryBackoffDelay),
					kubernetes.WithDurationWithCompletedStatusBeforeReplacement(options.durationBeforeReplacement),
					kubernetes.WithDrainGroups(options.drainGroupLabelKey),
					kubernetes.WithGlobalBlocking(globalLocker),
					kubernetes.WithGlobalConfigHandler(globalConfig)),
			}
		}

		if len(options.nodeLabels) > 0 {
			log.Info("node labels", zap.Any("labels", options.nodeLabels))
			if options.nodeLabelsExpr != "" {
				return fmt.Errorf("nodeLabels and NodeLabelsExpr cannot both be set")
			}
			if ptrStr, err := kubernetes.ConvertLabelsToFilterExpr(options.nodeLabels); err != nil {
				return err
			} else {
				options.nodeLabelsExpr = *ptrStr
			}
		}

		var nodeLabelFilter cache.ResourceEventHandler
		log.Debug("label expression", zap.Any("expr", options.nodeLabelsExpr))

		nodeLabelFilterFunc, err := kubernetes.NewNodeLabelFilter(options.nodeLabelsExpr, log)
		if err != nil {
			log.Sugar().Fatalf("Failed to parse node label expression: %v", err)
		}

		nodeLabelFilter = cache.FilteringResourceEventHandler{FilterFunc: nodeLabelFilterFunc, Handler: h}
		nodes := kubernetes.NewNodeWatch(ctx, cs, nodeLabelFilter)
		runtimeObjectStoreImpl.NodesStore = nodes
		// storeCloserFunc := runtimeObjectStoreImpl.Run(log)
		// defer storeCloserFunc()
		cordonLimiter.SetNodeLister(nodes)
		cordonDrainer.SetRuntimeObjectStore(runtimeObjectStoreImpl)

		id, err := os.Hostname()
		if err != nil {
			return fmt.Errorf("failed to get hostname: %v", err)
		}

		scopeObserver := kubernetes.NewScopeObserver(cs, globalConfig, runtimeObjectStoreImpl, options.scopeAnalysisPeriod, podFilteringFunc,
			kubernetes.PodOrControllerHasAnyOfTheAnnotations(runtimeObjectStoreImpl, options.optInPodAnnotations...),
			kubernetes.PodOrControllerHasAnyOfTheAnnotations(runtimeObjectStoreImpl, options.cordonProtectedPodAnnotations...),
			nodeLabelFilterFunc, log)
		go scopeObserver.Run(ctx.Done())
		if options.resetScopeLabel == true {
			go scopeObserver.Reset()
		}

		lock, err := resourcelock.New(
			resourcelock.EndpointsLeasesResourceLock,
			options.namespace,
			options.leaderElectionTokenName,
			cs.CoreV1(),
			cs.CoordinationV1(),
			resourcelock.ResourceLockConfig{
				Identity:      id,
				EventRecorder: k8sEventRecorder,
			},
		)
		if err != nil {
			return fmt.Errorf("failed to create lock: %v", err)
		}

		leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
			Lock:          lock,
			LeaseDuration: options.leaderElectionLeaseDuration,
			RenewDeadline: options.leaderElectionRenewDeadline,
			RetryPeriod:   options.leaderElectionRetryPeriod,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(ctx context.Context) {
					log.Info("watchers are running")
					if errLE := kubernetes.Await(nodes, pods, statefulSets, persistentVolumes, persistentVolumeClaims, globalLocker); errLE != nil {
						panic("leader election, error watching: " + errLE.Error())
					}

				},
				OnStoppedLeading: func() {
					panic("lost leader election")
				},
			},
		})
		return nil
	}

	err := root.Execute()
	_ = zap.L().Sync()
	if err != nil {
		zap.L().Fatal("Program exit on error", zap.Error(err))
	}

}

type httpRunner struct {
	address string
	logger  *zap.Logger
	h       map[string]http.Handler
}

func (r *httpRunner) Run(stop <-chan struct{}) {
	rt := httprouter.New()
	for path, handler := range r.h {
		rt.Handler("GET", path, handler)
	}

	s := &http.Server{Addr: r.address, Handler: rt}
	ctx, cancel := context.WithTimeout(context.Background(), 0*time.Second)
	go func() {
		<-stop
		if err := s.Shutdown(ctx); err != nil {
			r.logger.Error("Failed to shutdown httpRunner", zap.Error(err))
			return
		}
	}()
	if err := s.ListenAndServe(); err != nil {
		r.logger.Error("Failed to ListenAndServe httpRunner", zap.Error(err))
	}
	cancel()
}

// controllerRuntimeBootstrap This function is not called, it is just there to prepare the ground in terms of dependencies for next step where we will include ControllerRuntime library
func controllerRuntimeBootstrap() {
	cfg, fs := controllerruntime.ConfigFromFlags(false, false)
	if err := fs.Parse(os.Args[1:]); err != nil {
		fmt.Printf("error getting arguments: %v\n", err)
		os.Exit(1)
	}
	cfg.InfraParam.UpdateWithKubeContext(cfg.KubeClientConfig.ConfigFile, "")
	validationOptions := infraparameters.GetValidateAll()
	validationOptions.Datacenter, validationOptions.CloudProvider, validationOptions.CloudProviderProject = false, false, false
	if err := cfg.InfraParam.Validate(validationOptions); err != nil {
		fmt.Printf("infra param validation error: %v\n", err)
		os.Exit(1)
	}

	cfg.ManagerOptions.Scheme = runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(cfg.ManagerOptions.Scheme); err != nil {
		fmt.Printf("error while adding client-go scheme: %v\n", err)
		os.Exit(1)
	}
	if err := core.AddToScheme(cfg.ManagerOptions.Scheme); err != nil {
		fmt.Printf("error while adding v1 scheme: %v\n", err)
		os.Exit(1)
	}

	mgr, logger, _, err := controllerruntime.NewManager(cfg)
	if err != nil {
		fmt.Printf("error while creating manager: %v\n", err)
		os.Exit(1)
	}

	indexer, err := index.New(mgr.GetClient(), mgr.GetCache(), logger)
	if err != nil {
		fmt.Printf("error while initializing informer: %v\n", err)
		os.Exit(1)
	}

	// just to consume analyzer
	_ = analyser.NewPDBAnalyser(indexer)
	_ = drain.NewDrainSimulator(
		context.Background(),
		mgr.GetClient(),
		indexer,
		func(p core.Pod) (pass bool, reason string, err error) { return true, "", nil },
	)
	logger.Info("ControllerRuntime bootstrap")
}
