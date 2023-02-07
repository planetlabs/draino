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
	"strings"
	"time"

	"github.com/planetlabs/draino/internal/diagnostics"

	"github.com/DataDog/compute-go/controllerruntime"
	"github.com/DataDog/compute-go/infraparameters"
	"github.com/DataDog/compute-go/kubeclient"
	"github.com/DataDog/compute-go/service"
	"github.com/DataDog/compute-go/version"
	httptrace "gopkg.in/DataDog/dd-trace-go.v1/contrib/net/http"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/julienschmidt/httprouter"
	"github.com/planetlabs/draino/internal/candidate_runner"
	"github.com/planetlabs/draino/internal/candidate_runner/filters"
	"github.com/planetlabs/draino/internal/candidate_runner/sorters"
	"github.com/planetlabs/draino/internal/cli"
	drainbuffer "github.com/planetlabs/draino/internal/drain_buffer"
	"github.com/planetlabs/draino/internal/drain_runner"
	preprocessor "github.com/planetlabs/draino/internal/drain_runner/pre_processor"
	"github.com/planetlabs/draino/internal/groups"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/analyser"
	"github.com/planetlabs/draino/internal/kubernetes/drain"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	drainoklog "github.com/planetlabs/draino/internal/kubernetes/klog"
	"github.com/planetlabs/draino/internal/limit"
	"github.com/planetlabs/draino/internal/observability"
	protector "github.com/planetlabs/draino/internal/protector"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	client "k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Default leader election settings.
const (
	DefaultLeaderElectionLeaseDuration = 15 * time.Second
	DefaultLeaderElectionRenewDeadline = 10 * time.Second
	DefaultLeaderElectionRetryPeriod   = 2 * time.Second
)

func main() {

	// Read application flags
	cfg, fs := controllerruntime.ConfigFromFlags(false, false)
	cliHandlers := &cli.CLIHandlers{}
	cliCommands := &cli.CLICommands{ServerAddr: &cfg.Service.ServiceAddr}

	cfg.Service.RouteRegisterFunc = cliHandlers.RegisterRoute
	options, optFlags := optionsFromFlags()
	fs.AddFlagSet(optFlags)

	root := &cobra.Command{
		Short:        "disruption-budget-manager",
		Long:         "disruption-budget-manager",
		SilenceUsage: true,
	}
	root.PersistentFlags().AddFlagSet(fs)
	root.AddCommand(version.NewCommand())
	root.AddCommand(service.NewLogLevelCommand())
	root.AddCommand(cliCommands.Commands()...)

	root.RunE = func(cmd *cobra.Command, args []string) error {

		if errOptions := options.Validate(); errOptions != nil {
			return errOptions
		}

		zapConfig := zap.NewProductionConfig()
		zapConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		log, err := zapConfig.Build()
		if err != nil {
			return err
		}
		if options.debug {
			log, err = zap.NewDevelopment()
		}
		drainoklog.InitializeKlog(options.klogVerbosity)
		drainoklog.RedirectToLogger(log)

		defer log.Sync() // nolint:errcheck // no check required on program exit

		go launchTracerAndProfiler()

		DrainoLegacyMetrics(options, log)

		cs, err2 := GetKubernetesClientSet(&cfg.KubeClientConfig)
		if err2 != nil {
			return err2
		}

		// use a Go context so we can tell the leaderelection and other pieces when we want to step down
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pods := kubernetes.NewPodWatch(ctx, cs)
		statefulSets := kubernetes.NewStatefulsetWatch(ctx, cs)
		deployments := kubernetes.NewDeploymentWatch(ctx, cs)
		persistentVolumes := kubernetes.NewPersistentVolumeWatch(ctx, cs)
		persistentVolumeClaims := kubernetes.NewPersistentVolumeClaimWatch(ctx, cs)
		runtimeObjectStoreImpl := &kubernetes.RuntimeObjectStoreImpl{
			DeploymentStore:            deployments,
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

		// To maintain compatibility with draino v1 version we have to exclude pods from STS running on node without local-storage
		if options.excludeStatefulSetOnNodeWithoutStorage {
			podFilterCordon = append(podFilterCordon, kubernetes.NewPodFiltersNoStatefulSetOnNodeWithoutDisk(runtimeObjectStoreImpl))
		}

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

		eventRecorder, k8sEventRecorder := kubernetes.BuildEventRecorderWithAggregationOnEventType(zapr.NewLogger(log), cs, options.eventAggregationPeriod, options.excludedPodsPerNodeEstimation, options.logEvents)
		eventRecorderForDrainerActivities, _ := kubernetes.BuildEventRecorderWithAggregationOnEventTypeAndMessage(zapr.NewLogger(log), cs, options.eventAggregationPeriod, options.logEvents)

		consolidatedOptInAnnotations := append(options.optInPodAnnotations, options.shortLivedPodAnnotations...)

		globalConfig := kubernetes.GlobalConfig{
			Context:                            ctx,
			ConfigName:                         options.configName,
			SuppliedConditions:                 options.suppliedConditions,
			PVCManagementEnableIfNoEvictionUrl: options.pvcManagementByDefault,
		}

		drainerSkipPodFilter := kubernetes.NewPodFiltersIgnoreCompletedPods(
			kubernetes.NewPodFiltersIgnoreShortLivedPods(
				kubernetes.NewPodFiltersWithOptInFirst(kubernetes.PodOrControllerHasAnyOfTheAnnotations(runtimeObjectStoreImpl, consolidatedOptInAnnotations...), kubernetes.NewPodFilters(pf...)),
				runtimeObjectStoreImpl, options.shortLivedPodAnnotations...))

		cordonDrainer := kubernetes.NewAPICordonDrainer(cs,
			eventRecorderForDrainerActivities,
			kubernetes.MaxGracePeriod(options.minEvictionTimeout),
			kubernetes.EvictionHeadroom(options.evictionHeadroom),
			kubernetes.WithSkipDrain(options.skipDrain),
			kubernetes.WithPodFilter(drainerSkipPodFilter),
			kubernetes.WithCordonLimiter(cordonLimiter),
			kubernetes.WithNodeReplacementLimiter(nodeReplacementLimiter),
			kubernetes.WithStorageClassesAllowingDeletion(options.storageClassesAllowingVolumeDeletion),
			kubernetes.WithMaxDrainAttemptsBeforeFail(options.maxDrainAttemptsBeforeFail),
			kubernetes.WithGlobalConfig(globalConfig),
			kubernetes.WithAPICordonDrainerLogger(log),
		)

		// TODO do analysis and check if   drainerSkipPodFilter = NewPodFiltersIgnoreShortLivedPods(cordonPodFilteringFunc) ?
		cordonPodFilteringFunc := kubernetes.NewPodFiltersIgnoreCompletedPods(
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
			kubernetes.WithCordonPodFilter(cordonPodFilteringFunc),
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

		if options.noLegacyNodeHandler {
			nodeLabelFilter = cache.FilteringResourceEventHandler{FilterFunc: func(obj interface{}) bool {
				return false
			}, Handler: nil}
		} else {
			nodeLabelFilter = cache.FilteringResourceEventHandler{FilterFunc: nodeLabelFilterFunc, Handler: h}
		}
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

		lock, err := resourcelock.New(
			resourcelock.EndpointsLeasesResourceLock,
			cfg.InfraParam.Namespace,
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

		filters := filtersDefinitions{
			cordonPodFilter: cordonPodFilteringFunc,
			drainPodFilter:  drainerSkipPodFilter,
			nodeLabelFilter: nodeLabelFilterFunc,
		}
		if err = controllerRuntimeBootstrap(options, cfg, cordonDrainer, filters, runtimeObjectStoreImpl, globalConfig, log, cliHandlers, globalLocker); err != nil {
			return fmt.Errorf("failed to bootstrap the controller runtime section: %v", err)
		}

		leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
			Lock:          lock,
			LeaseDuration: options.leaderElectionLeaseDuration,
			RenewDeadline: options.leaderElectionRenewDeadline,
			RetryPeriod:   options.leaderElectionRetryPeriod,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(ctx context.Context) {
					log.Info("watchers are running")
					if errLE := kubernetes.Await(nodes, pods, statefulSets, deployments, persistentVolumes, persistentVolumeClaims); errLE != nil {
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

// launchTracerAndProfiler will initialize and run the tracer and the endpoint for the profiler
// the function is blocking launch it in a dedicated go-routine
func launchTracerAndProfiler() {
	tracer.Start(
		tracer.WithService("draino"),
	)
	defer tracer.Stop()
	mux := httptrace.NewServeMux()
	http.ListenAndServe("localhost:8085", mux) // for go profiler
}

func GetKubernetesClientSet(config *kubeclient.Config) (*client.Clientset, error) {
	if err := k8sclient.DecorateWithRateLimiter(config, "default"); err != nil {
		return nil, err
	}
	c, err := kubeclient.NewKubeConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed create Kubernetes client configuration: %v", err)
	}

	cs, err := client.NewForConfig(c)
	if err != nil {
		return nil, fmt.Errorf("failed create Kubernetes client: %v", err)
	}
	return cs, nil
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

// TODO should we put this in globalConfig ?
type filtersDefinitions struct {
	cordonPodFilter kubernetes.PodFilterFunc
	drainPodFilter  kubernetes.PodFilterFunc

	nodeLabelFilter kubernetes.NodeLabelFilterFunc
}

// getInitDrainBufferRunner returns a Runnable that is responsible for initializing the drain buffer
func getInitDrainBufferRunner(drainBuffer drainbuffer.DrainBuffer, logger *logr.Logger) manager.Runnable {
	return &RunTillSuccess{
		logger: logger,
		period: time.Second,
		fn:     func(ctx context.Context) error { return drainBuffer.Initialize(ctx) },
	}
}

// controllerRuntimeBootstrap This function is not called, it is just there to prepare the ground in terms of dependencies for next step where we will include ControllerRuntime library
func controllerRuntimeBootstrap(options *Options, cfg *controllerruntime.Config, drainer kubernetes.Drainer, filtersDef filtersDefinitions, store kubernetes.RuntimeObjectStore, globalConfig kubernetes.GlobalConfig, zlog *zap.Logger, cliHandlers *cli.CLIHandlers, globalBlocker kubernetes.GlobalBlocker) error {
	validationOptions := infraparameters.GetValidateAll()
	validationOptions.Datacenter, validationOptions.CloudProvider, validationOptions.CloudProviderProject, validationOptions.KubeClusterName = false, false, false, false
	if err := cfg.InfraParam.Validate(validationOptions); err != nil {
		return fmt.Errorf("infra param validation error: %v\n", err)
	}

	cfg.ManagerOptions.Scheme = runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(cfg.ManagerOptions.Scheme); err != nil {
		return fmt.Errorf("error while adding client-go scheme: %v\n", err)
	}
	if err := core.AddToScheme(cfg.ManagerOptions.Scheme); err != nil {
		return fmt.Errorf("error while adding v1 scheme: %v\n", err)
	}

	mgr, logger, _, err := controllerruntime.NewManager(cfg)
	if err != nil {
		return fmt.Errorf("error while creating manager: %v\n", err)
	}

	ctx := context.Background()
	indexer, err := index.New(ctx, mgr.GetClient(), mgr.GetCache(), logger)
	if err != nil {
		return fmt.Errorf("error while initializing informer: %v\n", err)
	}

	staticRetryStrategy := &drain.StaticRetryStrategy{
		AlertThreashold: 7,
		Delay:           options.schedulingRetryBackoffDelay,
	}
	retryWall, errRW := drain.NewRetryWall(mgr.GetClient(), mgr.GetLogger(), staticRetryStrategy)
	if errRW != nil {
		return errRW
	}

	cs, err := GetKubernetesClientSet(&cfg.KubeClientConfig)
	if err != nil {
		return err
	}

	kubeVersion, err := cs.ServerVersion()
	if err != nil {
		return err
	}

	eventRecorder, _ := kubernetes.BuildEventRecorderWithAggregationOnEventType(logger, cs, options.eventAggregationPeriod, options.excludedPodsPerNodeEstimation, options.logEvents)
	eventRecorderForDrainRunnerActivities, _ := kubernetes.BuildEventRecorderWithAggregationOnEventTypeAndMessage(logger, cs, options.eventAggregationPeriod, options.logEvents)

	pvProtector := protector.NewPVCProtector(store, zlog, globalConfig.PVCManagementEnableIfNoEvictionUrl)
	stabilityPeriodChecker := analyser.NewStabilityPeriodChecker(ctx, logger, mgr.GetClient(), nil, store, indexer, analyser.StabilityPeriodCheckerConfiguration{}, filtersDef.drainPodFilter)

	persistor := drainbuffer.NewConfigMapPersistor(cs.CoreV1().ConfigMaps(cfg.InfraParam.Namespace), options.drainBufferConfigMapName, cfg.InfraParam.Namespace)
	drainBuffer := drainbuffer.NewDrainBuffer(ctx, persistor, clock.RealClock{}, mgr.GetLogger(), eventRecorder, indexer, store, options.drainBuffer)
	// The drain buffer can only be initialized when the manager client cache was started.
	// Adding a custom runnable to the controller manager will make sure, that the initialization will be started as soon as possible.
	if err := mgr.Add(getInitDrainBufferRunner(drainBuffer, &logger)); err != nil {
		logger.Error(err, "cannot setup drain buffer initialization runnable")
		return err
	}

	keyGetter := groups.NewGroupKeyFromNodeMetadata(mgr.GetClient(), mgr.GetLogger(), eventRecorder, indexer, store, strings.Split(options.drainGroupLabelKey, ","), []string{kubernetes.DrainGroupAnnotation}, kubernetes.DrainGroupOverrideAnnotation)

	filterFactory, err := filters.NewFactory(
		filters.WithLogger(mgr.GetLogger()),
		filters.WithRetryWall(retryWall),
		filters.WithRuntimeObjectStore(store),
		filters.WithCordonPodFilter(filtersDef.cordonPodFilter),
		filters.WithNodeLabelsFilterFunction(filtersDef.nodeLabelFilter),
		filters.WithGlobalConfig(globalConfig),
		filters.WithStabilityPeriodChecker(stabilityPeriodChecker),
		filters.WithDrainBuffer(drainBuffer),
		filters.WithGroupKeyGetter(keyGetter),
		filters.WithGlobalBlocker(globalBlocker),
		filters.WithEventRecorder(eventRecorder),
		filters.WithPVCProtector(pvProtector),
	)
	if err != nil {
		logger.Error(err, "failed to configure the filters")
		return err
	}

	pdbAnalyser := analyser.NewPDBAnalyser(ctx, mgr.GetLogger(), indexer, clock.RealClock{}, options.podWarmupDelayExtension)

	drainRunnerFactory, err := drain_runner.NewFactory(
		drain_runner.WithKubeClient(mgr.GetClient()),
		drain_runner.WithClock(&clock.RealClock{}),
		drain_runner.WithDrainer(drainer),
		drain_runner.WithPreprocessors(
			preprocessor.NewWaitTimePreprocessor(options.waitBeforeDraining),
			preprocessor.NewNodeReplacementPreProcessor(mgr.GetClient(), options.preprovisioningActivatedByDefault, mgr.GetLogger()),
			preprocessor.NewPreActivitiesPreProcessor(mgr.GetClient(), indexer, store, mgr.GetLogger(), eventRecorderForDrainRunnerActivities, clock.RealClock{}, options.preActivityDefaultTimeout),
		),
		drain_runner.WithRerun(options.groupRunnerPeriod),
		drain_runner.WithRetryWall(retryWall),
		drain_runner.WithLogger(mgr.GetLogger()),
		drain_runner.WithSharedIndexInformer(indexer),
		drain_runner.WithEventRecorder(eventRecorderForDrainRunnerActivities),
		drain_runner.WithFilter(filterFactory.BuildCandidateFilter()),
		drain_runner.WithDrainBuffer(drainBuffer),
		drain_runner.WithGlobalConfig(globalConfig),
	)
	if err != nil {
		logger.Error(err, "failed to configure the drain_runner")
		return err
	}

	simulationPodFilter := kubernetes.NewPodFilters(filtersDef.drainPodFilter, kubernetes.PodOrControllerHasNoneOfTheAnnotations(store, kubernetes.EvictionAPIURLAnnotationKey))
	simulationRateLimiter := limit.NewRateLimiter(clock.RealClock{}, cfg.KubeClientConfig.QPS*options.simulationRateLimitingRatio, int(float32(cfg.KubeClientConfig.Burst)*options.simulationRateLimitingRatio))
	simulator := drain.NewDrainSimulator(context.Background(), mgr.GetClient(), indexer, simulationPodFilter, kubeVersion, eventRecorder, simulationRateLimiter, logger)
	sorters := candidate_runner.NodeSorters{
		sorters.CompareNodeAnnotationDrainPriority,
		sorters.NewConditionComparator(globalConfig.SuppliedConditions),
		pdbAnalyser.CompareNode,
	}

	drainCandidateRunnerFactory, err := candidate_runner.NewFactory(
		candidate_runner.WithKubeClient(mgr.GetClient()),
		candidate_runner.WithClock(&clock.RealClock{}),
		candidate_runner.WithRerun(options.groupRunnerPeriod),
		candidate_runner.WithLogger(mgr.GetLogger()),
		candidate_runner.WithSharedIndexInformer(indexer),
		candidate_runner.WithEventRecorder(eventRecorder),
		candidate_runner.WithMaxSimultaneousCandidates(1), // TODO should we move that to something that can be customized per user
		candidate_runner.WithFilter(filterFactory.BuildCandidateFilter()),
		candidate_runner.WithDrainSimulator(simulator),
		candidate_runner.WithNodeSorters(sorters),
		candidate_runner.WithDryRun(options.dryRun),
		candidate_runner.WithRetryWall(retryWall),
		candidate_runner.WithRateLimiter(limit.NewTypedRateLimiter(&clock.RealClock{}, kubernetes.GetRateLimitConfiguration(globalConfig.SuppliedConditions), options.drainRateLimitQPS, options.drainRateLimitBurst)),
		candidate_runner.WithGlobalConfig(globalConfig),
	)
	if err != nil {
		logger.Error(err, "failed to configure the candidate_runner")
		return err
	}

	groupRegistry := groups.NewGroupRegistry(ctx, mgr.GetClient(), mgr.GetLogger(), eventRecorder, keyGetter, drainRunnerFactory, drainCandidateRunnerFactory, filtersDef.nodeLabelFilter, store.HasSynced, options.groupRunnerPeriod)
	if err = groupRegistry.SetupWithManager(mgr); err != nil {
		logger.Error(err, "failed to setup groupRegistry")
		return err
	}
	groupFromPod := groups.NewGroupFromPod(mgr.GetClient(), mgr.GetLogger(), keyGetter, filtersDef.drainPodFilter, store.HasSynced)
	if err = groupFromPod.SetupWithManager(mgr); err != nil {
		logger.Error(err, "failed to setup groupFromPod")
		return err
	}

	diagnosticFactory, err := diagnostics.NewFactory(
		diagnostics.WithKubeClient(mgr.GetClient()),
		diagnostics.WithClock(&clock.RealClock{}),
		diagnostics.WithLogger(mgr.GetLogger()),
		diagnostics.WithFilter(filterFactory.BuildCandidateFilter()),
		diagnostics.WithDrainSimulator(simulator),
		diagnostics.WithNodeSorters(sorters),
		diagnostics.WithRetryWall(retryWall),
		diagnostics.WithDrainBuffer(drainBuffer),
		diagnostics.WithGlobalConfig(globalConfig),
		diagnostics.WithKeyGetter(keyGetter),
		diagnostics.WithStabilityPeriodChecker(stabilityPeriodChecker),
	)
	if err != nil {
		logger.Error(err, "failed to configure the diagnostics")
		return err
	}

	nodeDiagnostician := diagnosticFactory.BuildDiagnostician()
	diagnostics := diagnostics.NewDiagnosticsController(ctx, mgr.GetClient(), mgr.GetLogger(), eventRecorder, []diagnostics.Diagnostician{nodeDiagnostician}, store.HasSynced)
	if err = diagnostics.SetupWithManager(mgr); err != nil {
		logger.Error(err, "failed to setup diagnostics")
		return err
	}

	if errCli := cliHandlers.Initialize(logger, groupRegistry, drainCandidateRunnerFactory.BuildCandidateInfo(), drainRunnerFactory.BuildRunner(), nodeDiagnostician); errCli != nil {
		logger.Error(errCli, "Failed to initialize CLIHandlers")
		return errCli
	}

	scopeObserver := observability.NewScopeObserver(cs, globalConfig, store, options.scopeAnalysisPeriod, filtersDef.cordonPodFilter,
		kubernetes.PodOrControllerHasAnyOfTheAnnotations(store, options.optInPodAnnotations...),
		kubernetes.PodOrControllerHasAnyOfTheAnnotations(store, options.cordonProtectedPodAnnotations...),
		filtersDef.nodeLabelFilter, zlog, retryWall, keyGetter, groupRegistry)

	if options.resetScopeLabel == true {
		err = mgr.Add(&RunOnce{fn: func(context.Context) error { scopeObserver.Reset(); return nil }})
		if err != nil {
			logger.Error(err, "failed to attach scope observer cleanup")
			return err
		}
	}

	if err := mgr.Add(globalBlocker); err != nil {
		logger.Error(err, "failed to setup global blocker with controller runtime")
		return err
	}

	if err := mgr.Add(scopeObserver); err != nil {
		logger.Error(err, "failed to setup scope observer with controller runtime")
		return err
	}

	logger.Info("ControllerRuntime bootstrap done, running the manager")
	// Starting Manager
	go func() {
		logger.Info("Starting manager")
		if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
			logger.Error(err, "Controller Manager did exit with error")
			panic("Manager finished with error: " + err.Error()) // TODO remove this that is purely for testing and identifying an early exit of the code
		}
		logger.Info("Manager finished without error")
		panic("Manager finished normally") // TODO remove this that is purely for testing and identifying an early exit of the code
	}()

	return nil
}
