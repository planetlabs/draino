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
	"github.com/DataDog/compute-go/kubeclient"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sort"
	"time"

	"github.com/DataDog/compute-go/controllerruntime"
	"github.com/DataDog/compute-go/infraparameters"
	"k8s.io/apimachinery/pkg/runtime"

	"k8s.io/client-go/kubernetes/scheme"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"

	"contrib.go.opencensus.io/exporter/prometheus"
	"github.com/julienschmidt/httprouter"
	prom "github.com/prometheus/client_golang/prometheus"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"go.uber.org/zap"
	"gopkg.in/alecthomas/kingpin.v2"
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

	fmt.Println(os.Args[1:])

	if err := fs.Parse(os.Args[1:]); err != nil {
		fmt.Printf("error getting arguments: %v\n", err)
		os.Exit(1)
	}

	if errOptions := options.Validate(); errOptions != nil {
		panic(errOptions)
	}

	var (
		nodesCordoned = &view.View{
			Name:        "cordoned_nodes_total",
			Measure:     kubernetes.MeasureNodesCordoned,
			Description: "Number of nodes cordoned.",
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{kubernetes.TagResult, kubernetes.TagConditions, kubernetes.TagNodegroupName, kubernetes.TagNodegroupNamePrefix, kubernetes.TagNodegroupNamespace, kubernetes.TagTeam},
		}
		nodesUncordoned = &view.View{
			Name:        "uncordoned_nodes_total",
			Measure:     kubernetes.MeasureNodesUncordoned,
			Description: "Number of nodes uncordoned.",
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{kubernetes.TagResult},
		}
		nodesDrained = &view.View{
			Name:        "drained_nodes_total",
			Measure:     kubernetes.MeasureNodesDrained,
			Description: "Number of nodes drained.",
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{kubernetes.TagResult, kubernetes.TagFailureCause, kubernetes.TagConditions, kubernetes.TagNodegroupName, kubernetes.TagNodegroupNamePrefix, kubernetes.TagNodegroupNamespace, kubernetes.TagTeam},
		}
		nodesDrainScheduled = &view.View{
			Name:        "drain_scheduled_nodes_total",
			Measure:     kubernetes.MeasureNodesDrainScheduled,
			Description: "Number of nodes scheduled for drain.",
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{kubernetes.TagResult, kubernetes.TagConditions, kubernetes.TagNodegroupName, kubernetes.TagNodegroupNamePrefix, kubernetes.TagNodegroupNamespace, kubernetes.TagTeam},
		}
		limitedCordon = &view.View{
			Name:        "limited_cordon_total",
			Measure:     kubernetes.MeasureLimitedCordon,
			Description: "Number of limited cordon encountered.",
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{kubernetes.TagReason, kubernetes.TagConditions, kubernetes.TagNodegroupName, kubernetes.TagNodegroupNamePrefix, kubernetes.TagNodegroupNamespace, kubernetes.TagTeam},
		}
		skippedCordon = &view.View{
			Name:        "skipped_cordon_total",
			Measure:     kubernetes.MeasureSkippedCordon,
			Description: "Number of skipped cordon encountered.",
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{kubernetes.TagReason, kubernetes.TagConditions, kubernetes.TagNodegroupName, kubernetes.TagNodegroupNamePrefix, kubernetes.TagNodegroupNamespace, kubernetes.TagTeam},
		}
		nodesReplacement = &view.View{
			Name:        "node_replacement_request_total",
			Measure:     kubernetes.MeasureNodesReplacementRequest,
			Description: "Number of nodes replacement requested.",
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{kubernetes.TagResult, kubernetes.TagReason, kubernetes.TagNodegroupName, kubernetes.TagNodegroupNamePrefix, kubernetes.TagNodegroupNamespace, kubernetes.TagTeam},
		}
		nodesPreprovisioningLatency = &view.View{
			Name:        "node_preprovisioning_latency",
			Measure:     kubernetes.MeasurePreprovisioningLatency,
			Description: "Latency to get preprovisioned node",
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{kubernetes.TagResult, kubernetes.TagReason, kubernetes.TagNodegroupName, kubernetes.TagNodegroupNamePrefix, kubernetes.TagNodegroupNamespace, kubernetes.TagTeam},
		}
	)

	kingpin.FatalIfError(view.Register(nodesCordoned, nodesUncordoned, nodesDrained, nodesDrainScheduled, limitedCordon, skippedCordon, nodesReplacement, nodesPreprovisioningLatency), "cannot create metrics")

	promOptions := prometheus.Options{Namespace: kubernetes.Component, Registry: prom.NewRegistry()}
	kubernetes.InitWorkqueueMetrics(promOptions.Registry)
	p, err := prometheus.NewExporter(promOptions)
	kingpin.FatalIfError(err, "cannot export metrics")
	view.RegisterExporter(p)

	log, err := zap.NewProduction()
	if options.debug {
		log, err = zap.NewDevelopment()
	}

	drainoklog.InitializeKlog(options.klogVerbosity)
	drainoklog.RedirectToLogger(log)

	web := &httpRunner{address: options.listen, logger: log, h: map[string]http.Handler{
		"/metrics": p,
		"/healthz": http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { r.Body.Close() }), // nolint:errcheck // no err management in health check
	}}

	kingpin.FatalIfError(err, "cannot create log")
	defer log.Sync() // nolint:errcheck // no check required on program exit

	go func() {
		log.Info("web server is running", zap.String("listen", options.listen))
		kingpin.FatalIfError(kubernetes.Await(web), "error serving")
	}()

	err = k8sclient.DecorateWithRateLimiter(config, "default")
	c, err := kubeclient.NewKubeConfig(config)
	kingpin.FatalIfError(err, "cannot create Kubernetes client configuration")

	kingpin.FatalIfError(err, "failed to decorate kubernetes clientset config")

	cs, err := client.NewForConfig(c)
	kingpin.FatalIfError(err, "cannot create Kubernetes client")

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
		kingpin.FatalIfError(err, "can't get resources for controlby filtering for eviction")
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
		kingpin.FatalIfError(err, "can't get resources for 'controlledBy' filtering for cordon")
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
		kubernetes.MaxGracePeriod(options.maxGracePeriod),
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
			kingpin.Fatalf("nodeLabels and NodeLabelsExpr cannot both be set")
		}
		ptrStr, err := kubernetes.ConvertLabelsToFilterExpr(options.nodeLabels)
		if err != nil {
			kingpin.Fatalf(err.Error())
		}
		if ptrStr != nil {
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
	kingpin.FatalIfError(err, "cannot get hostname")

	scopeObserver := kubernetes.NewScopeObserver(cs, globalConfig, runtimeObjectStoreImpl, options.scopeAnalysisPeriod, podFilteringFunc,
		kubernetes.PodOrControllerHasAnyOfTheAnnotations(runtimeObjectStoreImpl, options.optInPodAnnotations...),
		kubernetes.PodOrControllerHasAnyOfTheAnnotations(runtimeObjectStoreImpl, options.cordonProtectedPodAnnotations...),
		nodeLabelFilterFunc, log)
	go scopeObserver.Run(ctx.Done())
	if options.resetScopeLabel == true {
		go scopeObserver.Reset()
	}

	lock, err := resourcelock.New(
		resourcelock.EndpointsResourceLock,
		options.namespace,
		options.leaderElectionTokenName,
		cs.CoreV1(),
		cs.CoordinationV1(),
		resourcelock.ResourceLockConfig{
			Identity:      id,
			EventRecorder: k8sEventRecorder,
		},
	)
	kingpin.FatalIfError(err, "cannot create lock")

	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: options.leaderElectionLeaseDuration,
		RenewDeadline: options.leaderElectionRenewDeadline,
		RetryPeriod:   options.leaderElectionRetryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				log.Info("watchers are running")
				kingpin.FatalIfError(kubernetes.Await(nodes, pods, statefulSets, persistentVolumes, persistentVolumeClaims, globalLocker), "error watching")
			},
			OnStoppedLeading: func() {
				kingpin.Fatalf("lost leader election")
			},
		},
	})
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
