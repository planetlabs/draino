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
	"path/filepath"
	"sort"
	"time"

	"contrib.go.opencensus.io/exporter/prometheus"
	"github.com/julienschmidt/httprouter"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"go.uber.org/zap"
	"gopkg.in/alecthomas/kingpin.v2"
	core "k8s.io/api/core/v1"
	client "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog"

	"github.com/planetlabs/draino/internal/kubernetes"
)

// Default leader election settings.
const (
	DefaultLeaderElectionLeaseDuration = 15 * time.Second
	DefaultLeaderElectionRenewDeadline = 10 * time.Second
	DefaultLeaderElectionRetryPeriod   = 2 * time.Second
)

func main() {
	go http.ListenAndServe("localhost:8085", nil) // for go profiler
	//nolint:lll // accept long lines in option declarations
	var (
		app = kingpin.New(filepath.Base(os.Args[0]), "Automatically cordons and drains nodes that match the supplied conditions.").DefaultEnvars()

		debug                       = app.Flag("debug", "Run with debug logging.").Short('d').Bool()
		listen                      = app.Flag("listen", "Address at which to expose /metrics and /healthz.").Default(":10002").String()
		kubecfg                     = app.Flag("kubeconfig", "Path to kubeconfig file. Leave unset to use in-cluster config.").String()
		apiserver                   = app.Flag("master", "Address of Kubernetes API server. Leave unset to use in-cluster config.").String()
		dryRun                      = app.Flag("dry-run", "Emit an event without cordoning or draining matching nodes.").Bool()
		maxGracePeriod              = app.Flag("max-grace-period", "Maximum time evicted pods will be given to terminate gracefully.").Default(kubernetes.DefaultMaxGracePeriod.String()).Duration()
		evictionHeadroom            = app.Flag("eviction-headroom", "Additional time to wait after a pod's termination grace period for it to have been deleted.").Default(kubernetes.DefaultEvictionOverhead.String()).Duration()
		drainBuffer                 = app.Flag("drain-buffer", "Minimum time between starting each drain. Nodes are always cordoned immediately.").Default(kubernetes.DefaultDrainBuffer.String()).Duration()
		schedulingRetryBackoffDelay = app.Flag("retry-backoff-delay", "Additional delay to add between retry schedules.").Default(kubernetes.DefaultSchedulingRetryBackoffDelay.String()).Duration()
		nodeLabels                  = app.Flag("node-label", "(Deprecated) Nodes with this label will be eligible for cordoning and draining. May be specified multiple times").Strings()
		nodeLabelsExpr              = app.Flag("node-label-expr", "Nodes that match this expression will be eligible for cordoning and draining.").String()
		namespace                   = app.Flag("namespace", "Namespace used to create leader election lock object.").Default("kube-system").String()

		leaderElectionLeaseDuration = app.Flag("leader-election-lease-duration", "Lease duration for leader election.").Default(DefaultLeaderElectionLeaseDuration.String()).Duration()
		leaderElectionRenewDeadline = app.Flag("leader-election-renew-deadline", "Leader election renew deadline.").Default(DefaultLeaderElectionRenewDeadline.String()).Duration()
		leaderElectionRetryPeriod   = app.Flag("leader-election-retry-period", "Leader election retry period.").Default(DefaultLeaderElectionRetryPeriod.String()).Duration()
		leaderElectionTokenName     = app.Flag("leader-election-token-name", "Leader election token name.").Default(kubernetes.Component).String()

		// Eviction filtering flags
		skipDrain                 = app.Flag("skip-drain", "Whether to skip draining nodes after cordoning.").Default("false").Bool()
		doNotEvictPodControlledBy = app.Flag("do-not-evict-pod-controlled-by", "Do not evict pods that are controlled by the designated kind, empty VALUE for uncontrolled pods, May be specified multiple times.").PlaceHolder("kind[[.version].group]] examples: StatefulSets StatefulSets.apps StatefulSets.apps.v1").Default("", kubernetes.KindStatefulSet, kubernetes.KindDaemonSet).Strings()
		evictLocalStoragePods     = app.Flag("evict-emptydir-pods", "Evict pods with local storage, i.e. with emptyDir volumes.").Bool()
		protectedPodAnnotations   = app.Flag("protected-pod-annotation", "Protect pods with this annotation from eviction. May be specified multiple times.").PlaceHolder("KEY[=VALUE]").Strings()
		drainGroupLabelKey        = app.Flag("drain-group-labels", "Comma separated list of label keys to be used to form draining groups.").PlaceHolder("KEY1,KEY2,...").Default("").String()

		// Cordon filtering flags
		doNotCordonPodControlledBy    = app.Flag("do-not-cordon-pod-controlled-by", "Do not cordon nodes hosting pods that are controlled by the designated kind, empty VALUE for uncontrolled pods, May be specified multiple times.").PlaceHolder("kind[[.version].group]] examples: StatefulSets StatefulSets.apps StatefulSets.apps.v1").Default("", kubernetes.KindStatefulSet).Strings()
		cordonLocalStoragePods        = app.Flag("cordon-emptydir-pods", "Evict pods with local storage, i.e. with emptyDir volumes.").Default("true").Bool()
		cordonProtectedPodAnnotations = app.Flag("cordon-protected-pod-annotation", "Protect nodes hosting pods with this annotation from cordon. May be specified multiple times.").PlaceHolder("KEY[=VALUE]").Strings()

		// Cordon limiter flags
		maxSimultaneousCordon          = app.Flag("max-simultaneous-cordon", "Maximum number of cordoned nodes in the cluster.").PlaceHolder("(Value|Value%)").Strings()
		maxSimultaneousCordonForLabels = app.Flag("max-simultaneous-cordon-for-labels", "Maximum number of cordoned nodes in the cluster for given labels. Example: '2,app,shard'").PlaceHolder("(Value|Value%),keys...").Strings()
		maxSimultaneousCordonForTaints = app.Flag("max-simultaneous-cordon-for-taints", "Maximum number of cordoned nodes in the cluster for given taints. Example: '33%,node'").PlaceHolder("(Value|Value%),keys...").Strings()
		maxNotReadyNodes               = app.Flag("max-notready-nodes", "Maximum number of NotReady nodes in the cluster. When exceeding this value draino stop taking actions.").PlaceHolder("(Value|Value%)").Strings()
		maxNotReadyNodesPeriod         = app.Flag("max-notready-nodes-period", "Polling period to check all nodes readiness").Default(kubernetes.DefaultMaxNotReadyNodesPeriod.String()).Duration()
		maxPendingPodsPeriod           = app.Flag("max-pending-pods-period", "Polling period to check volume of pending pods").Default(kubernetes.DefaultMaxPendingPodsPeriod.String()).Duration()
		maxPendingPods                 = app.Flag("max-pending-pods", "Maximum number of Pending Pods in the cluster. When exceeding this value draino stop taking actions.").PlaceHolder("(Value|Value%)").Strings()
		maxDrainAttemptsBeforeFail     = app.Flag("max-drain-attempts-before-fail", "Maximum number of failed drain attempts before giving-up on draining the node.").Default("8").Int()

		// Pod Opt-in flags
		optInPodAnnotations = app.Flag("opt-in-pod-annotation", "Pod filtering out is ignored if the pod holds one of these annotations. In a way, this makes the pod directly eligible for draino eviction. May be specified multiple times.").PlaceHolder("KEY[=VALUE]").Strings()

		// NodeReplacement limiter flags
		maxNodeReplacementPerHour = app.Flag("max-node-replacement-per-hour", "Maximum number of nodes per hour for which draino can ask replacement.").Default("2").Int()
		durationBeforeReplacement = app.Flag("duration-before-replacement", "Max duration we are waiting for a node with Completed drain status to be removed before asking for replacement.").Default(kubernetes.DefaultDurationBeforeReplacement.String()).Duration()

		// Preprovisioning flags
		preprovisioningTimeout     = app.Flag("preprovisioning-timeout", "Timeout for a node to be preprovisioned before draining").Default(kubernetes.DefaultPreprovisioningTimeout.String()).Duration()
		preprovisioningCheckPeriod = app.Flag("preprovisioning-check-period", "Period to check if a node has been preprovisioned").Default(kubernetes.DefaultPreprovisioningCheckPeriod.String()).Duration()

		// PV/PVC management
		storageClassesAllowingVolumeDeletion = app.Flag("storage-class-allows-pv-deletion", "Storage class for which persistent volume (and associated claim) deletion is allowed. May be specified multiple times.").PlaceHolder("storageClassName").Strings()

		configName           = app.Flag("config-name", "Name of the draino configuration").Required().String()
		resetScopeAnnotation = app.Flag("reset-config-annotations", "Reset the scope annotation on the nodes").Bool()
		scopeAnalysisPeriod  = app.Flag("scope-analysis-period", "Period to run the scope analysis and generate metric").Default((5 * time.Minute).String()).Duration()

		conditions = app.Arg("node-conditions", "Nodes for which any of these conditions are true will be cordoned and drained.").Required().Strings()
	)
	kingpin.MustParse(app.Parse(os.Args[1:]))

	// this is required to make all packages using klog write to stderr instead of tmp files
	klog.InitFlags(nil)

	var (
		nodesCordoned = &view.View{
			Name:        "cordoned_nodes_total",
			Measure:     kubernetes.MeasureNodesCordoned,
			Description: "Number of nodes cordoned.",
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{kubernetes.TagResult, kubernetes.TagConditions, kubernetes.TagNodegroupName, kubernetes.TagNodegroupNamespace, kubernetes.TagTeam},
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
			TagKeys:     []tag.Key{kubernetes.TagResult, kubernetes.TagFailureCause, kubernetes.TagConditions, kubernetes.TagNodegroupName, kubernetes.TagNodegroupNamespace, kubernetes.TagTeam},
		}
		nodesDrainScheduled = &view.View{
			Name:        "drain_scheduled_nodes_total",
			Measure:     kubernetes.MeasureNodesDrainScheduled,
			Description: "Number of nodes scheduled for drain.",
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{kubernetes.TagResult, kubernetes.TagConditions, kubernetes.TagNodegroupName, kubernetes.TagNodegroupNamespace, kubernetes.TagTeam},
		}
		limitedCordon = &view.View{
			Name:        "limited_cordon_total",
			Measure:     kubernetes.MeasureLimitedCordon,
			Description: "Number of limited cordon encountered.",
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{kubernetes.TagReason, kubernetes.TagConditions, kubernetes.TagNodegroupName, kubernetes.TagNodegroupNamespace, kubernetes.TagTeam},
		}
		skippedCordon = &view.View{
			Name:        "skipped_cordon_total",
			Measure:     kubernetes.MeasureSkippedCordon,
			Description: "Number of skipped cordon encountered.",
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{kubernetes.TagReason, kubernetes.TagConditions, kubernetes.TagNodegroupName, kubernetes.TagNodegroupNamespace, kubernetes.TagTeam},
		}
		nodesReplacement = &view.View{
			Name:        "node_replacement_request_total",
			Measure:     kubernetes.MeasureNodesReplacementRequest,
			Description: "Number of nodes replacement requested.",
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{kubernetes.TagResult, kubernetes.TagReason, kubernetes.TagNodegroupName, kubernetes.TagNodegroupNamespace, kubernetes.TagTeam},
		}
		nodesPreprovisioningLatency = &view.View{
			Name:        "node_preprovisioning_latency",
			Measure:     kubernetes.MeasurePreprovisioningLatency,
			Description: "Latency to get preprovisioned node",
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{kubernetes.TagResult, kubernetes.TagReason, kubernetes.TagNodegroupName, kubernetes.TagNodegroupNamespace, kubernetes.TagTeam},
		}
	)

	kingpin.FatalIfError(view.Register(nodesCordoned, nodesUncordoned, nodesDrained, nodesDrainScheduled, limitedCordon, skippedCordon, nodesReplacement, nodesPreprovisioningLatency), "cannot create metrics")

	p, err := prometheus.NewExporter(prometheus.Options{Namespace: kubernetes.Component})
	kingpin.FatalIfError(err, "cannot export metrics")
	view.RegisterExporter(p)

	log, err := zap.NewProduction()
	if *debug {
		log, err = zap.NewDevelopment()
	}

	web := &httpRunner{address: *listen, logger: log, h: map[string]http.Handler{
		"/metrics": p,
		"/healthz": http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { r.Body.Close() }), // nolint:errcheck // no err management in health check
	}}

	kingpin.FatalIfError(err, "cannot create log")
	defer log.Sync() // nolint:errcheck // no check required on program exit

	go func() {
		log.Info("web server is running", zap.String("listen", *listen))
		kingpin.FatalIfError(kubernetes.Await(web), "error serving")
	}()

	c, err := kubernetes.BuildConfigFromFlags(*apiserver, *kubecfg)
	kingpin.FatalIfError(err, "cannot create Kubernetes client configuration")

	cs, err := client.NewForConfig(c)
	kingpin.FatalIfError(err, "cannot create Kubernetes client")

	pods := kubernetes.NewPodWatch(cs)
	statefulSets := kubernetes.NewStatefulsetWatch(cs)
	persistentVolumes := kubernetes.NewPersistentVolumeWatch(cs)
	persistentVolumeClaims := kubernetes.NewPersistentVolumeClaimWatch(cs)
	runtimeObjectStoreImpl := &kubernetes.RuntimeObjectStoreImpl{
		StatefulSetsStore:          statefulSets,
		PodsStore:                  pods,
		PersistentVolumeStore:      persistentVolumes,
		PersistentVolumeClaimStore: persistentVolumeClaims,
	}

	// Sanitize user input
	sort.Strings(*conditions)

	// Eviction Filtering
	pf := []kubernetes.PodFilterFunc{kubernetes.MirrorPodFilter}
	if !*evictLocalStoragePods {
		pf = append(pf, kubernetes.LocalStoragePodFilter)
	}
	apiResources, err := kubernetes.GetAPIResourcesForGVK(cs, *doNotEvictPodControlledBy)
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
	pf = append(pf, kubernetes.UnprotectedPodFilter(append(systemKnownAnnotations, *protectedPodAnnotations...)...))

	// Cordon Filtering
	podFilterCordon := []kubernetes.PodFilterFunc{}
	if !*cordonLocalStoragePods {
		podFilterCordon = append(podFilterCordon, kubernetes.LocalStoragePodFilter)
	}
	apiResourcesCordon, err := kubernetes.GetAPIResourcesForGVK(cs, *doNotCordonPodControlledBy)
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
	podFilterCordon = append(podFilterCordon, kubernetes.UnprotectedPodFilter(*cordonProtectedPodAnnotations...))

	// Cordon limiter
	cordonLimiter := kubernetes.NewCordonLimiter(log)
	for _, p := range *maxSimultaneousCordon {
		max, percent, parseErr := kubernetes.ParseCordonMax(p)
		if parseErr != nil {
			kingpin.FatalIfError(parseErr, "cannot parse 'max-simultaneous-cordon' argument")
		}
		cordonLimiter.AddLimiter("MaxSimultaneousCordon:"+p, kubernetes.MaxSimultaneousCordonLimiterFunc(max, percent))
	}
	for _, p := range *maxSimultaneousCordonForLabels {
		max, percent, keys, parseErr := kubernetes.ParseCordonMaxForKeys(p)
		if parseErr != nil {
			kingpin.FatalIfError(parseErr, "cannot parse 'max-simultaneous-cordon-for-labels' argument")
		}
		cordonLimiter.AddLimiter("MaxSimultaneousCordonLimiterForLabels:"+p, kubernetes.MaxSimultaneousCordonLimiterForLabelsFunc(max, percent, keys))
	}
	for _, p := range *maxSimultaneousCordonForTaints {
		max, percent, keys, parseErr := kubernetes.ParseCordonMaxForKeys(p)
		if parseErr != nil {
			kingpin.FatalIfError(parseErr, "cannot parse 'max-simultaneous-cordon-for-taints' argument")
		}
		cordonLimiter.AddLimiter("MaxSimultaneousCordonLimiterForTaints:"+p, kubernetes.MaxSimultaneousCordonLimiterForTaintsFunc(max, percent, keys))
	}
	globalLocker := kubernetes.NewGlobalBlocker(log)
	for _, p := range *maxNotReadyNodes {
		max, percent, parseErr := kubernetes.ParseCordonMax(p)
		if parseErr != nil {
			kingpin.FatalIfError(parseErr, "cannot parse 'max-notready-nodes' argument")
		}
		globalLocker.AddBlocker("MaxNotReadyNodes:"+p, kubernetes.MaxNotReadyNodesCheckFunc(max, percent, runtimeObjectStoreImpl, log), *maxNotReadyNodesPeriod)
	}
	for _, p := range *maxPendingPods {
		max, percent, parseErr := kubernetes.ParseCordonMax(p)
		if parseErr != nil {
			kingpin.FatalIfError(parseErr, "cannot parse 'max-pending-pods' argument")
		}
		fmt.Println(max, percent)
		globalLocker.AddBlocker("MaxPendingPods:"+p, kubernetes.MaxPendingPodsCheckFunc(max, percent, runtimeObjectStoreImpl, log), *maxPendingPodsPeriod)
	}

	for name, blockStateFunc := range globalLocker.GetBlockStateCacheAccessor() {
		localFunc := blockStateFunc
		cordonLimiter.AddLimiter(name, func(_ *core.Node, _, _ []*core.Node) (bool, error) { return !localFunc(), nil })
	}

	nodeReplacementLimiter := kubernetes.NewNodeReplacementLimiter(*maxNodeReplacementPerHour, time.Now())

	eventRecorder := kubernetes.NewEventRecorder(cs)

	cordonDrainer := kubernetes.NewAPICordonDrainer(cs,
		eventRecorder,
		kubernetes.MaxGracePeriod(*maxGracePeriod),
		kubernetes.EvictionHeadroom(*evictionHeadroom),
		kubernetes.WithSkipDrain(*skipDrain),
		kubernetes.WithPodFilter(kubernetes.NewPodFiltersIgnoreCompletedPods(
			kubernetes.NewPodFiltersWithOptInFirst(kubernetes.PodOrControllerHasAnyOfTheAnnotations(runtimeObjectStoreImpl, *optInPodAnnotations...), kubernetes.NewPodFilters(pf...)))),
		kubernetes.WithCordonLimiter(cordonLimiter),
		kubernetes.WithNodeReplacementLimiter(nodeReplacementLimiter),
		kubernetes.WithStorageClassesAllowingDeletion(*storageClassesAllowingVolumeDeletion),
		kubernetes.WithMaxDrainAttemptsBeforeFail(*maxDrainAttemptsBeforeFail),
		kubernetes.WithAPICordonDrainerLogger(log),
	)

	podFilteringFunc := kubernetes.NewPodFiltersIgnoreCompletedPods(
		kubernetes.NewPodFiltersWithOptInFirst(
			kubernetes.PodOrControllerHasAnyOfTheAnnotations(runtimeObjectStoreImpl, *optInPodAnnotations...), kubernetes.NewPodFilters(podFilterCordon...)))

	var h cache.ResourceEventHandler = kubernetes.NewDrainingResourceEventHandler(
		cordonDrainer,
		runtimeObjectStoreImpl,
		eventRecorder,
		kubernetes.WithLogger(log),
		kubernetes.WithDrainBuffer(*drainBuffer),
		kubernetes.WithSchedulingBackoffDelay(*schedulingRetryBackoffDelay),
		kubernetes.WithDurationWithCompletedStatusBeforeReplacement(*durationBeforeReplacement),
		kubernetes.WithDrainGroups(*drainGroupLabelKey),
		kubernetes.WithConditionsFilter(*conditions),
		kubernetes.WithCordonPodFilter(podFilteringFunc),
		kubernetes.WithGlobalBlocking(globalLocker),
		kubernetes.WithPreprovisioningConfiguration(kubernetes.NodePreprovisioningConfiguration{Timeout: *preprovisioningTimeout, CheckPeriod: *preprovisioningCheckPeriod}))

	if *dryRun {
		h = cache.FilteringResourceEventHandler{
			FilterFunc: kubernetes.NewNodeProcessed().Filter,
			Handler: kubernetes.NewDrainingResourceEventHandler(
				&kubernetes.NoopCordonDrainer{},
				runtimeObjectStoreImpl,
				eventRecorder,
				kubernetes.WithLogger(log),
				kubernetes.WithDrainBuffer(*drainBuffer),
				kubernetes.WithSchedulingBackoffDelay(*schedulingRetryBackoffDelay),
				kubernetes.WithDurationWithCompletedStatusBeforeReplacement(*durationBeforeReplacement),
				kubernetes.WithDrainGroups(*drainGroupLabelKey),
				kubernetes.WithGlobalBlocking(globalLocker),
				kubernetes.WithConditionsFilter(*conditions)),
		}
	}

	if len(*nodeLabels) > 0 {
		log.Info("node labels", zap.Any("labels", nodeLabels))
		if *nodeLabelsExpr != "" {
			kingpin.Fatalf("nodeLabels and NodeLabelsExpr cannot both be set")
		}
		if nodeLabelsExpr, err = kubernetes.ConvertLabelsToFilterExpr(*nodeLabels); err != nil {
			kingpin.Fatalf(err.Error())
		}
	}

	var nodeLabelFilter cache.ResourceEventHandler
	log.Debug("label expression", zap.Any("expr", nodeLabelsExpr))

	nodeLabelFilterFunc, err := kubernetes.NewNodeLabelFilter(nodeLabelsExpr, log)
	if err != nil {
		log.Sugar().Fatalf("Failed to parse node label expression: %v", err)
	}

	nodeLabelFilter = cache.FilteringResourceEventHandler{FilterFunc: nodeLabelFilterFunc, Handler: h}
	nodes := kubernetes.NewNodeWatch(cs, nodeLabelFilter)
	runtimeObjectStoreImpl.NodesStore = nodes
	//storeCloserFunc := runtimeObjectStoreImpl.Run(log)
	//defer storeCloserFunc()
	cordonLimiter.SetNodeLister(nodes)
	cordonDrainer.SetRuntimeObjectStore(runtimeObjectStoreImpl)

	id, err := os.Hostname()
	kingpin.FatalIfError(err, "cannot get hostname")

	// use a Go context so we can tell the leaderelection code when we
	// want to step down
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scopeObserver := kubernetes.NewScopeObserver(cs, *configName, kubernetes.ParseConditions(*conditions), runtimeObjectStoreImpl, *scopeAnalysisPeriod, podFilteringFunc, kubernetes.PodOrControllerHasAnyOfTheAnnotations(runtimeObjectStoreImpl, *optInPodAnnotations...), kubernetes.PodOrControllerHasAnyOfTheAnnotations(runtimeObjectStoreImpl, *cordonProtectedPodAnnotations...), nodeLabelFilterFunc, log)
	go scopeObserver.Run(ctx.Done())
	if *resetScopeAnnotation == true {
		go scopeObserver.Reset()
	}

	lock, err := resourcelock.New(
		resourcelock.EndpointsResourceLock,
		*namespace,
		*leaderElectionTokenName,
		cs.CoreV1(),
		cs.CoordinationV1(),
		resourcelock.ResourceLockConfig{
			Identity:      id,
			EventRecorder: eventRecorder,
		},
	)
	kingpin.FatalIfError(err, "cannot create lock")

	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: *leaderElectionLeaseDuration,
		RenewDeadline: *leaderElectionRenewDeadline,
		RetryPeriod:   *leaderElectionRetryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				log.Info("watchers are running")
				kingpin.FatalIfError(kubernetes.Await(nodes, pods, statefulSets, persistentVolumes, globalLocker), "error watching")
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
