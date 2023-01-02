package main

import (
	"fmt"
	"time"

	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/labels"
)

// Options collects the program options/parameters
type Options struct {
	noLegacyNodeHandler         bool
	debug                       bool
	listen                      string
	kubecfg                     string
	apiserver                   string
	dryRun                      bool
	minEvictionTimeout          time.Duration
	evictionHeadroom            time.Duration
	drainBuffer                 time.Duration
	drainBufferConfigMapName    string
	schedulingRetryBackoffDelay time.Duration
	nodeLabels                  []string
	nodeLabelsExpr              string

	leaderElectionLeaseDuration time.Duration
	leaderElectionRenewDeadline time.Duration
	leaderElectionRetryPeriod   time.Duration
	leaderElectionTokenName     string

	// Eviction filtering flags
	skipDrain                 bool
	doNotEvictPodControlledBy []string
	evictLocalStoragePods     bool
	protectedPodAnnotations   []string
	drainGroupLabelKey        string

	// Cordon filtering flags
	doNotCordonPodControlledBy    []string
	cordonLocalStoragePods        bool
	cordonProtectedPodAnnotations []string

	// Cordon limiter flags
	skipCordonLimiterNodeAnnotation         string
	skipCordonLimiterNodeAnnotationSelector labels.Selector

	maxSimultaneousCordon          []string
	maxSimultaneousCordonFunctions map[string]kubernetes.LimiterFunc

	maxSimultaneousCordonForLabels          []string
	maxSimultaneousCordonForLabelsFunctions map[string]kubernetes.LimiterFunc

	maxSimultaneousCordonForTaints          []string
	maxSimultaneousCordonForTaintsFunctions map[string]kubernetes.LimiterFunc

	maxNotReadyNodes          []string
	maxNotReadyNodesFunctions map[string]kubernetes.ComputeBlockStateFunctionFactory
	maxNotReadyNodesPeriod    time.Duration

	maxPendingPods          []string
	maxPendingPodsFunctions map[string]kubernetes.ComputeBlockStateFunctionFactory
	maxPendingPodsPeriod    time.Duration

	maxDrainAttemptsBeforeFail int

	// Pod Opt-in flags
	optInPodAnnotations      []string
	shortLivedPodAnnotations []string

	// NodeReplacement limiter flags
	maxNodeReplacementPerHour int
	durationBeforeReplacement time.Duration

	// Preprovisioning flags
	preprovisioningTimeout            time.Duration
	preprovisioningCheckPeriod        time.Duration
	preprovisioningActivatedByDefault bool

	// PV/PVC management
	storageClassesAllowingVolumeDeletion []string
	pvcManagementByDefault               bool

	configName          string
	resetScopeLabel     bool
	scopeAnalysisPeriod time.Duration

	groupRunnerPeriod time.Duration

	klogVerbosity int32

	conditions         []string
	suppliedConditions []kubernetes.SuppliedCondition
}

func optionsFromFlags() (*Options, *pflag.FlagSet) {
	var (
		fs  pflag.FlagSet
		opt Options
	)
	fs.BoolVar(&opt.debug, "debug", false, "Run with debug logging.")
	fs.BoolVar(&opt.dryRun, "dry-run", false, "Emit an event without cordoning or draining matching nodes.")
	fs.BoolVar(&opt.skipDrain, "skip-drain", false, "Whether to skip draining nodes after cordoning.")
	fs.BoolVar(&opt.evictLocalStoragePods, "evict-emptydir-pods", false, "Evict pods with local storage, i.e. with emptyDir volumes.")
	fs.BoolVar(&opt.cordonLocalStoragePods, "cordon-emptydir-pods", true, "Evict pods with local storage, i.e. with emptyDir volumes.")
	fs.BoolVar(&opt.preprovisioningActivatedByDefault, "preprovisioning-by-default", false, "Set this flag to activate pre-provisioning by default for all nodes")
	fs.BoolVar(&opt.pvcManagementByDefault, "pvc-management-by-default", false, "PVC management is automatically activated for a workload that do not use eviction++")
	fs.BoolVar(&opt.resetScopeLabel, "reset-config-labels", false, "Reset the scope label on the nodes")
	fs.BoolVar(&opt.noLegacyNodeHandler, "no-legacy-node-handler", false, "Deactivate draino legacy node handler")

	fs.DurationVar(&opt.minEvictionTimeout, "min-eviction-timeout", kubernetes.DefaultMinEvictionTimeout, "Minimum time we wait to evict a pod. The pod terminationGracePeriod will be used if it is bigger.")
	fs.DurationVar(&opt.evictionHeadroom, "eviction-headroom", kubernetes.DefaultEvictionOverhead, "Additional time to wait after a pod's termination grace period for it to have been deleted.")
	fs.DurationVar(&opt.drainBuffer, "drain-buffer", kubernetes.DefaultDrainBuffer, "Minimum time between starting each drain. Nodes are always cordoned immediately.")
	fs.StringVar(&opt.drainBufferConfigMapName, "drain-buffer-configmap-name", "", "The name of the configmap used to persist the drain-buffer values. Default will be draino-<config-name>-drain-buffer.")
	fs.DurationVar(&opt.schedulingRetryBackoffDelay, "retry-backoff-delay", kubernetes.DefaultSchedulingRetryBackoffDelay, "Additional delay to add between retry schedules.")
	fs.DurationVar(&opt.leaderElectionLeaseDuration, "leader-election-lease-duration", DefaultLeaderElectionLeaseDuration, "Lease duration for leader election.")
	fs.DurationVar(&opt.leaderElectionRenewDeadline, "leader-election-renew-deadline", DefaultLeaderElectionRenewDeadline, "Leader election renew deadline.")
	fs.DurationVar(&opt.leaderElectionRetryPeriod, "leader-election-retry-period", DefaultLeaderElectionRetryPeriod, "Leader election retry period.")
	fs.DurationVar(&opt.maxNotReadyNodesPeriod, "max-notready-nodes-period", kubernetes.DefaultMaxNotReadyNodesPeriod, "Polling period to check all nodes readiness")
	fs.DurationVar(&opt.maxPendingPodsPeriod, "max-pending-pods-period", kubernetes.DefaultMaxPendingPodsPeriod, "Polling period to check volume of pending pods")
	fs.DurationVar(&opt.durationBeforeReplacement, "duration-before-replacement", kubernetes.DefaultDurationBeforeReplacement, "Max duration we are waiting for a node with Completed drain status to be removed before asking for replacement.")
	fs.DurationVar(&opt.preprovisioningTimeout, "preprovisioning-timeout", kubernetes.DefaultPreprovisioningTimeout, "Timeout for a node to be preprovisioned before draining")
	fs.DurationVar(&opt.preprovisioningCheckPeriod, "preprovisioning-check-period", kubernetes.DefaultPreprovisioningCheckPeriod, "Period to check if a node has been preprovisioned")
	fs.DurationVar(&opt.scopeAnalysisPeriod, "scope-analysis-period", 5*time.Minute, "Period to run the scope analysis and generate metric")
	fs.DurationVar(&opt.groupRunnerPeriod, "group-runner-period", 10*time.Second, "Period for running the group runner")

	fs.StringSliceVar(&opt.nodeLabels, "node-label", []string{}, "(Deprecated) Nodes with this label will be eligible for cordoning and draining. May be specified multiple times")
	fs.StringSliceVar(&opt.doNotEvictPodControlledBy, "do-not-evict-pod-controlled-by", []string{"", kubernetes.KindStatefulSet, kubernetes.KindDaemonSet},
		"Do not evict pods that are controlled by the designated kind, empty VALUE for uncontrolled pods, May be specified multiple times: kind[[.version].group]] examples: StatefulSets StatefulSets.apps StatefulSets.apps.v1")
	fs.StringSliceVar(&opt.protectedPodAnnotations, "protected-pod-annotation", []string{}, "Protect pods with this annotation from eviction. May be specified multiple times. KEY[=VALUE]")
	fs.StringSliceVar(&opt.doNotCordonPodControlledBy, "do-not-cordon-pod-controlled-by", []string{"", kubernetes.KindStatefulSet}, "Do not cordon nodes hosting pods that are controlled by the designated kind, empty VALUE for uncontrolled pods, May be specified multiple times. kind[[.version].group]] examples: StatefulSets StatefulSets.apps StatefulSets.apps.v1")
	fs.StringSliceVar(&opt.cordonProtectedPodAnnotations, "cordon-protected-pod-annotation", []string{}, "Protect nodes hosting pods with this annotation from cordon. May be specified multiple times. KEY[=VALUE]")
	fs.StringSliceVar(&opt.maxSimultaneousCordon, "max-simultaneous-cordon", []string{}, "Maximum number of cordoned nodes in the cluster. (Value|Value%)")
	fs.StringSliceVar(&opt.maxSimultaneousCordonForLabels, "max-simultaneous-cordon-for-labels", []string{}, "Maximum number of cordoned nodes in the cluster for given labels. Example: '2;app;shard'. (Value|Value%),keys...")
	fs.StringSliceVar(&opt.maxSimultaneousCordonForTaints, "max-simultaneous-cordon-for-taints", []string{}, "Maximum number of cordoned nodes in the cluster for given taints. Example: '33%;node'. (Value|Value%),keys...")
	fs.StringSliceVar(&opt.maxNotReadyNodes, "max-notready-nodes", []string{}, "Maximum number of NotReady nodes in the cluster. When exceeding this value draino stop taking actions. (Value|Value%)")
	fs.StringSliceVar(&opt.maxPendingPods, "max-pending-pods", []string{}, "Maximum number of Pending Pods in the cluster. When exceeding this value draino stop taking actions. (Value|Value%)")
	fs.StringSliceVar(&opt.optInPodAnnotations, "opt-in-pod-annotation", []string{}, "Pod filtering out is ignored if the pod holds one of these annotations. In a way, this makes the pod directly eligible for draino eviction. May be specified multiple times. KEY[=VALUE]")
	fs.StringSliceVar(&opt.shortLivedPodAnnotations, "short-lived-pod-annotation", []string{}, "Pod that have a short live, just like job; we prefer let them run till the end instead of evicting them; node is cordon. May be specified multiple times. KEY[=VALUE]")
	fs.StringSliceVar(&opt.storageClassesAllowingVolumeDeletion, "storage-class-allows-pv-deletion", []string{}, "Storage class for which persistent volume (and associated claim) deletion is allowed. May be specified multiple times.")

	fs.StringVar(&opt.nodeLabelsExpr, "node-label-expr", "", "Nodes that match this expression will be eligible for cordoning and draining.")
	fs.StringVar(&opt.listen, "listen", ":10002", "Address at which to expose /metrics and /healthz.")
	fs.StringVar(&opt.kubecfg, "kubeconfig", "", "Path to kubeconfig file. Leave unset to use in-cluster config.")
	fs.StringVar(&opt.apiserver, "master", "", "Address of Kubernetes API server. Leave unset to use in-cluster config.")
	fs.StringVar(&opt.leaderElectionTokenName, "leader-election-token-name", kubernetes.Component, "Leader election token name.")
	fs.StringVar(&opt.drainGroupLabelKey, "drain-group-labels", "", "Comma separated list of label keys to be used to form draining groups. KEY1,KEY2,...")
	fs.StringVar(&opt.skipCordonLimiterNodeAnnotation, "skip-cordon-limiter-node-annotation", "", "Skip all limiter logic if node has annotation. KEY[=VALUE]")
	fs.StringVar(&opt.configName, "config-name", "", "Name of the draino configuration")

	// We are using some values with json content, so don't use StringSlice: https://github.com/spf13/pflag/issues/370
	fs.StringArrayVar(&opt.conditions, "node-conditions", nil, "Nodes for which any of these conditions are true will be cordoned and drained.")

	fs.IntVar(&opt.maxDrainAttemptsBeforeFail, "max-drain-attempts-before-fail", 8, "Maximum number of failed drain attempts before giving-up on draining the node.")
	fs.IntVar(&opt.maxNodeReplacementPerHour, "max-node-replacement-per-hour", 2, "Maximum number of nodes per hour for which draino can ask replacement.")
	fs.Int32Var(&opt.klogVerbosity, "klog-verbosity", 4, "Verbosity to run klog at")

	return &opt, &fs
}

func (o *Options) Validate() error {
	if o.configName == "" {
		return fmt.Errorf("--config-name must be defined and not empty")
	}
	var err error

	// If the drain buffer config name is not set, we'll reuse the configName
	if o.drainBufferConfigMapName == "" {
		o.drainBufferConfigMapName = fmt.Sprintf("draino-%s-drain-buffer", o.configName)
	}

	// Cordon limiter validation
	o.skipCordonLimiterNodeAnnotationSelector, err = labels.Parse(o.skipCordonLimiterNodeAnnotation)
	if err != nil {
		return fmt.Errorf("cannot parse 'skip-cordon-limiter-node-annotation' argument, %#v", err)
	}

	o.maxSimultaneousCordonFunctions = map[string]kubernetes.LimiterFunc{}
	for _, p := range o.maxSimultaneousCordon {
		max, percent, parseErr := kubernetes.ParseCordonMax(p)
		if err != nil {
			return fmt.Errorf("cannot parse 'max-simultaneous-cordon' argument, %#v", parseErr)
		}
		o.maxSimultaneousCordonFunctions[p] = kubernetes.MaxSimultaneousCordonLimiterFunc(max, percent)
	}
	o.maxSimultaneousCordonForLabelsFunctions = map[string]kubernetes.LimiterFunc{}
	for _, p := range o.maxSimultaneousCordonForLabels {
		max, percent, keys, parseErr := kubernetes.ParseCordonMaxForKeys(p)
		if parseErr != nil {
			return fmt.Errorf("cannot parse 'max-simultaneous-cordon-for-labels' argument, %#v", parseErr)
		}
		o.maxSimultaneousCordonForLabelsFunctions[p] = kubernetes.MaxSimultaneousCordonLimiterForLabelsFunc(max, percent, keys)
	}
	o.maxSimultaneousCordonForTaintsFunctions = map[string]kubernetes.LimiterFunc{}
	for _, p := range o.maxSimultaneousCordonForTaints {
		max, percent, keys, parseErr := kubernetes.ParseCordonMaxForKeys(p)
		if parseErr != nil {
			return fmt.Errorf("cannot parse 'max-simultaneous-cordon-for-taints' argument, %#v", parseErr)
		}
		o.maxSimultaneousCordonForTaintsFunctions[p] = kubernetes.MaxSimultaneousCordonLimiterForTaintsFunc(max, percent, keys)
	}

	// NotReady Nodes and NotReady Pods
	factoryComputeBlockStateForNodes := func(max int, percent bool) kubernetes.ComputeBlockStateFunctionFactory {
		return func(s kubernetes.RuntimeObjectStore, l *zap.Logger) kubernetes.ComputeBlockStateFunction {
			return kubernetes.MaxNotReadyNodesCheckFunc(max, percent, s, l)
		}
	}
	o.maxNotReadyNodesFunctions = map[string]kubernetes.ComputeBlockStateFunctionFactory{}
	for _, p := range o.maxNotReadyNodes {
		max, percent, parseErr := kubernetes.ParseCordonMax(p)
		if parseErr != nil {
			return fmt.Errorf("cannot parse 'max-notready-nodes' argument, %#v", parseErr)
		}
		o.maxNotReadyNodesFunctions[p] = factoryComputeBlockStateForNodes(max, percent)
	}
	factoryComputeBlockStateForPods := func(max int, percent bool) kubernetes.ComputeBlockStateFunctionFactory {
		return func(s kubernetes.RuntimeObjectStore, l *zap.Logger) kubernetes.ComputeBlockStateFunction {
			return kubernetes.MaxPendingPodsCheckFunc(max, percent, s, l)
		}
	}
	o.maxPendingPodsFunctions = map[string]kubernetes.ComputeBlockStateFunctionFactory{}
	for _, p := range o.maxPendingPods {
		max, percent, parseErr := kubernetes.ParseCordonMax(p)
		if parseErr != nil {
			return fmt.Errorf("cannot parse 'max-pending-pods' argument, %#v", parseErr)
		}
		o.maxPendingPodsFunctions[p] = factoryComputeBlockStateForPods(max, percent)
	}

	// Check that conditions are defined and well formatted
	if len(o.conditions) == 0 {
		return fmt.Errorf("no condition defined")
	}
	if o.suppliedConditions, err = kubernetes.ParseConditions(o.conditions); err != nil {
		return fmt.Errorf("one of the conditions is not correctly formatted: %#v", err)
	}
	if o.groupRunnerPeriod < time.Second {
		return fmt.Errorf("group runner period should be at least 1s")
	}
	return nil
}
