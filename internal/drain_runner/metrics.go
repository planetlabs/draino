package drain_runner

import (
	"reflect"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	core "k8s.io/api/core/v1"

	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/metrics"
)

var (
	Metrics = struct {
		DrainedNodes         *prometheus.CounterVec
		PreProcessorFailures *prometheus.CounterVec
	}{
		DrainedNodes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "draino_drained_nodes_total",
			Help: "Number of nodes drained.",
		}, []string{kubernetes.TagResult.Name(), kubernetes.TagFailureCause.Name(), kubernetes.TagConditions.Name(), kubernetes.TagNodegroupName.Name(), kubernetes.TagNodegroupNamePrefix.Name(), kubernetes.TagNodegroupNamespace.Name(), kubernetes.TagTeam.Name()}),
		PreProcessorFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pre_processor_failures",
			Help: "Number of failures per nodegroup",
		}, []string{metrics.TagPreProcessor, metrics.TagReason, metrics.TagNodegroupName, metrics.TagNodegroupNamespace, metrics.TagGroupKey}),
	}
	registerOnce sync.Once
)

func RegisterMetrics(reg prometheus.Registerer) {
	registerOnce.Do(func() {
		values := reflect.ValueOf(Metrics)
		for i := 0; i < values.NumField(); i++ {
			collector := values.Field(i).Interface().(prometheus.Collector)
			reg.MustRegister(collector)
		}
	})
}

type DrainNodesResult string

const (
	DrainedNodeResultSucceeded DrainNodesResult = "succeeded"
	DrainedNodeResultFailed    DrainNodesResult = "failed"
)

func CounterDrainedNodes(node *core.Node, result DrainNodesResult, conditions []kubernetes.SuppliedCondition, failureReason kubernetes.FailureCause) {
	values := kubernetes.GetNodeTagsValues(node)
	conditionTypes := append(kubernetes.GetConditionsTypes(conditions), metrics.TagConditionAnyValue)

	for _, c := range conditionTypes {
		tags := []string{string(result), string(failureReason), c, values.NgName, kubernetes.GetNodeGroupNamePrefix(values.NgName), values.NgNamespace, values.Team}
		Metrics.DrainedNodes.WithLabelValues(tags...).Add(1)
	}
}

func CounterPreProcessorFailures(node *core.Node, preProcName, reason, drainGroup string) {
	values := kubernetes.GetNodeTagsValues(node)
	Metrics.PreProcessorFailures.WithLabelValues(preProcName, reason, values.NgName, values.NgNamespace, drainGroup).Add(1)
}

const (
	DrainRunnerComponent = "drain_runner"
)
