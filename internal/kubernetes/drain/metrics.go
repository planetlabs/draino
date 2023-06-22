package drain

import (
	"reflect"
	"strconv"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	core "k8s.io/api/core/v1"

	"github.com/planetlabs/draino/internal/kubernetes"
)

var (
	Metrics = struct {
		SimulatedNodes *prometheus.CounterVec
		SimulatedPods  *prometheus.CounterVec
	}{
		SimulatedNodes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "draino_simulated_nodes_total",
			Help: "Number of nodes simulated",
		}, []string{kubernetes.TagResult.Name(), kubernetes.TagNodegroupName.Name(), kubernetes.TagNodegroupNamePrefix.Name(), kubernetes.TagNodegroupNamespace.Name(), kubernetes.TagTeam.Name(), kubernetes.TagService.Name()}),
		SimulatedPods: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "draino_simulated_pods_total",
			Help: "Number of pods simulated",
		}, []string{kubernetes.TagResult.Name(), kubernetes.TagNodegroupName.Name(), kubernetes.TagNodegroupNamePrefix.Name(), kubernetes.TagNodegroupNamespace.Name(), kubernetes.TagTeam.Name(), kubernetes.TagService.Name(), kubernetes.TagUserEvictionURL.Name()}),
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

type SimulationResult string

const (
	SimulationSucceeded SimulationResult = "succeeded"
	SimulationFailed    SimulationResult = "failed"
)

func CounterSimulatedNodes(node *core.Node, result SimulationResult) {
	values := kubernetes.GetNodeTagsValues(node)

	tags := []string{string(result), values.NgName, kubernetes.GetNodeGroupNamePrefix(values.NgName), values.NgNamespace, values.Team, values.Service}
	Metrics.SimulatedNodes.WithLabelValues(tags...).Add(1)
}

func CounterSimulatedPods(pod *core.Pod, node *core.Node, result SimulationResult, evictionURL bool) {
	podValues := kubernetes.GetPodTagsValues(pod)
	nodeValues := kubernetes.GetNodeTagsValues(node)
	team := podValues.Team
	if team == "" {
		team = nodeValues.Team
	}
	service := podValues.Service
	if service == "" {
		service = nodeValues.Service
	}

	tags := []string{string(result), nodeValues.NgName, kubernetes.GetNodeGroupNamePrefix(nodeValues.NgName), nodeValues.NgNamespace, team, service, strconv.FormatBool(evictionURL)}
	Metrics.SimulatedPods.WithLabelValues(tags...).Add(1)
}
