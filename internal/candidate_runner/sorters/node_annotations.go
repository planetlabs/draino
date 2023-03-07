package sorters

import (
	"context"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	v1 "k8s.io/api/core/v1"
)

const (
	// AnnotationDrainPriorityPKey The annotation can have a numeric value associated. The higher the value, the higher the priority
	// Default value if the annotation is present is 1.
	// A negative value would mean that the node is to be drained not asap but latter, after the ones not having the annotation
	AnnotationDrainPriorityPKey = "node-lifecycle.datadoghq.com/drain-priority"

	eventDrainPriorityConfiguration = "DrainPriorityConfiguration"
)

func convertPriority(data string) (int, error) {
	return strconv.Atoi(data)
}

func NewAnnotationPrioritizer(store kubernetes.RuntimeObjectStore, indexer *index.Indexer, eventRecorder kubernetes.EventRecorder, logger logr.Logger) func(n1, n2 *v1.Node) bool {
	logger = logger.WithName("AnnotationPrioritizer")
	return func(n1, n2 *v1.Node) bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		val1, found, err1 := getHighestPrioFromNode(ctx, store, indexer, eventRecorder, n1)
		if err1 != nil {
			logger.Error(err1, "failed to fetch drain priority information from chain", "node", n1.Name)
		}
		if !found {
			val1 = 0
		}

		val2, found, err2 := getHighestPrioFromNode(ctx, store, indexer, eventRecorder, n2)
		if err2 != nil {
			logger.Error(err2, "failed to fetch drain priority information from chain", "node", n2.Name)
		}
		if !found {
			val2 = 0
		}

		return val1 > val2
	}
}

func getHighestPrioFromNode(ctx context.Context, store kubernetes.RuntimeObjectStore, indexer *index.Indexer, eventRecorder kubernetes.EventRecorder, node *v1.Node) (int, bool, error) {
	searchRes, err := kubernetes.SearchAnnotationFromNodeAndThenPodOrController(ctx, indexer, store, convertPriority, AnnotationDrainPriorityPKey, node, false, false)
	if err != nil {
		return 0, false, err
	}

	searchRes.HandlerError(
		func(n *v1.Node, err error) {
			eventRecorder.NodeEventf(ctx, n, v1.EventTypeWarning, eventDrainPriorityConfiguration, "Cannot parse configuration: %v", err)
		},
		func(p *v1.Pod, err error) {
			eventRecorder.PodEventf(ctx, p, v1.EventTypeWarning, eventDrainPriorityConfiguration, "Cannot parse configuration: %v", err)
		},
	)

	var found bool
	var max int
	for _, row := range searchRes.Results() {
		if !found || max < row.Value {
			max = row.Value
			found = true
		}
	}

	return max, found, nil
}
