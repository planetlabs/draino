package candidate_runner

import v1 "k8s.io/api/core/v1"

// This file contains the NodeSorters

const (
	NodeAnnotationDrainASAPKey = "node-lifecycle.datadoghq.com/drain-asap"
)

func CompareNoAnnotationDrainASAP(n1, n2 *v1.Node) bool {
	a1, a2 := 1, 1
	if n1.Labels != nil {
		if _, ok := n1.Labels[NodeAnnotationDrainASAPKey]; ok {
			a1 = 0
		}

	}
	if n2.Labels != nil {
		if _, ok := n2.Labels[NodeAnnotationDrainASAPKey]; ok {
			a2 = 0
		}
	}
	return a1 < a2
}
