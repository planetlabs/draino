package sorters

import (
	v1 "k8s.io/api/core/v1"
	"strconv"
)

const (
	// NodeAnnotationDrainASAPKey The annotation can have a numeric value associated. The higher the value, the higher the priority
	// Default value if the annotation is present is 1.
	// A negative value would mean that the node is to be drained not asap but latter, after the ones not having the annotation
	NodeAnnotationDrainASAPKey = "node-lifecycle.datadoghq.com/drain-asap"
)

func CompareNodeAnnotationDrainASAP(n1, n2 *v1.Node) bool {
	var err error
	a1, a2 := 0, 0
	if n1.Annotations != nil {
		if str, ok := n1.Annotations[NodeAnnotationDrainASAPKey]; ok {
			a1 = 1
			if str != "" {
				if a1, err = strconv.Atoi(str); err != nil {
					a1 = 1 // go back to default value in case of conversion error
				}
			}
		}

	}
	if n2.Annotations != nil {
		if str, ok := n2.Annotations[NodeAnnotationDrainASAPKey]; ok {
			a2 = 1
			if str != "" {
				if a2, err = strconv.Atoi(str); err != nil {
					a2 = 1 // go back to default value in case of conversion error
				}
			}
		}
	}
	return a1 > a2
}
