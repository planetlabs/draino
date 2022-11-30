package utils

import corev1 "k8s.io/api/core/v1"

func FindNodeCondition(t corev1.NodeConditionType, node *corev1.Node) (pos int, condition corev1.NodeCondition, found bool) {
	for i, c := range node.Status.Conditions {
		if c.Type == t {
			found = true
			pos = i
			condition = c
		}
	}
	return
}
