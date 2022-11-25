package utils

import corev1 "k8s.io/api/core/v1"

// IsPodReady is checking if the "Ready" condition is set to true
func IsPodReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.ContainersReady {
			if condition.Status == corev1.ConditionTrue {
				return true
			}
		}
	}
	return false
}