package utils

import corev1 "k8s.io/api/core/v1"

func GetPodNames(pods []*corev1.Pod) []string {
	result := make([]string, 0, len(pods))
	for _, pod := range pods {
		result = append(result, pod.Name)
	}
	return result
}
