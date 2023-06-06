package index

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	corev1 "k8s.io/api/core/v1"
)

type createPodOptions struct {
	Name     string
	Ns       string
	NodeName string
	IsReady  bool
	LS       labels.Set
}

func createPod(opts createPodOptions) *corev1.Pod {
	var label labels.Set = map[string]string{}
	if opts.LS != nil {
		label = opts.LS
	}
	ready := corev1.ConditionFalse
	if opts.IsReady {
		ready = corev1.ConditionTrue
	}
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      opts.Name,
			Namespace: opts.Ns,
			Labels:    label,
		},
		Spec: corev1.PodSpec{
			NodeName: opts.NodeName,
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: ready,
				},
			},
		},
	}
}
