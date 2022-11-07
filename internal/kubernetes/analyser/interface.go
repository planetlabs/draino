package analyser

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
)

// BlockingPod stores information about the pod, which is taking the disruption budget
type BlockingPod struct {
	NodeName string
	Pod      *corev1.Pod
	PDB      *policyv1.PodDisruptionBudget
}

// Interface is used to abstract the analyser implementation
type Interface interface {
	// BlockingPodsOnNode returns all pods running on the given node, that are taking a disruption budget
	BlockingPodsOnNode(ctx context.Context, nodeName string) ([]BlockingPod, error)
}
