package k8sclient

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/util/taints"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type DrainTaintValue = string

const (
	// DrainoTaintKey taint key used to show progress of node drain operation
	DrainoTaintKey = "node-lifecycle"

	// TaintDrainCandidate is used to mark a node as the next drain candidate.
	// Now it will be picked up by the drain runner.
	TaintDrainCandidate DrainTaintValue = "drain-candidate"
	// TaintDraining is used to show that all preprocessors are done and that the draining will start now
	TaintDraining DrainTaintValue = "draining"
	// TaintDrained is used to show that the drain was successfully done and the node is ready to be shutdown
	TaintDrained DrainTaintValue = "drained"
)

// AddNLATaint adds the nla taint with the given value to the given node.
// After the update is done, it will return the updated version of the node, which can be used for further updates.
func AddNLATaint(ctx context.Context, client client.Client, node *corev1.Node, now time.Time, value DrainTaintValue) (*corev1.Node, error) {
	taint := CreateNLATaint(value, now)
	newNode, updated, err := taints.AddOrUpdateTaint(node, taint)
	if err != nil {
		return nil, err
	}
	if !updated {
		return node, nil
	}

	err = client.Update(ctx, newNode)
	return newNode, err
}

// RemoveNLATaint removes the nla taint from the given node.
// After the update is done, it will return the updated version of the node, which can be used for further updates.
func RemoveNLATaint(ctx context.Context, client client.Client, node *corev1.Node) (*corev1.Node, error) {
	// In this case neither the taint value nor the timestamp do really matter
	taint := CreateNLATaint(DrainTaintValue("whatever"), time.Time{})
	newNode, updated, err := taints.RemoveTaint(node, taint)
	if err != nil {
		return nil, err
	}
	if !updated {
		return node, nil
	}
	err = client.Update(ctx, newNode)
	return newNode, err
}

// GetNLATaint searches for the nla taint and returns it if found.
func GetNLATaint(node *corev1.Node) (*corev1.Taint, bool) {
	if len(node.Spec.Taints) == 0 {
		return nil, false
	}

	// In this case neither the taint value nor the timestamp do really matter
	search := CreateNLATaint(TaintDrainCandidate, time.Time{})
	for _, taint := range node.Spec.Taints {
		if taint.MatchTaint(search) {
			return &taint, true
		}
	}

	return nil, false
}

// CreateNLATaint creates a new NLA taint with the given value and TS
func CreateNLATaint(val DrainTaintValue, now time.Time) *corev1.Taint {
	timeAdded := metav1.NewTime(now)
	taint := corev1.Taint{Key: DrainoTaintKey, Value: val, Effect: corev1.TaintEffectNoSchedule, TimeAdded: &timeAdded}
	return &taint
}
