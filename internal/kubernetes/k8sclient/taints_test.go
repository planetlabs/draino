package k8sclient

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestTaints_AddTaint(t *testing.T) {
	tests := []struct {
		Name     string
		Node     *corev1.Node
		NewTaint DrainTaintValue
	}{
		{
			Name:     "Should add one taint",
			Node:     createNode(""),
			NewTaint: TaintDrainCandidate,
		},
		{
			Name:     "Should overwrite a taint with new value",
			Node:     createNode(TaintDrained),
			NewTaint: TaintDrainCandidate,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			client := fake.NewFakeClient(tt.Node)
			now := time.Now()

			_, err := AddNLATaint(context.Background(), client, tt.Node, now, tt.NewTaint)
			assert.NoError(t, err)

			var node corev1.Node
			err = client.Get(context.Background(), types.NamespacedName{Name: tt.Node.Name}, &node)
			assert.NoError(t, err)
			assert.Equal(t, 1, len(node.Spec.Taints))

			taint, exist := GetNLATaint(&node)
			assert.True(t, exist)
			assert.Equal(t, tt.NewTaint, taint.Value)
			assert.Equal(t, now.Format(time.RFC3339), taint.TimeAdded.Format(time.RFC3339))
		})
	}
}

func TestTaints_RemoveTaint(t *testing.T) {
	tests := []struct {
		Name string
		Node *corev1.Node
	}{
		{
			Name: "Should remove the draino taint",
			Node: createNode(TaintDrained),
		},
		{
			Name: "Should not fail when removing a taint that doesn't exist",
			Node: createNode(""),
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			client := fake.NewFakeClient(tt.Node)

			_, err := RemoveNLATaint(context.Background(), client, tt.Node)
			assert.NoError(t, err)

			var node corev1.Node
			err = client.Get(context.Background(), types.NamespacedName{Name: tt.Node.Name}, &node)
			assert.NoError(t, err)
			assert.Equal(t, 0, len(node.Spec.Taints))

			taint, exist := GetNLATaint(&node)
			assert.False(t, exist)
			assert.Nil(t, taint)
		})
	}
}

func createNode(taintVal DrainTaintValue) *corev1.Node {
	taints := []corev1.Taint{}
	if taintVal != "" {
		taints = append(taints, *CreateNLATaint(taintVal, time.Now()))
	}
	return &corev1.Node{
		ObjectMeta: v1.ObjectMeta{Name: "foo-node"},
		Spec: corev1.NodeSpec{
			Taints: taints,
		},
	}
}
