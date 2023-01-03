package filters

import (
	"context"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNewNodeWithLabelFilter(t *testing.T) {
	n1In := &corev1.Node{
		ObjectMeta: v1.ObjectMeta{
			Labels: map[string]string{"in": "ok"},
		},
	}
	n2Out := &corev1.Node{
		ObjectMeta: v1.ObjectMeta{
			Labels: map[string]string{"in": "notIn"},
		},
	}
	n3Out := &corev1.Node{}

	tests := []struct {
		name  string
		nodes []*corev1.Node
		want  []*corev1.Node
	}{
		{name: "test", nodes: []*corev1.Node{n1In, n2Out, n3Out}, want: []*corev1.Node{n1In}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewNodeWithLabelFilter(func(o interface{}) bool {
				n, ok := o.(*corev1.Node)
				if !ok {
					panic("nota node")
				}
				if n.Labels == nil {
					return false
				}
				return n.Labels["in"] == "ok"
			})
			if got := f.Filter(context.Background(), tt.nodes); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("checkNodesLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}
