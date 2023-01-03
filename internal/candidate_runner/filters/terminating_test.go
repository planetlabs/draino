package filters

import (
	"context"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNewNodeTerminatingFilter(t *testing.T) {
	setClockForTest()
	n1, n2, n3 := &corev1.Node{}, &corev1.Node{}, &corev1.Node{
		ObjectMeta: v1.ObjectMeta{
			DeletionTimestamp: &v1.Time{Time: clockInTest.Now()},
		},
	}
	tests := []struct {
		name          string
		nodes         []*corev1.Node
		wantKeep      []*corev1.Node
		wantFilterOut []*corev1.Node
	}{
		{
			name:          "keep non terminating nodes",
			nodes:         []*corev1.Node{n1, n2, n3},
			wantKeep:      []*corev1.Node{n1, n2},
			wantFilterOut: []*corev1.Node{n3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewNodeTerminatingFilter()
			gotKeep := f.Filter(context.Background(), tt.nodes)
			if !reflect.DeepEqual(gotKeep, tt.wantKeep) {
				t.Errorf("checkNodesTerminating() gotKeep = %v, want %v", gotKeep, tt.wantKeep)
			}
		})
	}

}
