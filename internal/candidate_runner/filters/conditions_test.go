package filters

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/planetlabs/draino/internal/kubernetes"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"
	testing2 "k8s.io/utils/clock/testing"
)

var clockInTest clock.Clock

func setClockForTest() clock.Clock {
	if clockInTest != nil {
		return clockInTest
	}

	var nowInTest = time.Now()

	clockInTest = testing2.NewFakeClock(nowInTest)
	return clockInTest
}
func Test_NewNodeWithConditionFilter(t *testing.T) {
	setClockForTest()
	n1 := &corev1.Node{
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{
					Type:               "unknown",
					Status:             corev1.ConditionTrue,
					LastTransitionTime: v1.Time{Time: clockInTest.Now().Add(-time.Hour)},
				},
				{
					Type:               "known",
					Status:             corev1.ConditionTrue,
					LastTransitionTime: v1.Time{Time: clockInTest.Now().Add(-time.Hour)},
				},
			},
		},
	}
	n2 := &corev1.Node{
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{
					Type:               "unknown",
					Status:             corev1.ConditionTrue,
					LastTransitionTime: v1.Time{Time: clockInTest.Now().Add(-time.Hour)},
				},
			},
		},
	}
	n3 := &corev1.Node{
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{
					Type:               "known",
					Status:             corev1.ConditionTrue,
					LastTransitionTime: v1.Time{Time: clockInTest.Now().Add(-time.Hour)},
				},
			},
		},
	}
	n4 := &corev1.Node{
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{
					Type:               "known",
					Status:             corev1.ConditionFalse,
					LastTransitionTime: v1.Time{Time: clockInTest.Now().Add(-time.Hour)},
				},
			},
		},
	}
	tests := []struct {
		name       string
		conditions []kubernetes.SuppliedCondition
		nodes      []*corev1.Node
		want       []*corev1.Node
	}{
		{
			name: "no matching conditions",
			conditions: []kubernetes.SuppliedCondition{
				{
					Type:   "other",
					Status: corev1.ConditionTrue,
				},
			},
			nodes: []*corev1.Node{n1, n2, n3, n4},
			want:  []*corev1.Node{},
		},
		{
			name: "matching known condition",
			conditions: []kubernetes.SuppliedCondition{
				{
					Type:   "known",
					Status: corev1.ConditionTrue,
				},
			},
			nodes: []*corev1.Node{n1},
			want:  []*corev1.Node{n1},
		},

		{
			name: "matching known condition",
			conditions: []kubernetes.SuppliedCondition{
				{
					Type:   "known",
					Status: corev1.ConditionTrue,
				},
			},
			nodes: []*corev1.Node{n1, n2, n3, n4},
			want:  []*corev1.Node{n1, n3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewNodeWithConditionFilter(tt.conditions)

			if got := f.Filter(context.Background(), tt.nodes); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewNodeWithConditionFilter.Filter = %v, want %v", got, tt.want)
			}
		})
	}
}
