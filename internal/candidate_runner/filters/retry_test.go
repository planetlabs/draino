package filters

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/planetlabs/draino/internal/kubernetes/drain"
	corev1 "k8s.io/api/core/v1"
)

type fakeRetryWallWithDefinedTimeStamp struct {
	timestamp map[*corev1.Node]time.Time
}

var _ drain.RetryWall = &fakeRetryWallWithDefinedTimeStamp{}

func (f *fakeRetryWallWithDefinedTimeStamp) GetRetryWallTimestamp(node *corev1.Node) time.Time {
	return f.timestamp[node]
}

func (f *fakeRetryWallWithDefinedTimeStamp) SetNewRetryWallTimestamp(context.Context, *corev1.Node, string, time.Time) (*corev1.Node, error) {
	panic("implement me")
}
func (f *fakeRetryWallWithDefinedTimeStamp) GetDrainRetryAttemptsCount(*corev1.Node) int {
	panic("implement me")
}
func (f *fakeRetryWallWithDefinedTimeStamp) ResetRetryCount(context.Context, *corev1.Node) (*corev1.Node, error) {
	panic("implement me")
}
func (f *fakeRetryWallWithDefinedTimeStamp) IsAboveAlertingThreshold(*corev1.Node) bool {
	panic("implement me")
}

func TestNewRetryWallFilter(t *testing.T) {
	setClockForTest()
	n1, n2, n3 := &corev1.Node{}, &corev1.Node{}, &corev1.Node{}

	tests := []struct {
		name      string
		retryWall drain.RetryWall
		nodes     []*corev1.Node
		wantKeep  []*corev1.Node
	}{
		{
			name: "retryWall all in future",
			retryWall: &fakeRetryWallWithDefinedTimeStamp{timestamp: map[*corev1.Node]time.Time{
				n1: clockInTest.Now().Add(time.Hour),
				n2: clockInTest.Now().Add(time.Hour),
				n3: clockInTest.Now().Add(time.Hour),
			}},
			nodes:    []*corev1.Node{n1, n2, n3},
			wantKeep: []*corev1.Node{},
		},
		{
			name:      "retryWall all zero",
			retryWall: &fakeRetryWallWithDefinedTimeStamp{timestamp: map[*corev1.Node]time.Time{}},
			nodes:     []*corev1.Node{n1, n2, n3},
			wantKeep:  []*corev1.Node{n1, n2, n3},
		},
		{
			name: "retryWall one zero,one future, one past",
			retryWall: &fakeRetryWallWithDefinedTimeStamp{timestamp: map[*corev1.Node]time.Time{
				n2: clockInTest.Now().Add(time.Hour),
				n3: clockInTest.Now().Add(-1 * time.Hour),
			}},
			nodes:    []*corev1.Node{n1, n2, n3},
			wantKeep: []*corev1.Node{n1, n3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewRetryWallFilter(setClockForTest(), tt.retryWall)

			gotKeep := f.Filter(context.Background(), tt.nodes)
			if !reflect.DeepEqual(gotKeep, tt.wantKeep) {
				t.Errorf("checkNodesRetryWall() gotKeep = %v, want %v", gotKeep, tt.wantKeep)
			}
		})
	}
}
