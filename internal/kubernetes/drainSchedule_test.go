package kubernetes

import (
	"fmt"
	"testing"
	"time"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
)

func TestDrainSchedules_Schedule(t *testing.T) {
	fmt.Println("Now: " + time.Now().Format(time.RFC3339))
	period := time.Minute
	scheduler := NewDrainSchedules(&NoopCordonDrainer{}, &record.FakeRecorder{}, period, zap.NewNop())
	whenFirstSched := scheduler.(*DrainSchedules).WhenNextSchedule()

	type timeWindow struct {
		from, to time.Time
	}

	tests := []struct {
		name    string
		node    *v1.Node
		window  timeWindow
		wantErr bool
	}{
		{
			name: "first schedule",
			node: &v1.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
			window: timeWindow{
				from: whenFirstSched,
				to:   whenFirstSched.Add(2 * time.Second),
			},
		},
		{
			name: "second schedule",
			node: &v1.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName + "2"}},
			window: timeWindow{
				from: whenFirstSched.Add(period - 2*time.Second),
				to:   whenFirstSched.Add(period + 2*time.Second),
			},
		},
		{
			name: "third schedule",
			node: &v1.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName + "3"}},
			window: timeWindow{
				from: whenFirstSched.Add(2*period - 2*time.Second),
				to:   whenFirstSched.Add(2*period + 2*time.Second),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Check that node is not yet scheduled for drain
			hasSchedule, _, _ := scheduler.HasSchedule(tt.node.Name)
			if hasSchedule {
				t.Errorf("Node %v should not have any schedule", tt.node.Name)
			}

			when, err := scheduler.Schedule(tt.node)
			if (err != nil) != tt.wantErr {
				t.Errorf("DrainSchedules.Schedule() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// Check that node is scheduled for drain
			hasSchedule, _, _ = scheduler.HasSchedule(tt.node.Name)
			if !hasSchedule {
				t.Errorf("Missing schedule record for node %v", tt.node.Name)
			}
			// Check that scheduled are place in the goog time window
			if when.Before(tt.window.from) || when.After(tt.window.to) {
				t.Errorf("Schedule out of timeWindow")
			}
			// Deleting schedule
			scheduler.DeleteSchedule(tt.node.Name)
			// Check that node is no more scheduled for drain
			hasSchedule, _, _ = scheduler.HasSchedule(tt.node.Name)
			if hasSchedule {
				t.Errorf("Node %v should not been scheduled anymore", tt.node.Name)
			}
		})
	}
}
