package kubernetes

import (
	"errors"
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
	var failedCount int32 = 0
	scheduler := NewDrainSchedules(&NoopCordonDrainer{}, &record.FakeRecorder{}, period, DefaultSchedulingRetryBackoffDelay, []string{}, []SuppliedCondition{}, NodePreprovisioningConfiguration{}, zap.NewNop(), nil)
	whenFirstSched, _ := scheduler.Schedule(&v1.Node{ObjectMeta: meta.ObjectMeta{Name: "initNode"}}, failedCount)
	whenFirstSchedSpecificGroup, _ := scheduler.Schedule(&v1.Node{ObjectMeta: meta.ObjectMeta{Name: "initNodeGrp", Annotations: map[string]string{DrainGroupAnnotation: "grp1"}}}, failedCount)

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
			name: "first schedule in default group",
			node: &v1.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
			window: timeWindow{
				from: whenFirstSched.Add(period - 2*time.Second),
				to:   whenFirstSched.Add(period + 2*time.Second),
			},
		},
		{
			name: "second schedule in default group",
			node: &v1.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName + "2"}},
			window: timeWindow{
				from: whenFirstSched.Add(2*period - 2*time.Second),
				to:   whenFirstSched.Add(2*period + 2*time.Second),
			},
		},
		{
			name: "a schedule in group grp1",
			node: &v1.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName + "othergrp", Annotations: map[string]string{DrainGroupAnnotation: "grp1"}}},
			window: timeWindow{
				from: whenFirstSchedSpecificGroup.Add(period - 2*time.Second),
				to:   whenFirstSchedSpecificGroup.Add(period + 2*time.Second),
			},
		},
		{
			name: "third schedule in default group",
			node: &v1.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName + "3", Annotations: map[string]string{CustomDrainBufferAnnotation: "5m"}}},
			window: timeWindow{
				from: whenFirstSched.Add(3*period - 2*time.Second),
				to:   whenFirstSched.Add(3*period + 2*time.Second),
			},
		},
		{
			name: "fourth schedule (following 5m custom drain buffer) in default group",
			node: &v1.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName + "4"}},
			window: timeWindow{
				from: whenFirstSched.Add(3*period + 5*time.Minute - 2*time.Second),
				to:   whenFirstSched.Add(3*period + 5*time.Minute + 2*time.Second),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Check that node is not yet scheduled for drain
			hasSchedule, _ := scheduler.HasSchedule(tt.node)
			if hasSchedule {
				t.Errorf("Node %v should not have any schedule", tt.node.Name)
			}
			var failedCount int32 = 0
			when, err := scheduler.Schedule(tt.node, failedCount)
			if (err != nil) != tt.wantErr {
				t.Errorf("DrainSchedules.Schedule() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// Check that node is scheduled for drain
			hasSchedule, _ = scheduler.HasSchedule(tt.node)
			if !hasSchedule {
				t.Errorf("Missing schedule record for node %v", tt.node.Name)
			}
			// Check that scheduled are place in the good time window
			if when.Before(tt.window.from) || when.After(tt.window.to) {
				t.Errorf("Schedule out of timeWindow")
			}
		})
	}
}

type failDrainer struct {
	NoopCordonDrainer
}

func (d *failDrainer) Drain(n *v1.Node) error { return errors.New("myerr") }

// Test to ensure there are no races when calling HasSchedule while the
// scheduler is draining a node.
func TestDrainSchedules_HasSchedule_Polling(t *testing.T) {
	scheduler := NewDrainSchedules(&failDrainer{}, &record.FakeRecorder{}, 0, DefaultSchedulingRetryBackoffDelay, []string{}, []SuppliedCondition{}, NodePreprovisioningConfiguration{}, zap.NewNop(), nil)
	node := &v1.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}}
	var failedCount int32 = 0
	when, err := scheduler.Schedule(node, failedCount)
	if err != nil {
		t.Fatalf("DrainSchedules.Schedule() error = %v", err)
	}

	timeout := time.After(time.Until(when) + time.Minute)
	for {
		hasSchedule, failed := scheduler.HasSchedule(node)
		if !hasSchedule {
			t.Fatalf("Missing schedule record for node %v", node.Name)
		}
		if failed {
			// Having `failed` as true is the expected result here since this
			// test is using the `failDrainer{}` drainer. It means that
			// HasSchedule was successfully called during or after the draining
			// function was scheduled and the test can complete successfully.
			break
		}
		select {
		case <-time.After(time.Second):
			// Small sleep to ensure we're not running the CPU hot while
			// polling `HasSchedule`.
		case <-timeout:
			// This timeout prevents this test from running forever in case
			// some bug caused the draining function never to be scheduled.
			t.Fatalf("timeout waiting for HasSchedule to fail")
		}
	}
}

func TestDrainSchedules_DeleteSchedule(t *testing.T) {
	fmt.Println("Now: " + time.Now().Format(time.RFC3339))
	period := time.Minute
	var failedCount int32 = 0
	scheduler := NewDrainSchedules(&NoopCordonDrainer{}, &record.FakeRecorder{}, period, DefaultSchedulingRetryBackoffDelay, []string{}, []SuppliedCondition{}, NodePreprovisioningConfiguration{}, zap.NewNop(), nil)
	whenFirstSched, _ := scheduler.Schedule(&v1.Node{ObjectMeta: meta.ObjectMeta{Name: "initNode"}}, failedCount)

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
				from: whenFirstSched.Add(period - 2*time.Second),
				to:   whenFirstSched.Add(period + 2*time.Second),
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Check that node is not yet scheduled for drain
			hasSchedule, _ := scheduler.HasSchedule(tt.node)
			if hasSchedule {
				t.Errorf("Node %v should not have any schedule", tt.node.Name)
			}
			var failedCount int32 = 0
			when, err := scheduler.Schedule(tt.node, failedCount)
			if (err != nil) != tt.wantErr {
				t.Errorf("DrainSchedules.Schedule() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// Check that scheduled are place in the good time window
			if when.Before(tt.window.from) || when.After(tt.window.to) {
				t.Errorf("Schedule out of timeWindow")
			}
			// Check that node is scheduled for drain
			hasSchedule, _ = scheduler.HasSchedule(tt.node)
			if !hasSchedule {
				t.Errorf("Missing schedule record for node %v", tt.node.Name)
			}
			// Deleting schedule
			scheduler.DeleteSchedule(tt.node)
			// Check that node is no more scheduled for drain
			hasSchedule, _ = scheduler.HasSchedule(tt.node)
			if hasSchedule {
				t.Errorf("Node %v should not been scheduled anymore", tt.node.Name)
			}
		})
	}
}
