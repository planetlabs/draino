/*
Copyright 2018 Planet Labs Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing permissions
and limitations under the License.
*/

package kubernetes

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type mockCordonDrainer struct {
	calls []mockCall
}

type mockCall struct {
	name string
	node string
}

func (d *mockCordonDrainer) Cordon(n *core.Node, mutators ...nodeMutatorFn) error {
	d.calls = append(d.calls, mockCall{
		name: "Cordon",
		node: n.Name,
	})
	return nil
}

func (d *mockCordonDrainer) Uncordon(n *core.Node, mutators ...nodeMutatorFn) error {
	d.calls = append(d.calls, mockCall{
		name: "Uncordon",
		node: n.Name,
	})
	return nil
}

func (d *mockCordonDrainer) Drain(n *core.Node) error {
	d.calls = append(d.calls, mockCall{
		name: "Drain",
		node: n.Name,
	})
	return nil
}

func (d *mockCordonDrainer) MarkDrain(n *core.Node, when, finish time.Time, failed bool) error {
	d.calls = append(d.calls, mockCall{
		name: "MarkDrain",
		node: n.Name,
	})
	return nil
}

func (d *mockCordonDrainer) HasSchedule(name string) (has, failed bool) {
	d.calls = append(d.calls, mockCall{
		name: "HasSchedule",
		node: name,
	})
	return false, false
}

func (d *mockCordonDrainer) Schedule(node *core.Node) (time.Time, error) {
	d.calls = append(d.calls, mockCall{
		name: "Schedule",
		node: node.Name,
	})
	return time.Now(), nil
}

func (d *mockCordonDrainer) DeleteSchedule(name string) {
	d.calls = append(d.calls, mockCall{
		name: "DeleteSchedule",
		node: name,
	})
}

func TestDrainingResourceEventHandler(t *testing.T) {
	cases := []struct {
		name       string
		obj        interface{}
		conditions []string
		expected   []mockCall
	}{
		{
			name: "NoConditions",
			obj:  &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
		},
		{
			name: "NotANode",
			obj:  &core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName}},
		},
		{
			name:       "NoBadConditions",
			conditions: []string{"KernelPanic"},
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{
					Conditions: []core.NodeCondition{{
						Type:   "Other",
						Status: core.ConditionTrue,
					}},
				},
			},
		},
		{
			name:       "WithBadConditions",
			conditions: []string{"KernelPanic"},
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{
					Conditions: []core.NodeCondition{{
						Type:   "KernelPanic",
						Status: core.ConditionTrue,
					}},
				},
			},
			expected: []mockCall{
				{name: "Cordon", node: nodeName},
				{name: "HasSchedule", node: nodeName},
				{name: "Schedule", node: nodeName},
			},
		},
		{
			name:       "WithBadConditionsAlreadyCordoned",
			conditions: []string{"KernelPanic"},
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Spec:       core.NodeSpec{Unschedulable: true},
				Status: core.NodeStatus{
					Conditions: []core.NodeCondition{{
						Type:   "KernelPanic",
						Status: core.ConditionTrue,
					}},
				},
			},
			expected: []mockCall{
				{name: "HasSchedule", node: nodeName},
				{name: "Schedule", node: nodeName},
			},
		},
		{
			name:       "NoBadConditionsAlreadyCordoned",
			conditions: []string{"KernelPanic"},
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Spec:       core.NodeSpec{Unschedulable: true},
				Status: core.NodeStatus{
					Conditions: []core.NodeCondition{{
						Type:   "KernelPanic",
						Status: core.ConditionFalse,
					}},
				},
			},
		},
		{
			name:       "NoBadConditionsAlreadyCordonedByDraino",
			conditions: []string{"KernelPanic"},
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:        nodeName,
					Annotations: map[string]string{drainoConditionsAnnotationKey: "KernelPanic=True,0s"},
				},
				Spec: core.NodeSpec{Unschedulable: true},
				Status: core.NodeStatus{
					Conditions: []core.NodeCondition{{
						Type:   "KernelPanic",
						Status: core.ConditionFalse,
					}},
				},
			},
			expected: []mockCall{
				{name: "DeleteSchedule", node: nodeName},
				{name: "Uncordon", node: nodeName},
			},
		},
		{
			name:       "WithBadConditionsAlreadyCordonedByDraino",
			conditions: []string{"KernelPanic"},
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:        nodeName,
					Annotations: map[string]string{drainoConditionsAnnotationKey: "KernelPanic=True,0s"},
				},
				Spec: core.NodeSpec{Unschedulable: true},
				Status: core.NodeStatus{
					Conditions: []core.NodeCondition{{
						Type:   "KernelPanic",
						Status: core.ConditionTrue,
					}},
				},
			},
			expected: []mockCall{
				{name: "HasSchedule", node: nodeName},
				{name: "Schedule", node: nodeName},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cordonDrainer := &mockCordonDrainer{}
			h := NewDrainingResourceEventHandler(cordonDrainer, &record.FakeRecorder{}, WithDrainBuffer(0*time.Second), WithConditionsFilter(tc.conditions))
			h.drainScheduler = cordonDrainer
			h.OnUpdate(nil, tc.obj)

			if !reflect.DeepEqual(tc.expected, cordonDrainer.calls) {
				t.Errorf("cordonDrainer.calls: want %#v\ngot %#v", tc.expected, cordonDrainer.calls)
			}
		})
	}
}

func TestOffendingConditions(t *testing.T) {
	cases := []struct {
		name       string
		obj        *core.Node
		conditions []string
		expected   []SuppliedCondition
	}{
		{
			name: "SingleMatchingCondition",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					{Type: "Cool", Status: core.ConditionTrue},
				}},
			},
			conditions: []string{"Cool"},
			expected:   []SuppliedCondition{{Type: "Cool", Status: core.ConditionTrue}},
		},
		{
			name: "ManyMatchingConditions",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					{Type: "Cool", Status: core.ConditionTrue},
					{Type: "Rad", Status: core.ConditionTrue},
				}},
			},
			conditions: []string{"Cool", "Rad"},
			expected: []SuppliedCondition{
				{Type: "Cool", Status: core.ConditionTrue},
				{Type: "Rad", Status: core.ConditionTrue},
			},
		},
		{
			name: "PartiallyMatchingConditions",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					{Type: "Cool", Status: core.ConditionTrue},
					{Type: "Rad", Status: core.ConditionFalse},
				}},
			},
			conditions: []string{"Cool", "Rad"},
			expected: []SuppliedCondition{
				{Type: "Cool", Status: core.ConditionTrue},
			},
		},
		{
			name: "PartiallyAbsentConditions",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					{Type: "Rad", Status: core.ConditionTrue},
				}},
			},
			conditions: []string{"Cool", "Rad"},
			expected: []SuppliedCondition{
				{Type: "Rad", Status: core.ConditionTrue},
			},
		},
		{
			name: "SingleFalseCondition",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					{Type: "Cool", Status: core.ConditionFalse},
				}},
			},
			conditions: []string{"Cool"},
			expected:   nil,
		},
		{
			name:       "NoNodeConditions",
			obj:        &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
			conditions: []string{"Cool"},
			expected:   nil,
		},
		{
			name: "NoFilterConditions",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					{Type: "Cool", Status: core.ConditionFalse},
				}},
			},
			expected: nil,
		},
		{
			name: "NewConditionFormat",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					{Type: "Cool", Status: core.ConditionUnknown},
				}},
			},
			conditions: []string{"Cool=Unknown,10m"},
			expected: []SuppliedCondition{
				{Type: "Cool", Status: core.ConditionUnknown, MinimumDuration: 10 * time.Minute},
			},
		},
		{
			name: "NewConditionFormatDurationNotEnough",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					{Type: "Cool", Status: core.ConditionUnknown, LastTransitionTime: meta.NewTime(time.Now().Add(time.Duration(-9) * time.Minute))},
				}},
			},
			conditions: []string{"Cool=Unknown,10m"},
			expected:   nil,
		},
		{
			name: "NewConditionFormatDurationIsEnough",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					{Type: "Cool", Status: core.ConditionUnknown, LastTransitionTime: meta.NewTime(time.Now().Add(time.Duration(-15) * time.Minute))},
				}},
			},
			conditions: []string{"Cool=Unknown,14m"},
			expected: []SuppliedCondition{
				{Type: "Cool", Status: core.ConditionUnknown, MinimumDuration: 14 * time.Minute},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := NewDrainingResourceEventHandler(&NoopCordonDrainer{}, &record.FakeRecorder{}, WithConditionsFilter(tc.conditions))
			badConditions := h.offendingConditions(tc.obj)
			if !reflect.DeepEqual(badConditions, tc.expected) {
				t.Errorf("offendingConditions(tc.obj): want %#v, got %#v", tc.expected, badConditions)
			}
		})
	}
}

func TestDrainingResourceEventHandler_checkCordonFilters(t *testing.T) {

	node := &core.Node{
		ObjectMeta: meta.ObjectMeta{Name: "test-node"},
	}
	pod := &core.Pod{
		ObjectMeta: meta.ObjectMeta{Name: "test-pod"},
		Spec:       core.PodSpec{NodeName: "test-node"},
	}
	otherPod := &core.Pod{
		ObjectMeta: meta.ObjectMeta{Name: "other-pod"},
		Spec:       core.PodSpec{NodeName: "other-node"},
	}

	tests := []struct {
		name         string
		pods         []runtime.Object
		cordonFilter PodFilterFunc
		want         bool
	}{
		{
			name: "no Pods,no Filters",
			want: true,
		},
		{
			name:         "no Pods, Filter true",
			cordonFilter: func(p core.Pod) (bool, error) { return true, nil },
			want:         true,
		},
		{
			name:         "no Pods, Filter false",
			cordonFilter: func(p core.Pod) (bool, error) { return false, nil },
			want:         true,
		},
		{
			name:         "Pods, Filter true",
			pods:         []runtime.Object{pod, otherPod},
			cordonFilter: func(p core.Pod) (bool, error) { return true, nil },
			want:         true,
		},
		{
			name:         "Pods, Filter false",
			pods:         []runtime.Object{pod, otherPod},
			cordonFilter: func(p core.Pod) (bool, error) { return false, nil },
			want:         false,
		},
		{
			name:         "Pods on other node, Filter false",
			pods:         []runtime.Object{otherPod},
			cordonFilter: func(p core.Pod) (bool, error) { return false, nil },
			want:         true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Prepare podStore
			w := NewPodWatch(fake.NewSimpleClientset(tt.pods...))
			stop := make(chan struct{})
			defer close(stop)
			go w.SharedIndexInformer.Run(stop)
			// Wait for the informer to sync
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for !w.HasSynced() {
					time.Sleep(100 * time.Millisecond)
				}
			}()
			wg.Wait()

			h := &DrainingResourceEventHandler{
				logger:        zap.NewNop(),
				eventRecorder: &record.FakeRecorder{},
				podStore:      w,
				cordonFilter:  tt.cordonFilter,
			}
			if got := h.checkCordonFilters(node); got != tt.want {
				t.Errorf("checkCordonFilters() = %v, want %v", got, tt.want)
			}
		})
	}
}
