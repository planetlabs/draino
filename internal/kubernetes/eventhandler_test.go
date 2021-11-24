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
	"fmt"
	"reflect"
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

var _ CordonDrainer = &mockCordonDrainer{}

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

func (d *mockCordonDrainer) ResetRetryAnnotation(n *core.Node) error {
	d.calls = append(d.calls, mockCall{
		name: "ResetRetryAnnotation",
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

func (d *mockCordonDrainer) GetMaxDrainAttemptsBeforeFail(_ *core.Node) int32 {
	return 0
}

func (d *mockCordonDrainer) MarkDrain(n *core.Node, when, finish time.Time, failed bool, failCount int32) error {
	d.calls = append(d.calls, mockCall{
		name: "MarkDrain",
		node: n.Name,
	})
	return nil
}

func (d *mockCordonDrainer) MarkDrainDelete(n *core.Node) error {
	d.calls = append(d.calls, mockCall{
		name: "MarkDrainDelete",
		node: n.Name,
	})
	return nil
}

func (d *mockCordonDrainer) GetPodsToDrain(node string, podStore PodStore) ([]*core.Pod, error) {
	d.calls = append(d.calls, mockCall{
		name: "GetPodsToDrain",
		node: node,
	})
	return nil, nil
}

func (d *mockCordonDrainer) HasSchedule(node *core.Node) (has, failed bool) {
	d.calls = append(d.calls, mockCall{
		name: "HasSchedule",
		node: node.Name,
	})

	hasSchedule := node.Annotations["hasSchedule"] == "true"

	return hasSchedule, false
}

func (d *mockCordonDrainer) Schedule(node *core.Node, failedCount int32) (time.Time, error) {
	d.calls = append(d.calls, mockCall{
		name: "Schedule",
		node: node.Name,
	})
	return time.Now(), nil
}

func (d *mockCordonDrainer) DeleteSchedule(node *core.Node) {
	d.calls = append(d.calls, mockCall{
		name: "DeleteSchedule",
		node: node.Name,
	})
}

func (d *mockCordonDrainer) DeleteScheduleByName(name string) {
	d.calls = append(d.calls, mockCall{
		name: "DeleteSchedule",
		node: name,
	})
}

func (d *mockCordonDrainer) ReplaceNode(n *core.Node) (NodeReplacementStatus, error) {
	d.calls = append(d.calls, mockCall{
		name: "ReplaceNode",
		node: n.Name,
	})
	return NodeReplacementStatusNone, nil
}

func (d *mockCordonDrainer) PreprovisionNode(n *core.Node) (NodeReplacementStatus, error) {
	d.calls = append(d.calls, mockCall{
		name: "PreprovisionNode",
		node: n.Name,
	})
	return NodeReplacementStatusNone, nil
}

func TestDrainingResourceEventHandler(t *testing.T) {
	cases := []struct {
		name       string
		obj        runtime.Object
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
				{name: "GetPodsToDrain", node: nodeName},
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
					},
						{
							Type:   ConditionDrainedScheduled,
							Status: core.ConditionTrue,
						}},
				},
			},
			expected: []mockCall{
				{name: "GetPodsToDrain", node: nodeName},
				{name: "HasSchedule", node: nodeName},
				{name: "Schedule", node: nodeName},
			},
		},
		{
			name:       "WithBadConditionsAlreadyCordonedAndDrained",
			conditions: []string{"KernelPanic"},
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Spec:       core.NodeSpec{Unschedulable: true},
				Status: core.NodeStatus{
					Conditions: []core.NodeCondition{{
						Type:   "KernelPanic",
						Status: core.ConditionTrue,
					},
						{
							Type:    ConditionDrainedScheduled,
							Status:  core.ConditionFalse,
							Message: fmt.Sprintf(" ... | %s: %s", CompletedStr, time.Now().Format(time.RFC3339)),
						}},
				},
			},
			expected: []mockCall{
				{name: "GetPodsToDrain", node: nodeName},
				{name: "ReplaceNode", node: nodeName},
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
					Annotations: map[string]string{drainoConditionsAnnotationKey: "KernelPanic=True,0s", "hasSchedule": "true"},
				},
				Spec: core.NodeSpec{Unschedulable: true},
				Status: core.NodeStatus{
					Conditions: []core.NodeCondition{
						{
							Type:   "KernelPanic",
							Status: core.ConditionFalse,
						},
						{
							Type:    ConditionDrainedScheduled,
							Message: "[1] | Drain activity scheduled 2020-03-20T15:50:34+01:00 | Failed: 2020-03-20T15:55:50+01:00",
							Status:  core.ConditionFalse,
						},
					},
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
					Conditions: []core.NodeCondition{
						{
							Type:   "KernelPanic",
							Status: core.ConditionTrue,
						},
						{
							Type:    ConditionDrainedScheduled,
							Message: "[1] | Drain activity scheduled 2020-03-20T15:50:34+01:00",
							Status:  core.ConditionTrue,
						},
					},
				},
			},
			expected: []mockCall{
				{name: "GetPodsToDrain", node: nodeName},
				{name: "HasSchedule", node: nodeName},
				{name: "Schedule", node: nodeName},
			},
		},
		{
			name:       "WithBadConditionsAlreadyCordonedByDrainoAndMaxRetryFailed",
			conditions: []string{"KernelPanic"},
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:        nodeName,
					Annotations: map[string]string{drainoConditionsAnnotationKey: "KernelPanic=True,0s", drainRetryFailedAnnotationKey: drainRetryFailedAnnotationValue},
				},
				Spec: core.NodeSpec{Unschedulable: true},
				Status: core.NodeStatus{
					Conditions: []core.NodeCondition{
						{
							Type:   "KernelPanic",
							Status: core.ConditionTrue,
						},
						{
							Type:    ConditionDrainedScheduled,
							Message: "[7] | Drain activity scheduled 2020-03-20T15:50:34+01:00",
							Status:  core.ConditionTrue,
						},
					},
				},
			},
			expected: []mockCall{
				{name: "DeleteSchedule", node: nodeName},
				{name: "Uncordon", node: nodeName},
			},
		},
		{
			name:       "WithBadConditionsAlreadyCordonedByDrainoAndMaxRetryRestart",
			conditions: []string{"KernelPanic"},
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:        nodeName,
					Annotations: map[string]string{drainoConditionsAnnotationKey: "KernelPanic=True,0s", drainRetryFailedAnnotationKey: drainRetryRestartAnnotationValue},
				},
				Spec: core.NodeSpec{Unschedulable: true},
				Status: core.NodeStatus{
					Conditions: []core.NodeCondition{
						{
							Type:   "KernelPanic",
							Status: core.ConditionTrue,
						},
						{
							Type:    ConditionDrainedScheduled,
							Message: "[7] | Drain activity scheduled 2020-03-20T15:50:34+01:00",
							Status:  core.ConditionTrue,
						},
					},
				},
			},
			expected: []mockCall{
				{name: "DeleteSchedule", node: nodeName},
				{name: "Uncordon", node: nodeName},
				{name: "ResetRetryAnnotation", node: nodeName},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kclient := fake.NewSimpleClientset(tc.obj)
			store, closeCh := RunStoreForTest(kclient)
			defer closeCh()
			cordonDrainer := &mockCordonDrainer{}
			h := NewDrainingResourceEventHandler(kclient, cordonDrainer, store, &record.FakeRecorder{}, WithDrainBuffer(0*time.Second), WithConditionsFilter(tc.conditions))
			h.drainScheduler = cordonDrainer
			h.OnUpdate(nil, tc.obj)

			if !reflect.DeepEqual(tc.expected, cordonDrainer.calls) {
				t.Errorf("cordonDrainer.calls: want %#v\ngot %#v", tc.expected, cordonDrainer.calls)
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
			cordonFilter: func(p core.Pod) (bool, string, error) { return true, "", nil },
			want:         true,
		},
		{
			name:         "no Pods, Filter false",
			cordonFilter: func(p core.Pod) (bool, string, error) { return false, "", nil },
			want:         true,
		},
		{
			name:         "Pods, Filter true",
			pods:         []runtime.Object{pod, otherPod},
			cordonFilter: func(p core.Pod) (bool, string, error) { return true, "", nil },
			want:         true,
		},
		{
			name:         "Pods, Filter false",
			pods:         []runtime.Object{pod, otherPod},
			cordonFilter: func(p core.Pod) (bool, string, error) { return false, "", nil },
			want:         false,
		},
		{
			name:         "Pods on other node, Filter false",
			pods:         []runtime.Object{otherPod},
			cordonFilter: func(p core.Pod) (bool, string, error) { return false, "", nil },
			want:         true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kclient := fake.NewSimpleClientset(tt.pods...)
			store, closeCh := RunStoreForTest(kclient)
			defer closeCh()

			h := &DrainingResourceEventHandler{
				logger:        zap.NewNop(),
				eventRecorder: &record.FakeRecorder{},
				objectsStore:  store,
				cordonFilter:  tt.cordonFilter,
			}
			if got := h.checkCordonFilters(node); got != tt.want {
				t.Errorf("checkCordonFilters() = %v, want %v", got, tt.want)
			}
		})
	}
}
