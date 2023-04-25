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
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	core "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"

	//"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

const (
	nodeName = "coolNode"
	podName  = "coolPod"

	daemonsetName   = "coolDaemonSet"
	statefulsetName = "coolStatefulSet"
	deploymentName  = "coolDeployment"
	kindDeployment  = "Deployment"
	kindReplicaSet  = "ReplicaSet"
)

var (
	_ CordonDrainer = (*APICordonDrainer)(nil)
	_ CordonDrainer = (*NoopCordonDrainer)(nil)
)

var podGracePeriodSeconds int64 = 10
var isController = true
var errExploded = errors.New("kaboom")

type reactor struct {
	verb        string
	resource    string
	subresource string
	ret         runtime.Object
	err         error
}

func (r reactor) Fn() clienttesting.ReactionFunc {
	return func(a clienttesting.Action) (bool, runtime.Object, error) {
		if r.subresource != "" && a.GetSubresource() != r.subresource {
			return true, nil, fmt.Errorf("incorrect subresource: %v", a.GetSubresource())
		}
		return true, r.ret, r.err
	}
}

func newFakeClientSet(objects []runtime.Object, rs ...reactor) kubernetes.Interface {
	cs := fake.NewSimpleClientset(objects...)
	for _, r := range rs {
		cs.AddReactor(r.verb, r.resource, r.Fn())
	}
	return cs
}

func newFakeDynamicClient(objects ...runtime.Object) dynamic.Interface {
	scheme := runtime.NewScheme()
	if err := fake.AddToScheme(scheme); err != nil {
		return nil
	}
	return dynamicfake.NewSimpleDynamicClient(scheme, objects...)
}

func TestCordon(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name      string
		node      *core.Node
		mutators  []nodeMutatorFn
		expected  *core.Node
		reactions []reactor
		limiters  map[string]LimiterFunc
	}{
		{
			name: "CordonSchedulableNode",
			node: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
			expected: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Spec:       core.NodeSpec{Unschedulable: true},
			},
		},
		{
			name: "CordonUnschedulableNode",
			node: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Spec:       core.NodeSpec{Unschedulable: true},
			},
			expected: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Spec:       core.NodeSpec{Unschedulable: true},
			},
		},
		{
			name: "CordonNonExistentNode",
			node: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
			reactions: []reactor{
				{verb: "get", resource: "nodes", err: errors.New("nope")},
			},
		},
		{
			name: "ErrorCordoningSchedulableNode",
			node: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
			reactions: []reactor{
				{verb: "update", resource: "nodes", err: errors.New("nope")},
			},
		},
		{
			name: "CordonSchedulableNodeWithMutator",
			node: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
			mutators: []nodeMutatorFn{func(n *core.Node) {
				n.Annotations = map[string]string{"foo": "1"}
			}},
			expected: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName, Annotations: map[string]string{"foo": "1"}},
				Spec:       core.NodeSpec{Unschedulable: true},
			},
		},
		{
			name: "CordonUnschedulableNodeWithMutator",
			node: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Spec:       core.NodeSpec{Unschedulable: true},
			},
			mutators: []nodeMutatorFn{func(n *core.Node) {
				n.Annotations = map[string]string{"foo": "1"}
			}},
			expected: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Spec:       core.NodeSpec{Unschedulable: true},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewSimpleClientset(tc.node)
			for _, r := range tc.reactions {
				c.PrependReactor(r.verb, r.resource, r.Fn())
			}
			d := NewAPICordonDrainer(c, NewEventRecorder(&record.FakeRecorder{}))
			if err := d.Cordon(ctx, tc.node, tc.mutators...); err != nil {
				for _, r := range tc.reactions {
					if errors.Is(err, r.err) {
						return
					}
				}
				t.Errorf("d.Cordon(%v): %v", tc.node.Name, err)
			}
			{
				n, err := c.CoreV1().Nodes().Get(context.Background(), tc.node.GetName(), meta.GetOptions{})
				if err != nil {
					t.Errorf("node.Get(%v): %v", tc.node.Name, err)
				}
				if !reflect.DeepEqual(tc.expected, n) {
					t.Errorf("node.Get(%v): want %#v, got %#v", tc.node.Name, tc.expected, n)
				}
			}
		})
	}
}

func TestUncordon(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name      string
		node      *core.Node
		mutators  []nodeMutatorFn
		expected  *core.Node
		reactions []reactor
	}{
		{
			name:     "UncordonSchedulableNode",
			node:     &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
			expected: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
		},
		{
			name: "UncordonUnschedulableNode",
			node: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Spec:       core.NodeSpec{Unschedulable: true},
			},
			expected: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
		},
		{
			name: "UncordonNonExistentNode",
			node: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
			reactions: []reactor{
				{verb: "get", resource: "nodes", err: errors.New("nope")},
			},
		},
		{
			name: "ErrorUncordoningUnschedulableNode",
			node: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Spec:       core.NodeSpec{Unschedulable: true},
			},
			reactions: []reactor{
				{verb: "update", resource: "nodes", err: errors.New("nope")},
			},
		},
		{
			name: "UncordonSchedulableNodeWithMutator",
			node: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
			mutators: []nodeMutatorFn{func(n *core.Node) {
				n.Annotations = map[string]string{"foo": "1"}
			}},
			expected: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
		},
		{
			name: "UncordonUnschedulableNodeWithMutator",
			node: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Spec:       core.NodeSpec{Unschedulable: true},
			},
			mutators: []nodeMutatorFn{func(n *core.Node) {
				n.Annotations = map[string]string{"foo": "1"}
			}},
			expected: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName, Annotations: map[string]string{"foo": "1"}}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewSimpleClientset(tc.node)
			for _, r := range tc.reactions {
				c.PrependReactor(r.verb, r.resource, r.Fn())
			}
			d := NewAPICordonDrainer(c, NewEventRecorder(&record.FakeRecorder{}))
			if err := d.Uncordon(ctx, tc.node, tc.mutators...); err != nil {
				for _, r := range tc.reactions {
					if errors.Is(err, r.err) {
						return
					}
				}
				t.Errorf("d.Uncordon(%v): %v", tc.node.Name, err)
			}
			{
				n, err := c.CoreV1().Nodes().Get(context.Background(), tc.node.GetName(), meta.GetOptions{})
				if err != nil {
					t.Errorf("node.Get(%v): %v", tc.node.Name, err)
				}
				if !reflect.DeepEqual(tc.expected, n) {
					t.Errorf("node.Get(%v): want %#v, got %#v", tc.node.Name, tc.expected, n)
				}
			}
		})
	}
}

func TestDrain(t *testing.T) {
	ctx := context.Background()
	now := meta.Now()
	cases := []struct {
		name      string
		options   []APICordonDrainerOption
		node      *core.Node
		reactions []reactor
		errFn     func(err error) bool
	}{
		{
			name: "EvictOnePod",
			node: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}, Spec: core.NodeSpec{Unschedulable: true}},
			reactions: []reactor{
				reactor{
					verb:     "list",
					resource: "pods",
					ret: &core.PodList{Items: []core.Pod{
						core.Pod{
							ObjectMeta: meta.ObjectMeta{
								Name: podName,
								OwnerReferences: []meta.OwnerReference{meta.OwnerReference{
									Controller: &isController,
									Kind:       "Deployment",
								}},
							},
							Spec: core.PodSpec{TerminationGracePeriodSeconds: &podGracePeriodSeconds},
						},
					}},
				},
				reactor{
					verb:        "create",
					resource:    "pods",
					subresource: "eviction",
				},
				reactor{
					verb:     "get",
					resource: "pods",
					err:      apierrors.NewNotFound(schema.GroupResource{Resource: "pod"}, podName),
				},
			},
		},
		{
			name: "NodeSchedulableDontDrain",
			node: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}, Spec: core.NodeSpec{Unschedulable: false}},
			errFn: func(err error) bool {
				return errors.Is(err, NodeIsNotCordonError{
					NodeName: nodeName,
				})
			},
		},
		{
			name:    "PodDisappearsBeforeEviction",
			node:    &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}, Spec: core.NodeSpec{Unschedulable: true}},
			options: []APICordonDrainerOption{MaxGracePeriod(1 * time.Second), EvictionHeadroom(1 * time.Second)},
			reactions: []reactor{
				reactor{
					verb:     "list",
					resource: "pods",
					ret: &core.PodList{Items: []core.Pod{
						core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName}},
					}},
				},
				reactor{
					verb:        "create",
					resource:    "pods",
					subresource: "eviction",
					err:         apierrors.NewNotFound(schema.GroupResource{Resource: "pod"}, podName),
				},
			},
		},
		{
			name: "ErrorEvictingPod",
			node: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}, Spec: core.NodeSpec{Unschedulable: true}},
			reactions: []reactor{
				reactor{
					verb:     "list",
					resource: "pods",
					ret: &core.PodList{Items: []core.Pod{
						core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName}},
					}},
				},
				reactor{
					verb:        "create",
					resource:    "pods",
					subresource: "eviction",
					err:         errors.New("nope"),
				},
			},
		},
		{
			name:    "PodEvictionNotAllowed",
			node:    &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}, Spec: core.NodeSpec{Unschedulable: true}},
			options: []APICordonDrainerOption{MaxGracePeriod(1 * time.Second), EvictionHeadroom(1 * time.Second)},
			reactions: []reactor{
				reactor{
					verb:     "list",
					resource: "pods",
					ret: &core.PodList{Items: []core.Pod{
						core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName}},
					}},
				},
				reactor{
					verb:        "create",
					resource:    "pods",
					subresource: "eviction",
					err:         apierrors.NewTooManyRequests("nope", 5),
				},
			},
			errFn: func(err error) bool { return errors.As(err, &PodEvictionTimeoutError{}) },
		},
		{
			name: "EvictedPodReplacedWithDifferentUID",
			node: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}, Spec: core.NodeSpec{Unschedulable: true}},
			reactions: []reactor{
				reactor{
					verb:     "list",
					resource: "pods",
					ret: &core.PodList{Items: []core.Pod{
						core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName, UID: "a"}},
					}},
				},
				reactor{
					verb:        "create",
					resource:    "pods",
					subresource: "eviction",
				},
				reactor{
					verb:     "get",
					resource: "pods",
					ret:      &core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName, UID: "b"}},
				},
			},
		},
		{
			name: "ErrorConfirmingPodDeletion",
			node: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}, Spec: core.NodeSpec{Unschedulable: true}},
			reactions: []reactor{
				reactor{
					verb:     "list",
					resource: "pods",
					ret: &core.PodList{Items: []core.Pod{
						core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName}},
					}},
				},
				reactor{
					verb:        "create",
					resource:    "pods",
					subresource: "eviction",
				},
				reactor{
					verb:     "get",
					resource: "pods",
					err:      errors.New("nope"),
				},
			},
		},
		{
			name: "PodDoesNotPassFilter",
			node: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}, Spec: core.NodeSpec{Unschedulable: true}},
			options: []APICordonDrainerOption{WithPodFilter(func(p core.Pod) (bool, string, error) {
				if p.GetName() == "lamePod" {
					// This pod does not pass the filter.
					return false, "lame", nil
				}
				return true, "", nil
			})},
			reactions: []reactor{
				reactor{
					verb:     "list",
					resource: "pods",
					ret: &core.PodList{Items: []core.Pod{
						core.Pod{ObjectMeta: meta.ObjectMeta{Name: "lamePod"}},
						core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName}},
					}},
				},
				reactor{
					verb:        "create",
					resource:    "pods",
					subresource: "eviction",
				},
				reactor{
					verb:     "get",
					resource: "pods",
					err:      apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, podName),
				},
			},
		},
		{
			name: "PodFilterErrors",
			node: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}, Spec: core.NodeSpec{Unschedulable: true}},
			options: []APICordonDrainerOption{WithPodFilter(func(p core.Pod) (bool, string, error) {
				if p.GetName() == "explodeyPod" {
					return false, "explodey", errExploded
				}
				return true, "", nil
			})},
			reactions: []reactor{
				reactor{
					verb:     "list",
					resource: "pods",
					ret: &core.PodList{Items: []core.Pod{
						core.Pod{ObjectMeta: meta.ObjectMeta{Name: "explodeyPod"}},
						core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName}},
					}},
				},
				reactor{
					verb:        "create",
					resource:    "pods",
					subresource: "eviction",
				},
				reactor{
					verb:     "get",
					resource: "pods",
					err:      apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, podName),
				},
			},
			errFn: func(err error) bool { return errors.Is(err, errExploded) },
		},
		{
			name:    "SkipDrain",
			node:    &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}, Spec: core.NodeSpec{Unschedulable: true}},
			options: []APICordonDrainerOption{WithSkipDrain(true), WithAPICordonDrainerLogger(zap.NewNop())},
		},
		{
			name: "ErrorListingPods",
			node: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}, Spec: core.NodeSpec{Unschedulable: true}},
			reactions: []reactor{
				reactor{
					verb:     "list",
					resource: "pods",
					err:      errors.New("nope"),
				},
			},
		},
		{
			name: "DoNotEvictTerminatingPodButWaitForDeletion",
			node: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}, Spec: core.NodeSpec{Unschedulable: true}},
			reactions: []reactor{{
				verb:     "list",
				resource: "pods",
				ret: &core.PodList{Items: []core.Pod{{ObjectMeta: meta.ObjectMeta{
					Name:              podName,
					DeletionTimestamp: &now}}}},
			}, {
				verb:        "create",
				resource:    "pods",
				subresource: "eviction",
				err:         apierrors.NewTooManyRequestsError("some pdb name"),
			},
				{
					verb:     "get",
					resource: "pods",
					err:      apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, podName),
				}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newFakeClientSet([]runtime.Object{tc.node}, tc.reactions...)
			d := NewAPICordonDrainer(c, NewEventRecorder(&record.FakeRecorder{}), tc.options...)
			if err := d.Drain(ctx, tc.node); err != nil {
				for _, r := range tc.reactions {
					if errors.Is(err, r.err) {
						return
					}
				}
				if tc.errFn != nil && tc.errFn(err) {
					return
				}
				t.Errorf("d.Drain(%v): %v", tc.node.Name, err)
			}
		})
	}
}

func TestGetDrainConditionStatus(t *testing.T) {
	now := meta.Time{Time: time.Now()}
	cases := []struct {
		name        string
		node        *core.Node
		drainStatus DrainConditionStatus
		isErr       bool
	}{
		{
			name:  "conditionStatus0",
			node:  &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
			isErr: false,
		},
		{
			name: "conditionStatus1",
			node: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{
					Conditions: []core.NodeCondition{
						{
							Type:               core.NodeConditionType(ConditionDrainedScheduled),
							Status:             core.ConditionTrue,
							LastHeartbeatTime:  now,
							LastTransitionTime: now,
							Reason:             "Draino",
							Message:            "Drain activity scheduled 2021-11-16T03:50:44Z",
						},
					},
				},
			},
			drainStatus: DrainConditionStatus{Marked: true, Completed: false, Failed: false, FailedCount: 0, LastTransition: now.Time},
			isErr:       false,
		},
		{
			name: "conditionStatus failed(1)",
			node: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{
					Conditions: []core.NodeCondition{
						{
							Type:               core.NodeConditionType(ConditionDrainedScheduled),
							Status:             core.ConditionFalse,
							LastHeartbeatTime:  now,
							LastTransitionTime: now,
							Reason:             "Draino",
							Message:            "[1] | Drain activity scheduled 2020-03-20T15:50:34+01:00 | Failed: 2020-03-20T15:55:50+01:00",
						},
					},
				},
			},
			drainStatus: DrainConditionStatus{Marked: true, Completed: false, Failed: true, FailedCount: 1, LastTransition: now.Time},
			isErr:       false,
		},
		{
			name: "conditionStatus Failed(2)",
			node: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{
					Conditions: []core.NodeCondition{
						{
							Type:               core.NodeConditionType(ConditionDrainedScheduled),
							Status:             core.ConditionFalse,
							LastHeartbeatTime:  now,
							LastTransitionTime: now,
							Reason:             "Draino",
							Message:            "[2] | Drain activity scheduled 2020-03-20T15:50:34+01:00 | Failed: 2020-03-20T15:55:50+01:00",
						},
					},
				},
			},
			drainStatus: DrainConditionStatus{Marked: true, Completed: false, Failed: true, FailedCount: 2, LastTransition: now.Time},
			isErr:       false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewSimpleClientset(tc.node)
			{
				n, err := c.CoreV1().Nodes().Get(context.Background(), tc.node.GetName(), meta.GetOptions{})
				if err != nil {
					t.Errorf("node.Get(%v): %v", tc.node.Name, err)
				}
				drainStatus, err := GetDrainConditionStatus(n)
				if drainStatus != tc.drainStatus {
					t.Errorf("node %v initial drainStatus is not correct", tc.node.Name)
				}
			}
		})
	}
}

func TestMarkDrain(t *testing.T) {
	ctx := context.Background()
	now := meta.Time{Time: time.Now()}
	cases := []struct {
		name        string
		node        *core.Node
		drainStatus DrainConditionStatus
		isErr       bool
	}{
		{
			name:  "markDrain",
			node:  &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
			isErr: false,
		},
		{
			name: "markDrain again",
			node: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{
					Conditions: []core.NodeCondition{
						{
							Type:               core.NodeConditionType(ConditionDrainedScheduled),
							Status:             core.ConditionTrue,
							LastHeartbeatTime:  now,
							LastTransitionTime: now,
							Reason:             "Draino",
							Message:            "Drain activity scheduled 2020-03-20T15:50:34+01:00",
						},
					},
				},
			},
			drainStatus: DrainConditionStatus{Marked: true, Completed: false, Failed: false, FailedCount: 0, LastTransition: now.Time},
			isErr:       false,
		},
		{
			name: "markDrain Failed",
			node: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{
					Conditions: []core.NodeCondition{
						{
							Type:               core.NodeConditionType(ConditionDrainedScheduled),
							Status:             core.ConditionFalse,
							LastHeartbeatTime:  now,
							LastTransitionTime: now,
							Reason:             "Draino",
							Message:            "[1] | Drain activity scheduled 2020-03-20T15:50:34+01:00 | Failed: 2020-03-20T15:55:50+01:00",
						},
					},
				},
			},
			drainStatus: DrainConditionStatus{Marked: true, Completed: false, Failed: true, FailedCount: 1, LastTransition: now.Time},
			isErr:       false,
		},
		{
			name: "markDrain Failed(2)",
			node: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{
					Conditions: []core.NodeCondition{
						{
							Type:               core.NodeConditionType(ConditionDrainedScheduled),
							Status:             core.ConditionFalse,
							LastHeartbeatTime:  now,
							LastTransitionTime: now,
							Reason:             "Draino",
							Message:            "[2] | Drain activity scheduled 2020-03-20T15:50:34+01:00 | Failed: 2020-03-20T15:55:50+01:00",
						},
					},
				},
			},
			drainStatus: DrainConditionStatus{Marked: true, Completed: false, Failed: true, FailedCount: 2, LastTransition: now.Time},
			isErr:       false,
		},
		{
			name: "markDrain Failed(8)",
			node: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{
					Conditions: []core.NodeCondition{
						{
							Type:               core.NodeConditionType(ConditionDrainedScheduled),
							Status:             core.ConditionFalse,
							LastHeartbeatTime:  now,
							LastTransitionTime: now,
							Reason:             "Draino",
							Message:            "[8] | Drain activity scheduled 2020-03-20T15:50:34+01:00 | Failed: 2020-03-20T15:55:50+01:00",
						},
					},
				},
			},
			drainStatus: DrainConditionStatus{Marked: true, Completed: false, Failed: true, FailedCount: 8, LastTransition: now.Time},
			isErr:       false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewSimpleClientset(tc.node)
			d := NewAPICordonDrainer(c, NewEventRecorder(&record.FakeRecorder{}))
			{
				n, err := c.CoreV1().Nodes().Get(context.Background(), tc.node.GetName(), meta.GetOptions{})
				if err != nil {
					t.Errorf("node.Get(%v): %v", tc.node.Name, err)
				}
				drainStatus, err := GetDrainConditionStatus(n)
				if drainStatus != tc.drainStatus {
					t.Errorf("node %v initial drainStatus is not correct", tc.node.Name)
				}
				if !drainStatus.Marked {
					if err := d.MarkDrain(ctx, tc.node, time.Now(), time.Time{}, false, 0); err != nil {
						t.Errorf("d.MarkDrain(%v): %v", tc.node.Name, err)
					}
					{
						n, err := c.CoreV1().Nodes().Get(context.Background(), tc.node.GetName(), meta.GetOptions{})
						if err != nil {
							t.Errorf("node.Get(%v): %v", tc.node.Name, err)
						}
						if drainStatus, err = GetDrainConditionStatus(n); !drainStatus.Marked {
							t.Errorf("node %v is not marked for drain", tc.node.Name)
						}
					}
				}
			}
		})
	}
}

func TestSerializePolicy(t *testing.T) {
	pod := core.Pod{}
	pod.Name = "test-pod"
	pod.Namespace = "test-namespace"

	evictionPayload := &policy.Eviction{
		ObjectMeta: meta.ObjectMeta{Namespace: pod.GetNamespace(), Name: pod.GetName()},
	}

	assert.Equal(t, "{\"kind\":\"Eviction\",\"apiVersion\":\"policy/v1\",\"metadata\":{\"name\":\"test-pod\",\"namespace\":\"test-namespace\",\"creationTimestamp\":null}}\n", string(GetEvictionJsonPayload(evictionPayload).Bytes()))
}

func TestAPICordonDrainer_MarkDrainDelete(t *testing.T) {
	ctx := context.Background()
	someTimeAgo := meta.NewTime(time.Date(1978, time.April, 12, 22, 00, 00, 00, time.UTC))
	tests := []struct {
		name         string
		node         *core.Node
		expectedNode *core.Node
		wantErr      bool
	}{
		{
			name: "Remove drain schedule condition",
			node: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{
					Conditions: []core.NodeCondition{
						{
							Type:               core.NodeConditionType(ConditionDrainedScheduled),
							Status:             core.ConditionTrue,
							LastHeartbeatTime:  someTimeAgo,
							LastTransitionTime: someTimeAgo,
							Reason:             "Draino",
							Message:            "[0] | Drain activity scheduled",
						},
					},
				},
			},
			expectedNode: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{
					Conditions: []core.NodeCondition{},
				},
			},
		},
		{
			name: "Keep other condition than drain schedule",
			node: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{
					Conditions: []core.NodeCondition{
						{
							Type:               core.NodeConditionType("Ready"),
							Status:             core.ConditionTrue,
							LastHeartbeatTime:  someTimeAgo,
							LastTransitionTime: someTimeAgo,
							Reason:             "Whatever",
							Message:            "Message",
						},
						{
							Type:               core.NodeConditionType(ConditionDrainedScheduled),
							Status:             core.ConditionTrue,
							LastHeartbeatTime:  someTimeAgo,
							LastTransitionTime: someTimeAgo,
							Reason:             "Draino",
							Message:            "[0] | Drain activity scheduled",
						},
						{
							Type:               core.NodeConditionType("DifferentType"),
							Status:             core.ConditionTrue,
							LastHeartbeatTime:  someTimeAgo,
							LastTransitionTime: someTimeAgo,
							Reason:             "Draino",
							Message:            "[0] | Drain activity scheduled",
						},
					},
				},
			},
			expectedNode: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{
					Conditions: []core.NodeCondition{
						{
							Type:               core.NodeConditionType("Ready"),
							Status:             core.ConditionTrue,
							LastHeartbeatTime:  someTimeAgo,
							LastTransitionTime: someTimeAgo,
							Reason:             "Whatever",
							Message:            "Message",
						},
						{
							Type:               core.NodeConditionType("DifferentType"),
							Status:             core.ConditionTrue,
							LastHeartbeatTime:  someTimeAgo,
							LastTransitionTime: someTimeAgo,
							Reason:             "Draino",
							Message:            "[0] | Drain activity scheduled",
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &APICordonDrainer{
				c: fake.NewSimpleClientset(tt.node),
			}
			if err := d.MarkDrainDelete(ctx, tt.node); (err != nil) != tt.wantErr {
				t.Errorf("MarkDrainDelete() error = %v, wantErr %v", err, tt.wantErr)
			}
			newNode, err := d.c.CoreV1().Nodes().Get(context.Background(), tt.node.Name, meta.GetOptions{})
			if err != nil {
				t.Errorf("MarkDrainDelete(), can't retrieve node error = %v", err)
			}
			if !reflect.DeepEqual(newNode, tt.expectedNode) {
				t.Errorf("node comparison:\n======= want\n%#v\n======= got\n%#v", *tt.expectedNode, *newNode)
			}
		})
	}
}
