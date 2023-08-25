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
	"reflect"
	"testing"
	"time"

	"github.com/pkg/errors"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

const (
	nodeName = "coolNode"
	podName  = "coolPod"

	daemonsetName   = "coolDaemonSet"
	statefulsetName = "coolStatefulSet"
	deploymentName  = "coolDeployment"
	kindDeployment  = "Deployment"
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
			return true, nil, errors.Errorf("incorrect subresource: %v", a.GetSubresource())
		}
		return true, r.ret, r.err
	}
}

func newFakeClientSet(rs ...reactor) kubernetes.Interface {
	cs := &fake.Clientset{}
	for _, r := range rs {
		cs.AddReactor(r.verb, r.resource, r.Fn())
	}
	return cs
}

func TestCordon(t *testing.T) {
	cases := []struct {
		name      string
		node      *core.Node
		mutators  []nodeMutatorFn
		expected  *core.Node
		reactions []reactor
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
			d := NewAPICordonDrainer(c)
			if err := d.Cordon(tc.node, tc.mutators...); err != nil {
				for _, r := range tc.reactions {
					if errors.Cause(err) == r.err {
						return
					}
				}
				t.Errorf("d.Cordon(%v): %v", tc.node.Name, err)
			}
			{
				n, err := c.CoreV1().Nodes().Get(context.TODO(), tc.node.GetName(), meta.GetOptions{})
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
			d := NewAPICordonDrainer(c)
			if err := d.Uncordon(tc.node, tc.mutators...); err != nil {
				for _, r := range tc.reactions {
					if errors.Cause(err) == r.err {
						return
					}
				}
				t.Errorf("d.Uncordon(%v): %v", tc.node.Name, err)
			}
			{
				n, err := c.CoreV1().Nodes().Get(context.TODO(), tc.node.GetName(), meta.GetOptions{})
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
	cases := []struct {
		name      string
		options   []APICordonDrainerOption
		node      *core.Node
		reactions []reactor
		errFn     func(err error) bool
	}{
		{
			name: "EvictOnePod",
			node: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
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
					err:      apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, podName),
				},
			},
		},
		{
			name:    "PodDisappearsBeforeEviction",
			node:    &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
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
					err:         apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, podName),
				},
			},
		},
		{
			name: "ErrorEvictingPod",
			node: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
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
			node:    &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
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
			errFn: IsTimeout,
		},
		{
			name: "EvictedPodReplacedWithDifferentUID",
			node: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
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
			node: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
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
			node: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
			options: []APICordonDrainerOption{WithPodFilter(func(p core.Pod) (bool, error) {
				if p.GetName() == "lamePod" {
					// This pod does not pass the filter.
					return false, nil
				}
				return true, nil
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
			node: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
			options: []APICordonDrainerOption{WithPodFilter(func(p core.Pod) (bool, error) {
				if p.GetName() == "explodeyPod" {
					return false, errExploded
				}
				return true, nil
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
			errFn: func(err error) bool { return errors.Cause(err) == errExploded },
		},
		{
			name: "ErrorListingPods",
			node: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
			reactions: []reactor{
				reactor{
					verb:     "list",
					resource: "pods",
					err:      errors.New("nope"),
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newFakeClientSet(tc.reactions...)
			d := NewAPICordonDrainer(c, tc.options...)
			if err := d.Drain(tc.node); err != nil {
				for _, r := range tc.reactions {
					if errors.Cause(err) == r.err {
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

func TestMarkDrain(t *testing.T) {
	now := meta.Time{Time: time.Now()}
	cases := []struct {
		name     string
		node     *core.Node
		isMarked bool
	}{
		{
			name:     "markDrain",
			node:     &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
			isMarked: false,
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
							Message:            "Drain activity scheduled",
						},
					},
				},
			},
			isMarked: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewSimpleClientset(tc.node)
			d := NewAPICordonDrainer(c)
			{
				n, err := c.CoreV1().Nodes().Get(context.TODO(), tc.node.GetName(), meta.GetOptions{})
				if err != nil {
					t.Errorf("node.Get(%v): %v", tc.node.Name, err)
				}
				if IsMarkedForDrain(n) != tc.isMarked {
					t.Errorf("node %v initial mark is not correct", tc.node.Name)
				}
			}
			if err := d.MarkDrain(tc.node, time.Now(), time.Time{}, false); err != nil {
				t.Errorf("d.MarkDrain(%v): %v", tc.node.Name, err)
			}
			{
				n, err := c.CoreV1().Nodes().Get(context.TODO(), tc.node.GetName(), meta.GetOptions{})
				if err != nil {
					t.Errorf("node.Get(%v): %v", tc.node.Name, err)
				}
				if !IsMarkedForDrain(n) {
					t.Errorf("node %v is not marked for drain", tc.node.Name)
				}
			}
		})
	}
}
