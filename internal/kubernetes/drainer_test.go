package kubernetes

import (
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
	ns = "coolNamespace"

	nodeName = "coolNode"
	podName  = "coolPod"

	daemonsetName  = "coolDaemonSet"
	deploymentName = "coolDeployment"
	kindDeployment = "Deployment"
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
		reactions []reactor
	}{
		{
			name: "CordonSchedulableNode",
			node: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
			reactions: []reactor{
				reactor{
					verb:     "get",
					resource: "nodes",
					ret: &core.Node{
						ObjectMeta: meta.ObjectMeta{Name: nodeName},
						Spec:       core.NodeSpec{Unschedulable: false},
					},
				},
				reactor{
					verb:     "update",
					resource: "nodes",
					ret: &core.Node{
						ObjectMeta: meta.ObjectMeta{Name: nodeName},
						Spec:       core.NodeSpec{Unschedulable: true},
					},
				},
			},
		},
		{
			name: "CordonUnschedulableNode",
			node: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
			reactions: []reactor{
				reactor{
					verb:     "get",
					resource: "nodes",
					ret: &core.Node{
						ObjectMeta: meta.ObjectMeta{Name: nodeName},
						Spec:       core.NodeSpec{Unschedulable: true},
					},
				},
			},
		},
		{
			name: "CordonNonExistentNode",
			node: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
			reactions: []reactor{
				reactor{verb: "get", resource: "nodes", err: errors.New("nope")},
			},
		},
		{
			name: "ErrorCordoningSchedulableNode",
			node: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
			reactions: []reactor{
				reactor{
					verb:     "get",
					resource: "nodes",
					ret: &core.Node{
						ObjectMeta: meta.ObjectMeta{Name: nodeName},
						Spec:       core.NodeSpec{Unschedulable: false},
					},
				},
				reactor{verb: "update", resource: "nodes", err: errors.New("nope")},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &fake.Clientset{}
			for _, r := range tc.reactions {
				c.AddReactor(r.verb, r.resource, r.Fn())
			}

			d := NewAPICordonDrainer(c)
			if err := d.Cordon(tc.node); err != nil {
				for _, r := range tc.reactions {
					if errors.Cause(err) == r.err {
						return
					}
				}
				t.Errorf("d.Cordon(%v): %v", tc.node.Name, err)
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
