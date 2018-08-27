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
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

const (
	ns            = "coolNamespace"
	nodeName      = "coolNode"
	podName       = "coolPod"
	daemonsetName = "coolDaemonSet"
)

var podGracePeriodSeconds int64 = 10
var isController = true

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
			name: "FilterMirrorPod",
			node: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
			reactions: []reactor{
				reactor{
					verb:     "list",
					resource: "pods",
					ret: &core.PodList{Items: []core.Pod{
						core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName}},
						core.Pod{ObjectMeta: meta.ObjectMeta{
							Name:        "mirrorPod",
							Annotations: map[string]string{core.MirrorPodAnnotationKey: "true"},
						}},
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
			name: "FilterDaemonSetPod",
			node: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
			reactions: []reactor{
				reactor{
					verb:     "list",
					resource: "pods",
					ret: &core.PodList{Items: []core.Pod{
						core.Pod{
							ObjectMeta: meta.ObjectMeta{
								Name: "daemonsetPod",
								OwnerReferences: []meta.OwnerReference{meta.OwnerReference{
									Controller: &isController,
									Kind:       kindDaemonSet,
								}},
							},
							Spec: core.PodSpec{TerminationGracePeriodSeconds: &podGracePeriodSeconds},
						},
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
			name: "EvictOrphanedDaemonSetPod",
			node: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
			reactions: []reactor{
				reactor{
					verb:     "list",
					resource: "pods",
					ret: &core.PodList{Items: []core.Pod{
						core.Pod{
							ObjectMeta: meta.ObjectMeta{
								Name:      "orphanedDaemonsetPod",
								Namespace: ns,
								OwnerReferences: []meta.OwnerReference{meta.OwnerReference{
									Controller: &isController,
									Kind:       kindDaemonSet,
									Name:       daemonsetName,
								}},
							},
							Spec: core.PodSpec{TerminationGracePeriodSeconds: &podGracePeriodSeconds},
						},
					}},
				},
				reactor{
					verb:     "get",
					resource: "daemonsets",
					err:      apierrors.NewNotFound(schema.GroupResource{Resource: "daemonsets"}, daemonsetName),
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
			name: "ErrorGettingDaemonset",
			node: &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
			reactions: []reactor{
				reactor{
					verb:     "list",
					resource: "pods",
					ret: &core.PodList{Items: []core.Pod{
						core.Pod{
							ObjectMeta: meta.ObjectMeta{
								Name:      "orphanedDaemonsetPod",
								Namespace: ns,
								OwnerReferences: []meta.OwnerReference{meta.OwnerReference{
									Controller: &isController,
									Kind:       kindDaemonSet,
									Name:       daemonsetName,
								}},
							},
							Spec: core.PodSpec{TerminationGracePeriodSeconds: &podGracePeriodSeconds},
						},
					}},
				},
				reactor{
					verb:     "get",
					resource: "daemonsets",
					err:      errors.New("nope"),
				},
			},
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
			c := &fake.Clientset{}
			for _, r := range tc.reactions {
				c.AddReactor(r.verb, r.resource, r.Fn())
			}

			o := tc.options
			o = append(o, WithPodFilter(NewPodFilters(MirrorPodFilter, NewDaemonSetPodFilter(c))))
			d := NewAPICordonDrainer(c, o...)
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
