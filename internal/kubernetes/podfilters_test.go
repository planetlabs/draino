package kubernetes

import (
	"testing"

	"github.com/pkg/errors"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestPodFilters(t *testing.T) {
	cases := []struct {
		name         string
		filter       PodFilterFunc
		pod          core.Pod
		passesFilter bool
		errFn        func(err error) bool
	}{
		{
			name: "IsMirror",
			pod: core.Pod{
				ObjectMeta: meta.ObjectMeta{
					Name:        podName,
					Annotations: map[string]string{core.MirrorPodAnnotationKey: "definitelyahash"},
				},
			},
			filter:       MirrorPodFilter,
			passesFilter: false,
		},
		{
			name:         "IsNotMirror",
			pod:          core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName}},
			filter:       MirrorPodFilter,
			passesFilter: true,
		},
		{
			name: "HasLocalStorage",
			pod: core.Pod{
				ObjectMeta: meta.ObjectMeta{Name: podName},
				Spec: core.PodSpec{
					Volumes: []core.Volume{
						core.Volume{VolumeSource: core.VolumeSource{HostPath: &core.HostPathVolumeSource{}}},
						core.Volume{VolumeSource: core.VolumeSource{EmptyDir: &core.EmptyDirVolumeSource{}}},
					},
				},
			},
			filter:       LocalStoragePodFilter,
			passesFilter: false,
		},
		{
			name: "DoesNotHaveLocalStorage",
			pod: core.Pod{
				ObjectMeta: meta.ObjectMeta{Name: podName},
				Spec: core.PodSpec{
					Volumes: []core.Volume{core.Volume{VolumeSource: core.VolumeSource{HostPath: &core.HostPathVolumeSource{}}}},
				},
			},
			filter:       LocalStoragePodFilter,
			passesFilter: true,
		},
		{
			name:         "Unreplicated",
			pod:          core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName}},
			filter:       UnreplicatedPodFilter,
			passesFilter: false,
		},
		{
			name: "Replicated",
			pod: core.Pod{
				ObjectMeta: meta.ObjectMeta{
					Name: podName,
					OwnerReferences: []meta.OwnerReference{meta.OwnerReference{
						Controller: &isController,
						Kind:       kindDeployment,
						Name:       deploymentName,
					}},
				},
			},
			filter:       UnreplicatedPodFilter,
			passesFilter: true,
		},
		{
			name: "UnreplicatedButSucceeded",
			pod: core.Pod{
				ObjectMeta: meta.ObjectMeta{Name: podName},
				Status:     core.PodStatus{Phase: core.PodSucceeded},
			},
			filter:       UnreplicatedPodFilter,
			passesFilter: true,
		},
		{
			name: "UnreplicatedButFailed",
			pod: core.Pod{
				ObjectMeta: meta.ObjectMeta{Name: podName},
				Status:     core.PodStatus{Phase: core.PodFailed},
			},
			filter:       UnreplicatedPodFilter,
			passesFilter: true,
		},
		{
			name: "PartOfDaemonSet",
			pod: core.Pod{
				ObjectMeta: meta.ObjectMeta{
					Name: podName,
					OwnerReferences: []meta.OwnerReference{meta.OwnerReference{
						Controller: &isController,
						Kind:       kindDaemonSet,
						Name:       daemonsetName,
					}},
				},
			},
			filter:       NewDaemonSetPodFilter(newFakeClientSet(reactor{verb: "get", resource: "daemonsets"})),
			passesFilter: false,
		},
		{
			name: "ErrorGettingDaemonSet",
			pod: core.Pod{
				ObjectMeta: meta.ObjectMeta{
					Name: podName,
					OwnerReferences: []meta.OwnerReference{meta.OwnerReference{
						Controller: &isController,
						Kind:       kindDaemonSet,
						Name:       daemonsetName,
					}},
				},
			},
			filter: NewDaemonSetPodFilter(newFakeClientSet(reactor{
				verb:     "get",
				resource: "daemonsets",
				err:      errExploded,
			})),
			errFn: func(err error) bool { return errors.Cause(err) == errExploded },
		},
		{
			name: "OrphanedFromDaemonSet",
			pod: core.Pod{
				ObjectMeta: meta.ObjectMeta{
					Name: podName,
					OwnerReferences: []meta.OwnerReference{meta.OwnerReference{
						Controller: &isController,
						Kind:       kindDaemonSet,
						Name:       daemonsetName,
					}},
				},
			},
			filter: NewDaemonSetPodFilter(newFakeClientSet(reactor{
				verb:     "get",
				resource: "daemonsets",
				err:      apierrors.NewNotFound(schema.GroupResource{Resource: "daemonsets"}, daemonsetName),
			})),
			passesFilter: true,
		},
		{
			name: "NotPartOfDaemonSet",
			pod: core.Pod{
				ObjectMeta: meta.ObjectMeta{
					Name: podName,
					OwnerReferences: []meta.OwnerReference{meta.OwnerReference{
						Controller: &isController,
						Kind:       kindDeployment,
						Name:       deploymentName,
					}},
				},
			},
			filter:       NewDaemonSetPodFilter(newFakeClientSet()),
			passesFilter: true,
		},
		{
			name:         "NoFiltersProvided",
			pod:          core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName}},
			filter:       NewPodFilters(),
			passesFilter: true,
		},
		{
			name: "AllFiltersPass",
			pod:  core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName}},
			filter: NewPodFilters(
				func(_ core.Pod) (bool, error) { return true, nil },
				func(_ core.Pod) (bool, error) { return true, nil },
			),
			passesFilter: true,
		},
		{
			name: "OneFilterFails",
			pod:  core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName}},
			filter: NewPodFilters(
				func(_ core.Pod) (bool, error) { return true, nil },
				func(_ core.Pod) (bool, error) { return false, nil },
			),
			passesFilter: false,
		},
		{
			name: "OneFilterErrors",
			pod:  core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName}},
			filter: NewPodFilters(
				func(_ core.Pod) (bool, error) { return true, nil },
				func(_ core.Pod) (bool, error) { return false, errExploded },
			),
			errFn: func(err error) bool { return errors.Cause(err) == errExploded },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			passesFilter, err := tc.filter(tc.pod)
			if err != nil && tc.errFn != nil && !tc.errFn(err) {
				t.Errorf("tc.filter(%v): %v", tc.pod.GetName(), err)
			}
			if passesFilter != tc.passesFilter {
				t.Errorf("tc.filter(%v): want %v, got %v", tc.pod.GetName(), tc.passesFilter, passesFilter)
			}
		})
	}
}
