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
	"errors"
	"testing"

	v1 "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestPodFilters(t *testing.T) {
	cases := []struct {
		name              string
		filterBuilderFunc func(obj ...runtime.Object) PodFilterFunc
		objects           []runtime.Object
		pod               core.Pod
		passesFilter      bool
		errFn             func(err error) bool
	}{
		{
			name: "IsMirror",
			pod: core.Pod{
				ObjectMeta: meta.ObjectMeta{
					Name:        podName,
					Annotations: map[string]string{core.MirrorPodAnnotationKey: "definitelyahash"},
				},
			},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc { return MirrorPodFilter },
			passesFilter:      false,
		},
		{
			name:              "IsNotMirror",
			pod:               core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName}},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc { return MirrorPodFilter },
			passesFilter:      true,
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
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc { return LocalStoragePodFilter },
			passesFilter:      false,
		},
		{
			name: "DoesNotHaveLocalStorage",
			pod: core.Pod{
				ObjectMeta: meta.ObjectMeta{Name: podName},
				Spec: core.PodSpec{
					Volumes: []core.Volume{core.Volume{VolumeSource: core.VolumeSource{HostPath: &core.HostPathVolumeSource{}}}},
				},
			},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc { return LocalStoragePodFilter },
			passesFilter:      true,
		},
		{
			name: "Unreplicated",
			pod:  core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName}},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc {
				return NewPodControlledByFilter([]*meta.APIResource{nil})
			},
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
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc {
				return NewPodControlledByFilter([]*meta.APIResource{nil})
			},
			passesFilter: true,
		},
		{
			name: "UnreplicatedButSucceeded",
			pod: core.Pod{
				ObjectMeta: meta.ObjectMeta{Name: podName},
				Status:     core.PodStatus{Phase: core.PodSucceeded},
			},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc {
				return NewPodControlledByFilter([]*meta.APIResource{nil})
			},
			passesFilter: true,
		},
		{
			name: "UnreplicatedButFailed",
			pod: core.Pod{
				ObjectMeta: meta.ObjectMeta{Name: podName},
				Status:     core.PodStatus{Phase: core.PodFailed},
			},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc {
				return NewPodControlledByFilter([]*meta.APIResource{nil})
			},
			passesFilter: true,
		},
		{
			name: "PartOfDaemonSet",
			pod: core.Pod{
				ObjectMeta: meta.ObjectMeta{
					Name: podName,
					OwnerReferences: []meta.OwnerReference{meta.OwnerReference{
						Controller: &isController,
						Kind:       KindDaemonSet,
						Name:       daemonsetName,
						APIVersion: "apps/v1",
					}},
				},
			},
			objects: []runtime.Object{
				&v1.DaemonSet{
					ObjectMeta: meta.ObjectMeta{
						Name: daemonsetName,
					},
				},
			},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc {
				return NewPodControlledByFilter([]*meta.APIResource{{
					Name:    "daemonsets",
					Group:   "apps",
					Version: "v1",
					Kind:    KindDaemonSet,
				}})
			},
			passesFilter: false,
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
						APIVersion: "apps/v1",
					}},
				},
			},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc {
				return NewPodControlledByFilter([]*meta.APIResource{{
					Name:    "daemonsets",
					Group:   "apps",
					Version: "v1",
					Kind:    KindDaemonSet,
				}})
			},
			passesFilter: true,
		},
		{
			name: "PartOfStatefulSet",
			pod: core.Pod{
				ObjectMeta: meta.ObjectMeta{
					Name: podName,
					OwnerReferences: []meta.OwnerReference{meta.OwnerReference{
						Controller: &isController,
						Kind:       KindStatefulSet,
						Name:       statefulsetName,
						APIVersion: "apps/v1",
					}},
				},
			},
			objects: []runtime.Object{
				&v1.StatefulSet{
					ObjectMeta: meta.ObjectMeta{
						Name: statefulsetName,
					},
				},
			},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc {
				return NewPodControlledByFilter([]*meta.APIResource{{
					Name:    "statefulsets",
					Group:   "apps",
					Version: "v1",
					Kind:    KindStatefulSet,
				}})
			},
			passesFilter: false,
		},
		{
			name: "PartOfStatefulSet - multiple filters",
			pod: core.Pod{
				ObjectMeta: meta.ObjectMeta{
					Name: podName,
					OwnerReferences: []meta.OwnerReference{meta.OwnerReference{
						Controller: &isController,
						Kind:       KindStatefulSet,
						Name:       statefulsetName,
						APIVersion: "apps/v1",
					}},
				},
			},
			objects: []runtime.Object{
				&v1.StatefulSet{
					ObjectMeta: meta.ObjectMeta{
						Name: statefulsetName,
					},
				},
			},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc {
				return NewPodControlledByFilter([]*meta.APIResource{nil, {
					Name:    "daemonsets",
					Group:   "apps",
					Version: "v1",
					Kind:    KindDaemonSet,
				}, {
					Name:    "statefulsets",
					Group:   "apps",
					Version: "v1",
					Kind:    KindStatefulSet,
				}})
			},
			passesFilter: false,
		},
		{
			name: "NotPartOfStatefulSet",
			pod: core.Pod{
				ObjectMeta: meta.ObjectMeta{
					Name: podName,
					OwnerReferences: []meta.OwnerReference{meta.OwnerReference{
						Controller: &isController,
						Kind:       kindDeployment,
						Name:       deploymentName,
						APIVersion: "apps/v1",
					}},
				},
			},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc {
				return NewPodControlledByFilter([]*meta.APIResource{{
					Name:    "statefulsets",
					Group:   "apps",
					Version: "v1",
					Kind:    KindStatefulSet,
				}})
			},
			passesFilter: true,
		},
		{
			name: "NotPartOfStatefulSet - multiple filters",
			pod: core.Pod{
				ObjectMeta: meta.ObjectMeta{
					Name: podName,
					OwnerReferences: []meta.OwnerReference{meta.OwnerReference{
						Controller: &isController,
						Kind:       kindDeployment,
						Name:       deploymentName,
						APIVersion: "apps/v1",
					}},
				},
			},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc {
				return NewPodControlledByFilter([]*meta.APIResource{nil, {
					Name:    "daemonsets",
					Group:   "apps",
					Version: "v1",
					Kind:    KindDaemonSet,
				}, {
					Name:    "statefulsets",
					Group:   "apps",
					Version: "v1",
					Kind:    KindStatefulSet,
				}})
			},
			passesFilter: true,
		},
		{
			name: "NoProtectionFromPodEviction",
			pod: core.Pod{
				ObjectMeta: meta.ObjectMeta{
					Name:        podName,
					Annotations: map[string]string{"Random": "true"},
				},
			},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc { return UnprotectedPodFilter() },
			passesFilter:      true,
		},
		{
			name: "NoPodAnnotations",
			pod: core.Pod{
				ObjectMeta: meta.ObjectMeta{
					Name: podName,
				},
			},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc { return UnprotectedPodFilter("Protect") },
			passesFilter:      true,
		},
		{
			name: "NoPodAnnotationsWithEmptyUserValue",
			pod: core.Pod{
				ObjectMeta: meta.ObjectMeta{
					Name: podName,
				},
			},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc { return UnprotectedPodFilter("Protect=") },
			passesFilter:      true,
		},
		{
			name: "NoMatchingProtectionAnnotations",
			pod: core.Pod{
				ObjectMeta: meta.ObjectMeta{
					Name:        podName,
					Annotations: map[string]string{"Useless": "true"},
				},
			},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc { return UnprotectedPodFilter("Protect", "ProtectTwo=true") },
			passesFilter:      true,
		},
		{
			name: "AltNoMatchingProtectionAnnotations",
			pod: core.Pod{
				ObjectMeta: meta.ObjectMeta{
					Name:        podName,
					Annotations: map[string]string{"NeedsAValue": ""},
				},
			},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc {
				return UnprotectedPodFilter("Protect", "ProtectTwo=true", "NeedsAValue=true")
			},
			passesFilter: true,
		},
		{
			name: "KeyOnlyProtectionAnnotation",
			pod: core.Pod{
				ObjectMeta: meta.ObjectMeta{
					Name:        podName,
					Annotations: map[string]string{"Protect": ""},
				},
			},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc { return UnprotectedPodFilter("Protect") },
			passesFilter:      false,
		},
		{
			name: "MultipleKeyOnlyProtectionAnnotations",
			pod: core.Pod{
				ObjectMeta: meta.ObjectMeta{
					Name:        podName,
					Annotations: map[string]string{"ProtectTwo": ""},
				},
			},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc { return UnprotectedPodFilter("ProtectOne", "ProtectTwo") },
			passesFilter:      false,
		},
		{
			name: "SingleProtectionAnnotation",
			pod: core.Pod{
				ObjectMeta: meta.ObjectMeta{
					Name:        podName,
					Annotations: map[string]string{"Protect": "true"},
				},
			},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc { return UnprotectedPodFilter("Protect=true") },
			passesFilter:      false,
		},
		{
			name: "MultipleProtectionAnnotations",
			pod: core.Pod{
				ObjectMeta: meta.ObjectMeta{
					Name:        podName,
					Annotations: map[string]string{"ProtectTwo": "true"},
				},
			},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc {
				return UnprotectedPodFilter("ProtectOne=true", "ProtectTwo=true")
			},
			passesFilter: false,
		},
		{
			name: "MultipleMixedProtectionAnnotations",
			pod: core.Pod{
				ObjectMeta: meta.ObjectMeta{
					Name:        podName,
					Annotations: map[string]string{"ProtectTwo": ""},
				},
			},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc {
				return UnprotectedPodFilter("ProtectOne=true", "ProtectTwo")
			},
			passesFilter: false,
		},
		{
			name: "AltMultipleMixedProtectionAnnotations",
			pod: core.Pod{
				ObjectMeta: meta.ObjectMeta{
					Name:        podName,
					Annotations: map[string]string{"ProtectOne": "true"},
				},
			},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc {
				return UnprotectedPodFilter("ProtectOne", "ProtectTwo=true")
			},
			passesFilter: false,
		},
		{
			name:              "NoFiltersProvided",
			pod:               core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName}},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc { return NewPodFilters() },
			passesFilter:      true,
		},
		{
			name: "AllFiltersPass",
			pod:  core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName}},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc {
				return NewPodFilters(
					func(_ core.Pod) (bool, string, error) { return true, "", nil },
					func(_ core.Pod) (bool, string, error) { return true, "", nil },
				)
			},
			passesFilter: true,
		},
		{
			name: "OneFilterFails",
			pod:  core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName}},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc {
				return NewPodFilters(
					func(_ core.Pod) (bool, string, error) { return true, "", nil },
					func(_ core.Pod) (bool, string, error) { return false, "", nil },
				)
			},
			passesFilter: false,
		},
		{
			name: "OneFilterErrors",
			pod:  core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName}},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc {
				return NewPodFilters(
					func(_ core.Pod) (bool, string, error) { return true, "", nil },
					func(_ core.Pod) (bool, string, error) { return false, "", errExploded },
				)
			},
			errFn: func(err error) bool { return errors.Is(err, errExploded) },
		},
		{
			name: "ControlledBySomething",
			pod: core.Pod{
				ObjectMeta: meta.ObjectMeta{
					Name: podName,
					OwnerReferences: []meta.OwnerReference{meta.OwnerReference{
						Controller: &isController,
						Kind:       "Something",
						Name:       "some-name",
						APIVersion: "agroup/v3",
					}},
				},
			},
			objects: []runtime.Object{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "agroup/v3",
						"kind":       "Something",
						"metadata": map[string]interface{}{
							"name": "some-name",
						},
					},
				},
			},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc {
				return NewPodControlledByFilter([]*meta.APIResource{{
					Name:    "somethings",
					Group:   "agroup",
					Version: "v3",
					Kind:    "Something",
				}})
			},
			passesFilter: false,
		},
		{
			name: "podHasAnyOfTheAnnotations - empty list",
			pod:  core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName}},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc {
				return podHasAnyOfTheAnnotations([]string{}...)
			},
			passesFilter: false,
		},
		{
			name: "podHasAnyOfTheAnnotations - non empty list - no annotation",
			pod:  core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName}},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc {
				return podHasAnyOfTheAnnotations([]string{"test=1"}...)
			},
			passesFilter: false,
		},
		{
			name: "podHasAnyOfTheAnnotations - match",
			pod:  core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName, Annotations: map[string]string{"test": "1"}}},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc {
				return podHasAnyOfTheAnnotations([]string{"test=1"}...)
			},
			passesFilter: true,
		},
		{
			name: "podHasAnyOfTheAnnotations - match key not value",
			pod:  core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName, Annotations: map[string]string{"test": "1"}}},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc {
				return podHasAnyOfTheAnnotations([]string{"test=2"}...)
			},
			passesFilter: false,
		},
		{
			name: "podHasAnyOfTheAnnotations - match key empty value",
			pod:  core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName, Annotations: map[string]string{"test": ""}}},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc {
				return podHasAnyOfTheAnnotations([]string{"test="}...)
			},
			passesFilter: true,
		},
		{
			name: "podHasAnyOfTheAnnotations - match key empty value no equal sign",
			pod:  core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName, Annotations: map[string]string{"test": ""}}},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc {
				return podHasAnyOfTheAnnotations([]string{"test"}...)
			},
			passesFilter: true,
		},
		{
			name: "podHasAnyOfTheAnnotations - match one in list",
			pod:  core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName, Annotations: map[string]string{"test": "1", "foo": "bar", "other": "value"}}},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc {
				return podHasAnyOfTheAnnotations([]string{"aaa=bbb", "test=1"}...)
			},
			passesFilter: true,
		},
		{
			name: "podHasAnyOfTheAnnotations - no match in list",
			pod:  core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName, Annotations: map[string]string{"test": "1", "foo": "bar", "other": "value"}}},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc {
				return podHasAnyOfTheAnnotations([]string{"test", "whatever"}...)
			},
			passesFilter: false,
		},
		{
			name: "NewPodFiltersWithOptInFirst - no opt-in and filter true",
			pod:  core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName, Annotations: map[string]string{"test": "1", "foo": "bar", "other": "value"}}},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc {
				return NewPodFiltersWithOptInFirst(UserOptInViaPodAnnotation(nil...),
					func(p core.Pod) (pass bool, reason string, err error) { return true, "", nil })
			},
			passesFilter: true,
		},
		{
			name: "NewPodFiltersWithOptInFirst - no opt-in and filter false",
			pod:  core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName, Annotations: map[string]string{"test": "1", "foo": "bar", "other": "value"}}},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc {
				return NewPodFiltersWithOptInFirst(UserOptInViaPodAnnotation(nil...),
					func(p core.Pod) (pass bool, reason string, err error) { return false, "", nil })
			},
			passesFilter: false,
		},
		{
			name: "NewPodFiltersWithOptInFirst - opt-in and filter false",
			pod:  core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName, Annotations: map[string]string{"test": "1", "foo": "bar", "other": "value"}}},
			filterBuilderFunc: func(obj ...runtime.Object) PodFilterFunc {
				return NewPodFiltersWithOptInFirst(UserOptInViaPodAnnotation([]string{"foo=bar"}...),
					func(p core.Pod) (pass bool, reason string, err error) { return false, "", nil })
			},
			passesFilter: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			filter := tc.filterBuilderFunc(tc.objects...)
			passesFilter, _, err := filter(tc.pod)
			if err != nil && tc.errFn != nil && !tc.errFn(err) {
				t.Errorf("tc.filter(%v): %v", tc.pod.GetName(), err)
			}
			if passesFilter != tc.passesFilter {
				t.Errorf("tc.filter(%v): want %v, got %v", tc.pod.GetName(), tc.passesFilter, passesFilter)
			}
		})
	}
}
