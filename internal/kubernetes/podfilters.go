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
	"strings"

	"k8s.io/apimachinery/pkg/api/errors"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// A PodFilterFunc returns true if the supplied pod passes the filter.
type PodFilterFunc func(p core.Pod) (pass bool, reason string, err error)

// MirrorPodFilter returns true if the supplied pod is not a mirror pod, i.e. a
// pod created by a manifest on the node rather than the API server.
func MirrorPodFilter(p core.Pod) (bool, string, error) {
	_, mirrorPod := p.GetAnnotations()[core.MirrorPodAnnotationKey]
	if !mirrorPod {
		return true, "", nil
	}
	return false, "pod-mirror", nil
}

// LocalStoragePodFilter returns true if the supplied pod does not have local
// storage, i.e. does not use any 'empty dir' volumes.
func LocalStoragePodFilter(p core.Pod) (bool, string, error) {
	for _, v := range p.Spec.Volumes {
		if v.EmptyDir != nil {
			return false, "pod-local-storage-emptydir", nil
		}
	}
	return true, "", nil
}

func NewPodControlledByFilter(controlledByAPIResources []*meta.APIResource) PodFilterFunc {
	return func(p core.Pod) (bool, string, error) {
		for _, controlledBy := range controlledByAPIResources {
			if controlledBy == nil { //means uncontrolled pod
				if p.Status.Phase == core.PodSucceeded || p.Status.Phase == core.PodFailed {
					continue
				}
				if meta.GetControllerOf(&p) == nil {
					return false, "pod-uncontrolled", nil
				}
				continue
			}

			c := meta.GetControllerOf(&p)
			if c == nil || c.Kind != controlledBy.Kind || c.APIVersion != controlledBy.Group+"/"+controlledBy.Version {
				continue
			}
			return false, "pod-controlledby-" + strings.ToLower(controlledBy.Kind), nil
		}
		return true, "", nil
	}
}

// UnprotectedPodFilter returns a FilterFunc that returns true if the
// supplied pod does not have any of the user-specified annotations for
// protection from eviction
func UnprotectedPodFilter(store RuntimeObjectStore, checkController bool, annotations ...string) PodFilterFunc {
	return func(p core.Pod) (bool, string, error) {
		for _, annot := range annotations {
			selector, err := labels.Parse(annot)
			if err != nil {
				return false, "", err
			}
			if selector.Matches(labels.Set(p.GetAnnotations())) {
				return false, "pod-annotation", nil
			}
			if checkController {
				if ctrl, found := GetControllerForPod(&p, store); found {
					if selector.Matches(labels.Set(ctrl.GetAnnotations())) {
						return false, "ctrl-annotation", nil
					}
				}
			}
		}
		return true, "", nil
	}
}

func PodOrControllerHasNoneOfTheAnnotations(store RuntimeObjectStore, annotations ...string) PodFilterFunc {
	fn := PodOrControllerHasAnyOfTheAnnotations(store, annotations...)
	return func(p core.Pod) (pass bool, reason string, err error) {
		pass, reason, err = fn(p)
		if err != nil {
			return false, "", err
		}
		return !pass, reason, nil
	}
}

func PodOrControllerHasAnyOfTheAnnotations(store RuntimeObjectStore, annotations ...string) PodFilterFunc {
	return func(p core.Pod) (bool, string, error) {
		for _, annot := range annotations {
			selector, err := labels.Parse(annot)
			if err != nil {
				return false, "", err
			}
			if selector.Matches(labels.Set(p.GetAnnotations())) {
				return true, "pod-annotation", nil
			}
			if ctrl, found := GetControllerForPod(&p, store); found {
				if selector.Matches(labels.Set(ctrl.GetAnnotations())) {
					return true, "ctrl-annotation", nil
				}
			}
		}
		return false, "", nil
	}
}

// NewPodFilters returns a FilterFunc that returns true if all of the supplied
// FilterFuncs return true.
func NewPodFilters(filters ...PodFilterFunc) PodFilterFunc {
	return func(p core.Pod) (bool, string, error) {
		for _, fn := range filters {
			passes, reason, err := fn(p)
			if err != nil {
				return false, "error", fmt.Errorf("cannot apply filters: %w", err)
			}
			if !passes {
				return false, reason, nil
			}
		}
		return true, "", nil
	}
}

func NewPodFiltersWithOptInFirst(optInFilter, filter PodFilterFunc) PodFilterFunc {
	return func(p core.Pod) (bool, string, error) {
		optIn, r, err := optInFilter(p)
		if err != nil {
			return false, r, err
		}
		if optIn {
			return true, r, err
		}
		return filter(p)
	}
}

func NewPodFiltersIgnoreShortLivedPods(filter PodFilterFunc, store RuntimeObjectStore, annotations ...string) PodFilterFunc {
	shortLivedPodFilter := PodOrControllerHasAnyOfTheAnnotations(store, annotations...)
	return func(p core.Pod) (bool, string, error) {
		isShortLived, _, err := shortLivedPodFilter(p)
		if err != nil {
			return false, "error-in-short-lived-filter", err
		}
		if isShortLived {
			return false, "short-lived-pod", nil
		}
		return filter(p)
	}
}

func NewPodFiltersIgnoreCompletedPods(filter PodFilterFunc) PodFilterFunc {
	return func(p core.Pod) (bool, string, error) {
		if p.Status.Phase == core.PodSucceeded || p.Status.Phase == core.PodFailed {
			return true, "", nil
		}
		return filter(p)
	}
}

// NewPodFiltersNoStatefulSetOnNodeWithoutDisk for backward compatibility with Draino v1 configurations
// we need to exclude pods that are associated with STS and that run on a node without local-storage
func NewPodFiltersNoStatefulSetOnNodeWithoutDisk(store RuntimeObjectStore) PodFilterFunc {
	return func(p core.Pod) (bool, string, error) {
		if !IsPodFromStatefulset(&p) {
			return true, "", nil
		}

		if p.Spec.NodeName == "" {
			return true, "", nil
		}
		node, err := store.Nodes().Get(p.Spec.NodeName)
		if errors.IsNotFound(err) {
			return true, "", nil
		}
		if err != nil {
			return false, "can't check node for the pod", err
		}
		if node.Labels == nil {
			return true, "", nil
		}
		if node.Labels["node-lifecycle.datadoghq.com/enabled"] == "true" {
			return true, "", nil
		}
		if node.Labels["nodegroups.datadoghq.com/local-storage"] == "false" {
			return false, fmt.Sprintf("StatefulSet pod %s/%s on node without local-storage", p.Namespace, p.Name), nil
		}
		return true, "", nil
	}
}
