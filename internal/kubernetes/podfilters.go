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

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
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
func UnprotectedPodFilter(annotations ...string) PodFilterFunc {
	return func(p core.Pod) (bool, string, error) {
		var filter bool
		for _, annot := range annotations {
			// Try to split the annotation into key-value pairs
			kv := strings.SplitN(annot, "=", 2)
			if len(kv) < 2 {
				// If the annotation is a single string, then simply check for
				// the existence of the annotation key
				_, filter = p.GetAnnotations()[kv[0]]
			} else {
				// If the annotation is a key-value pair, then check if the
				// value for the pod annotation matches that of the
				// user-specified value
				v, ok := p.GetAnnotations()[kv[0]]
				filter = ok && v == kv[1]
			}
			if filter {
				return false, "pod-annotation", nil
			}
		}
		return true, "", nil
	}
}

// UserOptOutViaPodAnnotation returns a FilterFunc that returns true if the
// supplied pod has any of the user-specified annotations for out-opt (aka protection)
func UserOptOutViaPodAnnotation(annotations ...string) PodFilterFunc {
	f := UnprotectedPodFilter(annotations...)
	return func(p core.Pod) (bool, string, error) {
		b, s, e := f(p)
		return !b, s, e
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
