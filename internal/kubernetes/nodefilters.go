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
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"strings"
)

// NewNodeLabelFilter returns a filter that returns true if the supplied object
// is a node with all of the supplied labels.
func NewNodeLabelFilter(labels map[string]string) func(o interface{}) bool {
	return func(o interface{}) bool {
		n, ok := o.(*core.Node)
		if !ok {
			return false
		}
		for k, v := range labels {
			if n.GetLabels()[k] != v {
				return false
			}
		}
		return true
	}
}

// NewNodeConditionFilter returns a filter that returns true if the supplied
// object is a node with any of the supplied node conditions.
func NewNodeConditionFilter(ct []string) func(o interface{}) bool {
	var specifiedConditions [][]string
	//make parsing once
	for _, t := range ct {
		kv := strings.SplitN(t, "=", 2)
		// default values as if we specify only condition type e.g. OutOfMemory
		tp := t
		st := "True"

		// if we provided type and state as if OutOfMemory=Unknown
		if len(kv) == 2 {
			tp = kv[0]
			st = kv[1]
		}
		specifiedConditions = append(specifiedConditions, []string{tp, st})
	}

	return func(o interface{}) bool {
		n, ok := o.(*core.Node)
		if !ok {
			return false
		}
		if len(ct) == 0 {
			return true
		}

		for _, condition := range specifiedConditions {
			for _, c := range n.Status.Conditions {
				if c.Type == core.NodeConditionType(condition[0]) && c.Status == core.ConditionStatus(condition[1]) {
					return true
				}
			}
		}
		return false
	}
}

// NodeSchedulableFilter returns true if the supplied object is a schedulable
// node.
func NodeSchedulableFilter(o interface{}) bool {
	n, ok := o.(*core.Node)
	if !ok {
		return false
	}
	return !n.Spec.Unschedulable
}

// NodeProcessed tracks whether nodes have been processed before using a map.
type NodeProcessed map[types.UID]bool

// NewNodeProcessed returns a new node processed filter.
func NewNodeProcessed() NodeProcessed {
	return make(NodeProcessed)
}

// Filter returns true if the supplied object is a node that this filter has
// not seen before. It is not threadsafe and should always be the last filter
// applied.
func (processed NodeProcessed) Filter(o interface{}) bool {
	n, ok := o.(*core.Node)
	if !ok {
		return false
	}
	if processed[n.GetUID()] {
		return false
	}
	processed[n.GetUID()] = true
	return true
}
