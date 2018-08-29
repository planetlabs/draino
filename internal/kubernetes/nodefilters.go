package kubernetes

import (
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
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
	return func(o interface{}) bool {
		n, ok := o.(*core.Node)
		if !ok {
			return false
		}
		if len(ct) == 0 {
			return true
		}
		for _, t := range ct {
			for _, c := range n.Status.Conditions {
				if c.Type == core.NodeConditionType(t) && c.Status == core.ConditionTrue {
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
