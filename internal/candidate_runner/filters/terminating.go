package filters

import (
	v1 "k8s.io/api/core/v1"
)

func NewNodeTerminatingFilter() Filter {
	return FilterFromFunction("node_terminating",
		func(n *v1.Node) bool {
			return n.DeletionTimestamp == nil || n.DeletionTimestamp.IsZero()
		})
}
