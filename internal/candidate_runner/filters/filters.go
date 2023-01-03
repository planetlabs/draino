package filters

import (
	"context"

	v1 "k8s.io/api/core/v1"
)

type Filter interface {
	Name() string
	// Filter should be used to process and entire list. It is optimized for list processing
	Filter(ctx context.Context, nodes []*v1.Node) (keep []*v1.Node)
	// FilterNode returns the name of the filter and the a detailed reason for rejection
	FilterNode(ctx context.Context, n *v1.Node) (keep bool, name, reason string)
}

type NodeFilterFunc func(ctx context.Context, n *v1.Node) bool
type NodeFilterFuncWithReason func(ctx context.Context, n *v1.Node) (bool, string)

type genericFilterFromFunc struct {
	name string
	f    NodeFilterFuncWithReason
}

func (g *genericFilterFromFunc) Name() string {
	return g.name
}

func (g *genericFilterFromFunc) FilterNode(ctx context.Context, n *v1.Node) (keep bool, name, reason string) {
	keep, reason = g.f(ctx, n)
	name = g.name
	return
}

func (g *genericFilterFromFunc) Filter(ctx context.Context, nodes []*v1.Node) (keep []*v1.Node) {
	keep = make([]*v1.Node, 0, len(nodes))
	for _, n := range nodes {
		if accept, _ := g.f(ctx, n); accept {
			keep = append(keep, n)
		}
	}
	return
}

var _ Filter = &genericFilterFromFunc{}

// NodeFilterFuncFromInterfaceFunc This function will allow us to adapt the legacy filter type: kubernetes.NodeLabelFilterFunc
func NodeFilterFuncFromInterfaceFunc(f func(ctx context.Context, o interface{}) bool) NodeFilterFunc {
	return func(ctx context.Context, n *v1.Node) bool {
		return f(ctx, n)
	}
}

func FilterFromFunction(name string, filterFunc NodeFilterFunc) Filter {
	return FilterFromFunctionWithReason(
		name,
		func(ctx context.Context, n *v1.Node) (bool, string) {
			return filterFunc(ctx, n), "rejected"
		},
	)
}

func FilterFromFunctionWithReason(name string, filterFunc NodeFilterFuncWithReason) Filter {
	return &genericFilterFromFunc{
		name: name,
		f:    filterFunc,
	}
}
