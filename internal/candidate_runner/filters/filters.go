package filters

import (
	"context"

	v1 "k8s.io/api/core/v1"
)

type Filter interface {
	Name() string
	// Filter should be used to process and entire list. It is optimized for list processing
	Filter(ctx context.Context, nodes []*v1.Node) (keep []*v1.Node)
	// FilterNode returns the name(s) of the filter(s) and detailed reason(s) for rejection
	FilterNode(ctx context.Context, n *v1.Node) FilterOutput
}

type FilterOutput struct {
	Keep   bool
	Checks []CheckOutput `json:",omitempty"`
}

type CheckOutput struct {
	FilterName string
	Keep       bool
	Reason     string `json:",omitempty"`
}

func (f FilterOutput) OnlyFailingChecks() FilterOutput {
	if f.Keep {
		return FilterOutput{Keep: true}
	}
	results := FilterOutput{Keep: false}
	for _, c := range f.Checks {
		if !c.Keep {
			results.Checks = append(results.Checks, c)
		}
	}
	return results
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

func (g *genericFilterFromFunc) FilterNode(ctx context.Context, n *v1.Node) FilterOutput {
	keep, reason := g.f(ctx, n)
	return FilterOutput{
		Keep: keep,
		Checks: []CheckOutput{
			{
				FilterName: g.name,
				Keep:       keep,
				Reason:     reason,
			},
		},
	}
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
			return filterFunc(ctx, n), ""
		},
	)
}

func FilterFromFunctionWithReason(name string, filterFunc NodeFilterFuncWithReason) Filter {
	return &genericFilterFromFunc{
		name: name,
		f:    filterFunc,
	}
}
