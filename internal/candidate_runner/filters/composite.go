package filters

import (
	"context"
	"fmt"
	"github.com/planetlabs/draino/internal/kubernetes"
	"strings"

	"github.com/DataDog/compute-go/logs"
	"github.com/go-logr/logr"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	v1 "k8s.io/api/core/v1"
)

type CompositeFilter struct {
	logger  logr.Logger
	filters []Filter

	name string // the name depends on the order that we use for the filter. So this cached value must be reset in case the filters order is changed.
}

const (
	CompositeFilterSeparator = "|"
)

func (c *CompositeFilter) Name() string {
	if c.name != "" {
		return c.name
	}
	var names []string
	for _, f := range c.filters {
		names = append(names, f.Name())
	}
	c.name = strings.Join(names, CompositeFilterSeparator)
	return c.name
}

func (c *CompositeFilter) Filter(ctx context.Context, nodes []*v1.Node) (keep []*v1.Node) {
	span, ctx := tracer.StartSpanFromContext(ctx, "FilterDrainCandidates")
	defer span.Finish()

	var filteringStr []string
	for _, f := range c.filters {
		nodes = f.Filter(ctx, nodes)
		filteringStr = append(filteringStr, fmt.Sprintf("%s:%d", f.Name(), len(nodes)))
		if len(nodes) == 0 {
			break
		}
	}
	c.logger.V(logs.ZapDebug).Info("filtering", "result", strings.Join(filteringStr, CompositeFilterSeparator))
	return nodes
}

func (c *CompositeFilter) FilterNode(ctx context.Context, n *v1.Node) FilterOutput {
	keep := true
	result := FilterOutput{}
	for _, f := range c.filters {
		fOut := f.FilterNode(ctx, n)
		result.Checks = append(result.Checks, fOut.Checks...)
		keep = keep && fOut.Keep
	}
	result.Keep = keep
	kubernetes.LogrForVerboseNode(c.logger, n, "result", result)
	return result
}

var _ Filter = &CompositeFilter{}
