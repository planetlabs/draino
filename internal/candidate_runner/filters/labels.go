package filters

import (
	"context"

	"github.com/planetlabs/draino/internal/kubernetes"
)

func NewNodeWithLabelFilter(nodeLabelsFilterFunc kubernetes.NodeLabelFilterFunc) Filter {
	return FilterFromFunction("labels", NodeFilterFuncFromInterfaceFunc(func(ctx context.Context, o interface{}) bool { return nodeLabelsFilterFunc(o) }))
}
