package filters

import (
	"github.com/planetlabs/draino/internal/kubernetes"
)

func NewNodeWithLabelFilter(nodeLabelsFilterFunc kubernetes.NodeLabelFilterFunc) Filter {
	return FilterFromFunction("labels", NodeFilterFuncFromInterfaceFunc(nodeLabelsFilterFunc))
}
