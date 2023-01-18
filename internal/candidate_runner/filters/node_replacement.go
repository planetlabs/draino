package filters

import (
	"context"

	"github.com/planetlabs/draino/internal/kubernetes"
	corev1 "k8s.io/api/core/v1"
)

func NewFailedNodeReplacementFilter() Filter {
	return FilterFromFunction(
		"node_replacement_failed",
		func(ctx context.Context, n *corev1.Node) bool {
			if n.Labels == nil {
				return true
			}

			val, exist := n.Labels[kubernetes.NodeLabelKeyReplaceRequest]
			return !exist || val != kubernetes.NodeLabelValueReplaceFailed
		},
	)
}
