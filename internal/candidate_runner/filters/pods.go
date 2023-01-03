package filters

import (
	"context"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes"
	v1 "k8s.io/api/core/v1"
)

func NewPodFilter(logger logr.Logger, podFilter kubernetes.PodFilterFunc, objectsStore kubernetes.RuntimeObjectStore) Filter {
	return FilterFromFunctionWithReason("pods",
		func(ctx context.Context, n *v1.Node) (bool, string) {
			if podFilter == nil || objectsStore == nil || objectsStore.Pods() == nil {
				return false, "bad_filter_initialization"
			}
			pods, err := objectsStore.Pods().ListPodsForNode(n.Name)
			if err != nil {
				logger.Error(err, "failed to list pod for node", "node", n.Name)
				return false, "store_error"
			}
			for _, pod := range pods {
				ok, reason, err := podFilter(*pod)
				if err != nil {
					logger.Error(err, "failed to run pod filter", "node", n.Name, "pod", pod.Name, "namespace", pod.Namespace)
					return false, "filter_error"
				}
				if !ok {
					return false, reason
				}
			}
			return true, ""
		})
}
