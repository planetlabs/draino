package filters

import (
	"context"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/utils"
	"github.com/planetlabs/draino/internal/protector"
	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/apis/core"
	"strings"
)

func NewPVCBoundFilter(protector protector.PVCProtector, eventRecorder kubernetes.EventRecorder) Filter {
	return FilterFromFunctionWithReason("pvcBound",
		func(ctx context.Context, n *v1.Node) (bool, string) {
			podsAssociatedWithPV, err := protector.GetUnscheduledPodsBoundToNodeByPV(n)
			if err != nil {
				return false, "fail to check pvcBound: " + err.Error()
			}
			if len(podsAssociatedWithPV) > 0 {
				pods := strings.Join(utils.GetPodNames(podsAssociatedWithPV), "; ")
				eventRecorder.NodeEventf(ctx, n, core.EventTypeWarning, kubernetes.EventReasonPendingPodWithLocalPV, "Pod(s) "+pods+" associated with local PV on that node")
				return false, "pvcProtection triggered for pods: " + pods
			}
			return true, ""
		})
}
