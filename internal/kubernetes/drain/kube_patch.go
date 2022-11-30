package drain

import (
	"encoding/json"
	"fmt"

	"github.com/planetlabs/draino/internal/kubernetes/utils"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type NodeConditionPatch struct {
	ConditionType corev1.NodeConditionType
}

var _ client.Patch = &NodeConditionPatch{}

func (_ *NodeConditionPatch) Type() types.PatchType {
	return types.StrategicMergePatchType
}

func (self *NodeConditionPatch) Data(obj client.Object) ([]byte, error) {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return nil, fmt.Errorf("cannot parse object into node")
	}

	_, condition, found := utils.FindNodeCondition(self.ConditionType, node)
	if !found {
		return nil, fmt.Errorf("cannot find condition on node")
	}

	// This is inspired by a kubernetes function that is updating the node conditions in the same way
	// https://github.com/kubernetes/kubernetes/blob/fa1b6765d55c3f3c00299a9e279732342cfb00f7/staging/src/k8s.io/component-helpers/node/util/conditions.go#L45
	// It was tested on a local cluster and works for both create & updates.
	return json.Marshal(map[string]interface{}{
		"status": map[string]interface{}{
			"conditions": []v1.NodeCondition{condition},
		},
	})
}
