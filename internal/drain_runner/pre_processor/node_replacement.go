package pre_processor

import (
	"context"

	"github.com/DataDog/compute-go/logs"
	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NodeReplacementPreProcessor is used to spin up a replacement before draining a node
type NodeReplacementPreProcessor struct {
	kclient           client.Client
	logger            logr.Logger
	allNodesByDefault bool
}

func NewNodeReplacementPreProcessor(client client.Client, replaceAllNodesByDefault bool, logger logr.Logger) DrainPreProcessor {
	return &NodeReplacementPreProcessor{
		kclient:           client,
		logger:            logger.WithName("NodeReplacementPreProzessor"),
		allNodesByDefault: replaceAllNodesByDefault,
	}
}

func (_ *NodeReplacementPreProcessor) GetName() string {
	return "NodeReplacementPreProzessor"
}

func (pre *NodeReplacementPreProcessor) IsDone(ctx context.Context, node *corev1.Node) (bool, PreProcessNotDoneReason, error) {
	if !kubernetes.HasPreprovisioningAnnotation(node, pre.allNodesByDefault) {
		return true, "", nil
	}

	logger := pre.logger.WithValues("node", node.Name)

	if node.Labels == nil {
		node.Labels = map[string]string{}
	}
	state, exist := node.Labels[kubernetes.NodeLabelKeyReplaceRequest]
	if !exist {
		logger.Info("Attach node replacement label")
		err := kubernetes.PatchNodeLabelKeyCR(ctx, pre.kclient, node, kubernetes.NodeLabelKeyReplaceRequest, kubernetes.NodeLabelValueReplaceRequested)
		return false, PreProcessNotDoneReasonProcessing, err
	}

	switch state {
	case kubernetes.NodeLabelValueReplaceDone:
		logger.Info("Node replacement finished successfully")
		return true, "", nil
	case kubernetes.NodeLabelValueReplaceFailed:
		logger.Info("Failed to replace node")
		return false, PreProcessNotDoneReasonFailure, nil
	default:
		logger.V(logs.ZapDebug).Info("Waiting for node replacement")
		return false, PreProcessNotDoneReasonProcessing, nil
	}
}
