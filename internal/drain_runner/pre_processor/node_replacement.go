package pre_processor

import (
	"context"

	"github.com/DataDog/compute-go/logs"
	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type NodeReplacer struct {
	kclient client.Client
	logger  logr.Logger
}

func NewNodeReplacer(client client.Client, logger logr.Logger) *NodeReplacer {
	return &NodeReplacer{
		kclient: client,
		logger:  logger.WithName("NodeReplacer"),
	}
}

func (repl *NodeReplacer) TriggerNodeReplacement(ctx context.Context, node *corev1.Node) error {
	logger := repl.logger.WithValues("node", node.Name)

	_, exist := node.Labels[kubernetes.NodeLabelKeyReplaceRequest]
	if exist {
		return nil
	}

	logger.Info("Attach node replacement label")
	return k8sclient.PatchNodeLabelKeyCR(ctx, repl.kclient, node, kubernetes.NodeLabelKeyReplaceRequest, kubernetes.NodeLabelValueReplaceRequested)
}

func (repl *NodeReplacer) ResetReplacement(ctx context.Context, node *corev1.Node) error {
	return k8sclient.PatchDeleteNodeLabelKeyCR(ctx, repl.kclient, node, kubernetes.NodeLabelKeyReplaceRequest)
}

func (repl *NodeReplacer) IsDone(node *corev1.Node) (bool, PreProcessNotDoneReason) {
	logger := repl.logger.WithValues("node", node.Name)
	state := node.Labels[kubernetes.NodeLabelKeyReplaceRequest]
	switch state {
	case kubernetes.NodeLabelValueReplaceDone:
		logger.Info("Node replacement finished successfully")
		return true, ""
	case kubernetes.NodeLabelValueReplaceFailed:
		logger.Info("Failed to replace node")
		return false, PreProcessNotDoneReasonFailure
	default:
		logger.V(logs.ZapDebug).Info("Waiting for node replacement")
		return false, PreProcessNotDoneReasonProcessing
	}
}

// NodeReplacementPreProcessor is used to spin up a replacement before draining a node
type NodeReplacementPreProcessor struct {
	allNodesByDefault bool
	nodeReplacer      *NodeReplacer
}

func NewNodeReplacementPreProcessor(client client.Client, replaceAllNodesByDefault bool, logger logr.Logger) DrainPreProcessor {
	return &NodeReplacementPreProcessor{
		nodeReplacer:      NewNodeReplacer(client, logger),
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

	if err := pre.nodeReplacer.TriggerNodeReplacement(ctx, node); err != nil {
		return false, "", err
	}

	isDone, reason := pre.nodeReplacer.IsDone(node)
	return isDone, reason, nil
}

func (pre *NodeReplacementPreProcessor) Reset(ctx context.Context, node *corev1.Node) error {
	return pre.nodeReplacer.ResetReplacement(ctx, node)
}
