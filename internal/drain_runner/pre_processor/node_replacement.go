package pre_processor

import (
	"context"
	"time"

	"github.com/DataDog/compute-go/logs"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
)

const (
	PreprovisioningAnnotationKey        = "node-lifecycle.datadoghq.com/provision-new-node-before-drain"
	PreprovisioningAnnotationValue      = "true"
	PreprovisioningFalseAnnotationValue = "false"
	PreprovisioningTimeout              = 60 * time.Minute
)

type NodeReplacer struct {
	kclient client.Client
	logger  logr.Logger
	clock   clock.Clock
}

func NewNodeReplacer(client client.Client, logger logr.Logger, clock clock.Clock) *NodeReplacer {
	return &NodeReplacer{
		kclient: client,
		logger:  logger.WithName("NodeReplacer"),
		clock:   clock,
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

	taint, exist := k8sclient.GetNLATaint(node)
	if !exist {
		logger.Info("Taint does not exist anymore; Node replacemnet aborted.")
		return false, PreProcessNotDoneReasonNotCandidate
	}

	if repl.clock.Since(taint.TimeAdded.Time) > PreprovisioningTimeout {
		logger.Info("Node replacement timeout")
		return false, PreProcessNotDoneReasonTimeout
	}

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

func NewNodeReplacementPreProcessor(client client.Client, replaceAllNodesByDefault bool, logger logr.Logger, clock clock.Clock) DrainPreProcessor {
	return &NodeReplacementPreProcessor{
		nodeReplacer:      NewNodeReplacer(client, logger, clock),
		allNodesByDefault: replaceAllNodesByDefault,
	}
}

func (_ *NodeReplacementPreProcessor) GetName() string {
	return "NodeReplacementPreProzessor"
}

func (pre *NodeReplacementPreProcessor) IsDone(ctx context.Context, node *corev1.Node) (bool, PreProcessNotDoneReason, error) {
	if !HasPreprovisioningAnnotation(node, pre.allNodesByDefault) {
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

func HasPreprovisioningAnnotation(node *corev1.Node, allNodesByDefault bool) bool {
	if node.Annotations == nil {
		return allNodesByDefault
	}
	return node.Annotations[PreprovisioningAnnotationKey] == PreprovisioningAnnotationValue || (allNodesByDefault && !(node.Annotations[PreprovisioningAnnotationKey] == PreprovisioningFalseAnnotationValue))
}
