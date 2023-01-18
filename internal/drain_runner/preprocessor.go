package drain_runner

import (
	"context"
	"fmt"
	"time"

	"github.com/DataDog/compute-go/logs"
	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// DrainPreProcessor is used to execute pre-drain activities.
type DrainPreProcessor interface {
	// GetName returns the unique name of the preprocessor
	GetName() string
	// IsDone will process the given node and returns true if the activity is done
	IsDone(context.Context, *corev1.Node) (isDone bool, err error)
}

// PreProcessorFatalError is used to indicate that there was an unrecoverable error during the pre processing.
type PreProcessorFatalError struct {
	msg string
}

func (err PreProcessorFatalError) Error() string {
	return err.msg
}

func PreProcessorFatalErrorf(format string, args ...any) error {
	return PreProcessorFatalError{
		msg: fmt.Sprintf(format, args...),
	}
}

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

func (pre *NodeReplacementPreProcessor) IsDone(ctx context.Context, node *corev1.Node) (bool, error) {
	if !kubernetes.HasPreprovisioningAnnotation(node, pre.allNodesByDefault) {
		return true, nil
	}

	logger := pre.logger.WithValues("node", node.Name)

	if node.Labels == nil {
		node.Labels = map[string]string{}
	}
	state, exist := node.Labels[kubernetes.NodeLabelKeyReplaceRequest]
	if !exist {
		logger.Info("Attach node replacement label")
		err := kubernetes.PatchNodeLabelKeyCR(ctx, pre.kclient, node, kubernetes.NodeLabelKeyReplaceRequest, kubernetes.NodeLabelValueReplaceRequested)
		return false, err
	}

	switch state {
	case kubernetes.NodeLabelValueReplaceDone:
		logger.Info("Node replacement finished successfully")
		return true, nil
	case kubernetes.NodeLabelValueReplaceFailed:
		err := PreProcessorFatalErrorf("node pre-provisioning failed")
		logger.Error(err, "Failed to replace node")
		return true, err
	default:
		logger.V(logs.ZapDebug).Info("Waiting for node replacement")
		return false, nil
	}
}

// WaitTimePreprocessor is a preprocessor used to wait for a certain amount of time before draining a node.
type WaitTimePreprocessor struct {
	waitFor time.Duration
}

func NewWaitTimePreprocessor(waitFor time.Duration) DrainPreProcessor {
	return &WaitTimePreprocessor{waitFor}
}

func (_ *WaitTimePreprocessor) GetName() string {
	return "WaitTimePreprocessor"
}

func (pre *WaitTimePreprocessor) IsDone(ctx context.Context, node *corev1.Node) (bool, error) {
	taint, exist := k8sclient.GetNLATaint(node)
	if !exist {
		return false, fmt.Errorf("'%s' doesn't have a NLA taint", node.Name)
	}

	if taint.Value != k8sclient.TaintDrainCandidate {
		// TODO should we return an error in case a node has a weird state here?
		return true, nil
	}

	waitUntil := taint.TimeAdded.Add(pre.waitFor)
	return waitUntil.Before(time.Now()), nil
}
