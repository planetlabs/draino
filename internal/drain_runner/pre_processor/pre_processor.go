package pre_processor

import (
	"context"
	"fmt"
	"time"

	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	corev1 "k8s.io/api/core/v1"
)

type PreProcessNotDoneReason string

const (
	PreProcessNotDoneReasonProcessing PreProcessNotDoneReason = "processing"
	PreProcessNotDoneReasonTimeout    PreProcessNotDoneReason = "timeout"
	PreProcessNotDoneReasonFailure    PreProcessNotDoneReason = "pre_prcessing_failure"
)

// DrainPreProcessor is used to execute pre-drain activities.
type DrainPreProcessor interface {
	// GetName returns the unique name of the preprocessor
	GetName() string
	// IsDone will process the given node and returns true if the activity is done
	IsDone(context.Context, *corev1.Node) (isDone bool, reason PreProcessNotDoneReason, err error)
	// Reset will reset the state of the pre processor
	Reset(context.Context, *corev1.Node) error
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

func (_ *WaitTimePreprocessor) Reset(context.Context, *corev1.Node) error {
	return nil
}

func (pre *WaitTimePreprocessor) IsDone(ctx context.Context, node *corev1.Node) (bool, PreProcessNotDoneReason, error) {
	taint, exist := k8sclient.GetNLATaint(node)
	if !exist {
		return false, "", fmt.Errorf("'%s' doesn't have a NLA taint", node.Name)
	}

	if taint.Value != k8sclient.TaintDrainCandidate {
		// TODO should we return an error in case a node has a weird state here?
		return true, "", nil
	}

	waitUntil := taint.TimeAdded.Add(pre.waitFor)
	return waitUntil.Before(time.Now()), PreProcessNotDoneReasonProcessing, nil
}
