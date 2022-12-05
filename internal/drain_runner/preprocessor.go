package drain_runner

import (
	"fmt"
	"time"

	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	corev1 "k8s.io/api/core/v1"
)

// DrainPreProzessor is used to execute pre-drain activities.
type DrainPreProzessor interface {
	// GetName returns the unique name of the preprocessor
	GetName() string
	// IsDone will process the given node and returns true if the activity is done
	IsDone(*corev1.Node) (bool, error)
}

// WaitTimePreprocessor is a preprocessor used to wait for a certain amount of time before draining a node.
type WaitTimePreprocessor struct {
	waitFor time.Duration
}

func NewWaitTimePreprocessor(waitFor time.Duration) DrainPreProzessor {
	return &WaitTimePreprocessor{waitFor}
}

func (_ *WaitTimePreprocessor) GetName() string {
	return "WaitTimePreprocessor"
}

func (pre *WaitTimePreprocessor) IsDone(node *corev1.Node) (bool, error) {
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
