package drain_runner

import (
	"context"
	"strings"
	"time"

	"k8s.io/kubernetes/pkg/apis/core"

	"github.com/go-logr/logr"
	drainbuffer "github.com/planetlabs/draino/internal/drain_buffer"
	"github.com/planetlabs/draino/internal/groups"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/drain"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	"github.com/planetlabs/draino/internal/kubernetes/utils"
	"github.com/planetlabs/draino/internal/protector"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// DrainTimeout how long is it acceptable for a drain to run
// TODO is this a good value? ==> probably not because that depends on the terminationGracePeriod of the pods. See getGracePeriodWithEvictionHeadRoom and getMinEvictionTimeoutWithEvictionHeadRoom
const DrainTimeout = 10 * time.Minute

// Make sure that the drain runner is implementing the group runner interface
var _ groups.Runner = &drainRunner{}

// drainRunner implements the groups.Runner interface and will be used to drain nodes of the given group configuration
type drainRunner struct {
	client              client.Client
	logger              logr.Logger
	clock               clock.Clock
	retryWall           drain.RetryWall
	drainer             kubernetes.Drainer
	sharedIndexInformer index.GetSharedIndexInformer
	runEvery            time.Duration
	pvProtector         protector.PVProtector
	eventRecorder       kubernetes.EventRecorder
	drainBuffer         drainbuffer.DrainBuffer

	preprocessors []DrainPreProzessor
}

func (runner *drainRunner) Run(info *groups.RunnerInfo) error {
	ctx, cancel := context.WithCancel(info.Context)

	runner.logger = runner.logger.WithValues("groupKey", info.Key)

	// run an endless loop until there are no drain candidates left
	wait.UntilWithContext(ctx, func(ctx context.Context) {

		runner.handleLeftOverDraining(ctx, info)

		if emptyGroup := runner.handleGroup(ctx, info); emptyGroup {
			cancel()
			return
		}
	}, runner.runEvery)
	return nil
}

// handleLeftOverDraining perform cleanup of nodes blocked in `draining` phase.
// If the controller was restarted, it is possible that some nodes were left
// with a taint `draining`. They would be blocked with that taint if we do nothing.
// Here we are searching for such cases and we are sending them back to the pool by removing the taint.
// These nodes might become candidate again in a near future.
func (runner *drainRunner) handleLeftOverDraining(ctx context.Context, info *groups.RunnerInfo) {
	draining, _, err := runner.getNodesForNLATaint(ctx, info.Key, k8sclient.TaintDraining)
	if err != nil {
		runner.logger.Error(err, "cannot get draining nodes for group")
		return
	}

	if len(draining) > 0 {
		runner.logger.Info("Found some nodes that were stuck in draining", "count", len(draining))
	}
	// TODO add metric to track amount of nodes blocked in draining
	for _, n := range draining {
		if errRmTaint := runner.removeFailedCandidate(ctx, n, "Node stuck in draining (controller restart?)"); errRmTaint != nil {
			// we just log the error, it will come back at next iteration
			runner.logger.Error(err, "Failed to remove 'draining' taint", "node", n.Name)
		}
	}
}

func (runner *drainRunner) handleGroup(ctx context.Context, info *groups.RunnerInfo) (emptyGroup bool) {
	candidates, groupHasAtLeastOneNode, err := runner.getNodesForNLATaint(ctx, info.Key, k8sclient.TaintDrainCandidate)
	// in case of an error we'll just try it again
	if err != nil {
		runner.logger.Error(err, "cannot get drain candidates for group")
		return
	}
	// TODO add metric to track amount of candidates
	if !groupHasAtLeastOneNode {
		// If there are no candidates left, we'll stop the loop
		runner.logger.Info("no node in group left, stopping.")
		emptyGroup = true
		return
	}

	for _, candidate := range candidates {
		if err := runner.handleCandidate(ctx, info, candidate); err != nil {
			runner.logger.Error(err, "error during candidate evaluation", "node_name", candidate.Name)
		}
	}
	return
}

func (runner *drainRunner) handleCandidate(ctx context.Context, info *groups.RunnerInfo, candidate *corev1.Node) error {
	podsAssociatedWithPV, err := runner.pvProtector.GetUnscheduledPodsBoundToNodeByPV(candidate)
	if err != nil {
		return err
	}

	// TODO add metric to track how often this happens
	if len(podsAssociatedWithPV) > 0 {
		runner.logger.Info("Removing candidate status, because node is hosting PV for pods", "node", candidate.Name, "pods", strings.Join(utils.GetPodNames(podsAssociatedWithPV), "; "))
		runner.eventRecorder.NodeEventf(ctx, candidate, core.EventTypeWarning, kubernetes.EventReasonPendingPodWithLocalPV, "Pod(s) "+strings.Join(utils.GetPodNames(podsAssociatedWithPV), ", ")+" associated with local PV on that node")
		_, err := k8sclient.RemoveNLATaint(ctx, runner.client, candidate)
		return err
	}

	kubernetes.LogrForVerboseNode(runner.logger, candidate, "Node is candidate for drain, checking pre-activities")
	allPreprocessorsDone := runner.checkPreprocessors(candidate)
	if !allPreprocessorsDone {
		runner.logger.Info("waiting for preprocessors to be done before draining", "node_name", candidate.Name)
		return nil
	}

	runner.logger.Info("start draining", "node_name", candidate.Name)

	// Draining a node is a blocking operation. This makes sure that one drain does not affect the other by taking PDB budget.
	// TODO add metric to show how many drains are successful / failed
	runner.eventRecorder.NodeEventf(ctx, candidate, core.EventTypeNormal, kubernetes.EventReasonDrainStarting, "Draining node")
	err = runner.drainCandidate(ctx, info, candidate)
	if err != nil {
		runner.logger.Error(err, "failed to drain node", "node_name", candidate.Name)
		runner.eventRecorder.NodeEventf(ctx, candidate, core.EventTypeWarning, kubernetes.EventReasonDrainFailed, "Drain failed: %v", err)
		return runner.removeFailedCandidate(ctx, candidate, err.Error())
	}
	runner.eventRecorder.NodeEventf(ctx, candidate, core.EventTypeNormal, kubernetes.EventReasonDrainSucceeded, "Drained node")
	runner.logger.Info("successfully drained node", "node_name", candidate.Name)

	return nil
}

func (runner *drainRunner) checkPreprocessors(candidate *corev1.Node) bool {
	allPreprocessorsDone := true
	for _, pre := range runner.preprocessors {
		done, err := pre.IsDone(candidate)
		if err != nil {
			allPreprocessorsDone = false
			runner.logger.Error(err, "failed during preprocessor evaluation", "preprocessor", pre.GetName(), "node_name", candidate.Name)
			continue
		}
		if !done {
			runner.logger.Info("preprocessor still pending", "node_name", candidate.Name, "preprocessor", pre.GetName())
			allPreprocessorsDone = false
		}
	}
	return allPreprocessorsDone
}

func (runner *drainRunner) drainCandidate(ctx context.Context, info *groups.RunnerInfo, candidate *corev1.Node) error {
	// TODO add metric about to track the runtime of this method
	candidate, err := k8sclient.AddNLATaint(ctx, runner.client, candidate, runner.clock.Now(), k8sclient.TaintDraining)
	if err != nil {
		return err
	}

	// This will make sure that the individual drain, will not block the loop forever
	// TODO maybe we should deal with that timeout issue INSIDE the `drain` function because the timeout depends
	// TODO on what is running in the node, there could be long terminationGracePeriod on pods.
	drainContext, cancel := context.WithTimeout(ctx, DrainTimeout)
	defer cancel()

	err = runner.drainer.Drain(drainContext, candidate)
	if err != nil {
		return err
	}

	runner.drainBuffer.StoreSuccessfulDrain(info.Key, 0)
	_, err = k8sclient.AddNLATaint(ctx, runner.client, candidate, runner.clock.Now(), k8sclient.TaintDrained)
	return err
}

func (runner *drainRunner) removeFailedCandidate(ctx context.Context, candidate *corev1.Node, reason string) error {
	err := runner.retryWall.SetNewRetryWallTimestamp(ctx, candidate, reason, runner.clock.Now())
	if err != nil {
		return err
	}
	rw := runner.retryWall.GetRetryWallTimestamp(candidate)
	runner.eventRecorder.NodeEventf(ctx, candidate, core.EventTypeWarning, kubernetes.EventReasonDrainFailed, "Drain failed: next attempt after %v", rw)
	_, err = k8sclient.RemoveNLATaint(ctx, runner.client, candidate)
	return err
}

// getNodesForNLATaint return nodes that match the taint. The boolean is set to true if some nodes are still present in the group, regardless of the taint.
func (runner *drainRunner) getNodesForNLATaint(ctx context.Context, key groups.GroupKey, taintValue k8sclient.DrainTaintValue) ([]*corev1.Node, bool, error) {
	nodes, err := index.GetFromIndex[corev1.Node](ctx, runner.sharedIndexInformer, groups.SchedulingGroupIdx, string(key))
	if err != nil {
		return nil, false, err
	}

	candidates := make([]*corev1.Node, 0)
	for _, node := range nodes {
		taint, exist := k8sclient.GetNLATaint(node)
		// if the node doesn't have the draino taint, it should not be processed
		if !exist {
			continue
		}
		if taint.Value != taintValue {
			continue
		}
		candidates = append(candidates, node)
	}
	return candidates, len(nodes) > 0, nil
}
