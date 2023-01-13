package drain_runner

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/planetlabs/draino/internal/candidate_runner/filters"
	drainbuffer "github.com/planetlabs/draino/internal/drain_buffer"
	"github.com/planetlabs/draino/internal/groups"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/drain"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
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
	eventRecorder       kubernetes.EventRecorder
	filter              filters.Filter
	drainBuffer         drainbuffer.DrainBuffer
	suppliedConditions  []kubernetes.SuppliedCondition

	preprocessors []DrainPreProzessor
}

func (runner *drainRunner) Run(info *groups.RunnerInfo) error {
	ctx, cancel := context.WithCancel(info.Context)

	runner.logger = runner.logger.WithValues("groupKey", info.Key)

	// run an endless loop until there are no drain candidates left
	wait.UntilWithContext(ctx, func(ctx context.Context) {
		if !runner.drainBuffer.IsReady() {
			runner.logger.Info("pausing drain runner until drain buffer is properly initialized")
			return
		}

		span, ctx := tracer.StartSpanFromContext(ctx, "DrainCandidate")
		defer span.Finish()

		var drainInfo DataInfo
		start := runner.clock.Now()
		defer func() {
			drainInfo.ProcessingDuration = runner.clock.Now().Sub(start)
			info.Data.Set(DrainRunnerInfo, drainInfo)
		}()

		// Can't be done asynchronously, must be done in sequence with `handleGroup/handleCandidate` because these
		// are the function dealing with the taint lifecycle
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
// Here we are searching for such cases, and we are sending them back to the pool by removing the taint.
// These nodes might become candidate again in a near future.
func (runner *drainRunner) handleLeftOverDraining(ctx context.Context, info *groups.RunnerInfo) {
	span, ctx := tracer.StartSpanFromContext(ctx, "ResetStuckDrainAttempts")
	defer span.Finish()

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
		updatedNode, errRetryWall := runner.updateRetryWallOnCandidate(ctx, n, "Node stuck in draining (controller restart?)")
		if errRetryWall != nil {
			// we just log the error, it will come back at next iteration
			runner.logger.Error(errRetryWall, "Failed to update retry wall", "node", n.Name)
		}
		if _, err = k8sclient.RemoveNLATaint(ctx, runner.client, updatedNode); err != nil {
			runner.logger.Error(err, "Failed to remove taint on node left over in 'draining'", "node", n.Name)
			return
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
			runner.logger.Error(err, "error during candidate evaluation", "node", candidate.Name)
		}
	}
	return
}

// handleCandidate look at the candidate and attempt a drain
// while being under processing the node gets the `draining` taint
// at the end of the function, the node should be left with either `drained` taint or no taint and a retryWall set.
// In that last case (no taint and retry wall), the node will be picked up again later by the candidate_runner
// This function concentrates the taint management on the node for the drain_runner.
// During the pre-activities resolution phase the node keeps its `drain_candidate` taint.
func (runner *drainRunner) handleCandidate(ctx context.Context, info *groups.RunnerInfo, candidate *corev1.Node) error {
	span, ctx := tracer.StartSpanFromContext(ctx, "HandleDrainCandidate")
	defer span.Finish()

	loggerForNode := runner.logger.WithValues("node", candidate.Name)

	// Check if the node is still candidate before processing
	filterOutput := runner.filter.FilterNode(ctx, candidate)
	if !filterOutput.Keep {
		loggerForNode.Info("Removing candidate status", "rejections", filterOutput.OnlyFailingChecks().Checks)
		_, errRmTaint := k8sclient.RemoveNLATaint(ctx, runner.client, candidate)
		return errRmTaint
	}

	// Checking pre-activities
	kubernetes.LogrForVerboseNode(runner.logger, candidate, "Node is candidate for drain, checking pre-activities")
	allPreprocessorsDone := runner.checkPreprocessors(ctx, candidate)
	if !allPreprocessorsDone {
		loggerForNode.Info("waiting for preprocessors to be done before draining", "node", candidate.Name)
		return nil
	}

	loggerForNode.Info("start draining")
	// Draining a node is a blocking operation. This makes sure that one drain does not affect the other by taking PDB budget.
	var err error
	candidate, err = k8sclient.AddNLATaint(ctx, runner.client, candidate, runner.clock.Now(), k8sclient.TaintDraining)
	if err != nil {
		return err
	}
	runner.eventRecorder.NodeEventf(ctx, candidate, core.EventTypeNormal, kubernetes.EventReasonDrainStarting, "Draining node")

	err = runner.drainCandidate(ctx, info, candidate)
	var errRefresh error
	candidate, errRefresh = runner.refreshNode(ctx, candidate)
	if errRefresh != nil {
		if errors.IsNotFound(errRefresh) {
			loggerForNode.Info("node has been deleted while we were waiting for the drain to complete")
			CounterDrainedNodes(candidate, DrainedNodeResultSucceeded, kubernetes.GetNodeOffendingConditions(candidate, runner.suppliedConditions), "node_deleted")
			return nil
		}
		loggerForNode.Error(errRefresh, "failed to refresh node after drain")
		CounterDrainedNodes(candidate, DrainedNodeResultFailed, kubernetes.GetNodeOffendingConditions(candidate, runner.suppliedConditions), "node_refresh")
		return errRefresh
	}
	if err != nil {
		CounterDrainedNodes(candidate, DrainedNodeResultFailed, kubernetes.GetNodeOffendingConditions(candidate, runner.suppliedConditions), string(kubernetes.GetFailureCause(err)))
		loggerForNode.Error(err, "failed to drain node")
		runner.eventRecorder.NodeEventf(ctx, candidate, core.EventTypeWarning, kubernetes.EventReasonDrainFailed, "Drain failed: %v", err)
		updatedNode, errRetryWall := runner.updateRetryWallOnCandidate(ctx, candidate, err.Error())
		if errRetryWall != nil {
			loggerForNode.Error(errRetryWall, "Failed to remove taint following drain failure")
			return errRetryWall
		}
		if _, errTaint := k8sclient.RemoveNLATaint(ctx, runner.client, updatedNode); errTaint != nil {
			loggerForNode.Error(errTaint, "Failed to remove taint following drain failure")
			return errTaint
		}
		return err
	}
	if candidate, err = k8sclient.AddNLATaint(ctx, runner.client, candidate, runner.clock.Now(), k8sclient.TaintDrained); err != nil {
		loggerForNode.Error(err, "Failed to add 'drained' taint")
		return err
	}
	CounterDrainedNodes(candidate, DrainedNodeResultSucceeded, kubernetes.GetNodeOffendingConditions(candidate, runner.suppliedConditions), "")
	runner.eventRecorder.NodeEventf(ctx, candidate, core.EventTypeNormal, kubernetes.EventReasonDrainSucceeded, "Drained node")
	runner.logger.Info("successfully drained node", "node", candidate.Name)
	return nil
}

func (runner *drainRunner) checkPreprocessors(ctx context.Context, candidate *corev1.Node) bool {
	span, ctx := tracer.StartSpanFromContext(ctx, "RunDrainPreprocessors")
	defer span.Finish()

	allPreprocessorsDone := true
	for _, pre := range runner.preprocessors {
		done, err := pre.IsDone(ctx, candidate)
		if err != nil {
			allPreprocessorsDone = false
			runner.logger.Error(err, "failed during preprocessor evaluation", "preprocessor", pre.GetName(), "node", candidate.Name)
			continue
		}
		if !done {
			runner.logger.Info("preprocessor still pending", "node", candidate.Name, "preprocessor", pre.GetName())
			allPreprocessorsDone = false
		}
	}
	return allPreprocessorsDone
}

func (runner *drainRunner) refreshNode(ctx context.Context, node *corev1.Node) (refreshedNode *corev1.Node, err error) {

	var n corev1.Node
	err = runner.client.Get(ctx, types.NamespacedName{Name: node.Name}, &n)

	return &n, err
}

func (runner *drainRunner) drainCandidate(ctx context.Context, info *groups.RunnerInfo, candidate *corev1.Node) error {

	// This will make sure that the individual drain, will not block the loop forever
	// TODO maybe we should deal with that timeout issue INSIDE the `drain` function because the timeout depends
	// TODO on what is running in the node, there could be long terminationGracePeriod on pods.
	drainContext, cancel := context.WithTimeout(ctx, DrainTimeout)
	defer cancel()

	err := runner.drainer.Drain(drainContext, candidate)
	if err != nil {
		return err
	}

	// We can ignore the error as it's only fired when the drain buffer is not initialized.
	// This cannot happen as the main loop of the drain runner will be blocked in that case.
	_ = runner.drainBuffer.StoreSuccessfulDrain(info.Key, 0)
	return err
}

func (runner *drainRunner) updateRetryWallOnCandidate(ctx context.Context, candidate *corev1.Node, reason string) (*corev1.Node, error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "ResetFailedCandidate")
	defer span.Finish()

	newNode, err := runner.retryWall.SetNewRetryWallTimestamp(ctx, candidate, reason, runner.clock.Now())
	if err != nil {
		return nil, err
	}
	rw := runner.retryWall.GetRetryWallTimestamp(newNode)
	runner.eventRecorder.NodeEventf(ctx, newNode, core.EventTypeWarning, kubernetes.EventReasonDrainFailed, "Drain failed: next attempt after %v", rw)
	// We saw the following error here "the object has been modified; please apply your changes to the latest version and try again"
	// In order to fix it, SetNewRetryWallTimestamp is returning the new version of the node.
	// This will not remove the error completely, but the amount of occurrences should be very low.
	return newNode, err
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
