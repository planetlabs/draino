package candidate_runner

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/DataDog/compute-go/logs"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/planetlabs/draino/internal/candidate_runner/filters"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	"github.com/planetlabs/draino/internal/kubernetes/utils"
	"github.com/planetlabs/draino/internal/limit"
	"github.com/planetlabs/draino/internal/scheduler"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/groups"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/drain"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	drainRetryFailedAnnotationKey    = "draino/drain-retry-failed"
	drainRetryRestartAnnotationValue = "restart"
)

// Make sure that the drain runner is implementing the group runner interface
var _ groups.Runner = &candidateRunner{}

type NodeSorters []scheduler.LessFunc[*corev1.Node]
type NodeIteratorFactory func([]*corev1.Node, NodeSorters) scheduler.ItemProvider[*corev1.Node]

type FilterFactory func(group string) filters.Filter

// candidateRunner implements the groups.Runner interface and will be used to drain nodes of the given group configuration
type candidateRunner struct {
	client              client.Client
	logger              logr.Logger
	clock               clock.Clock
	sharedIndexInformer index.GetSharedIndexInformer
	runEvery            time.Duration
	eventRecorder       kubernetes.EventRecorder
	retryWall           drain.RetryWall
	filter              filters.Filter
	rateLimiter         limit.TypedRateLimiter
	suppliedConditions  []kubernetes.SuppliedCondition

	maxSimultaneousCandidates int
	dryRun                    bool

	nodeSorters         NodeSorters
	nodeIteratorFactory NodeIteratorFactory
	drainSimulator      drain.DrainSimulator
}

func (runner *candidateRunner) GetNodes(ctx context.Context, key groups.GroupKey) ([]*corev1.Node, error) {
	return index.GetFromIndex[corev1.Node](ctx, runner.sharedIndexInformer, groups.SchedulingGroupIdx, string(key))
}

func (runner *candidateRunner) Run(info *groups.RunnerInfo) error {
	ctx, cancel := context.WithCancel(info.Context)
	// TODO if we add metrics associated with that key, when the group is closed we should purge all the series associated with that key (cleanup-gauges with groupKey=...)?
	runner.logger = runner.logger.WithValues("groupKey", info.Key)
	go runner.runCleanupWithContext(ctx, info)

	// run an endless loop until there are no drain candidates left
	wait.UntilWithContext(ctx, func(ctx context.Context) {
		span, ctx := tracer.StartSpanFromContext(ctx, "EvaluateCandidates")
		defer span.Finish()

		start := runner.clock.Now()

		var dataInfo, previousDataInfo DataInfo
		dataInfo.Slots = runner.maxSimultaneousCandidates
		if previous, hasPrevious := info.Data.Get(CandidateRunnerInfoKey); hasPrevious {
			previousDataInfo = previous.(DataInfo)
			dataInfo.importLongLastingData(previousDataInfo)
		}

		defer func() {
			dataInfo.LastRunTime = runner.clock.Now()
			dataInfo.ProcessingDuration = runner.clock.Now().Sub(start)
			info.Data.Set(CandidateRunnerInfoKey, dataInfo)
		}()

		nodes, err := runner.GetNodes(ctx, info.Key)
		// in case of an error we'll just try it again
		if err != nil {
			runner.logger.Error(err, "cannot get nodes for group")
			return
		}

		dataInfo.NodeCount = len(nodes)

		// TODO add metric to track amount of nodes in the group
		if len(nodes) == 0 {
			// If there are no candidates left, we'll stop the loop and the runner by closing the context
			runner.logger.Info("no nodes in group left, stopping.")
			cancel()
			return
		}

		// filter nodes that are already candidate
		nodes, alreadyCandidateNodes, maxReached := runner.checkAlreadyCandidates(nodes)
		dataInfo.CurrentCandidates = utils.NodesNames(alreadyCandidateNodes)
		if maxReached {
			runner.logger.Info("Max candidate already reached", "count", runner.maxSimultaneousCandidates, "nodes", strings.Join(utils.NodesNames(alreadyCandidateNodes), ","))
			return
		}
		remainCandidateSlot := runner.maxSimultaneousCandidates - len(alreadyCandidateNodes)

		evaluatedCount := len(nodes)
		nodes = runner.filter.Filter(ctx, nodes)
		dataInfo.FilteredOutCount = evaluatedCount - len(nodes)

		// TODO think about adding tracing to the tree iterator/expander
		var candidatesName []string
		nodeProvider := runner.GetNodeIterator(nodes)
	groupIteration:
		for node, ok := nodeProvider.Next(); ok; node, ok = nodeProvider.Next() {
			logForNode := runner.logger.WithValues("node", node.Name)
			// check that the node can be drained
			canDrain, reasons, errDrainSimulation := runner.drainSimulator.SimulateDrain(ctx, node)
			if len(errDrainSimulation) > 0 {
				for _, e := range errDrainSimulation {
					if k8sclient.IsClientSideRateLimiting(e) {
						dataInfo.LastRunRateLimited = true
						logForNode.Info("Not exploring the group further: simulation rate limited")
						break groupIteration
					}
				}
				dataInfo.LastSimulationRejections = append(dataInfo.LastSimulationRejections, node.Name)
				logForNode.Error(errDrainSimulation[0], "Failed to simulate drain")
				continue
			}
			if !canDrain {
				dataInfo.LastSimulationRejections = append(dataInfo.LastSimulationRejections, node.Name)
				logForNode.Info("Rejected by drain simulation", "reason", strings.Join(reasons, ";"))
				continue
			}

			// Check if one of the condition rate limiters has capacity
			if !runner.hasConditionRateLimitingCapacity(node) {
				logForNode.V(logs.ZapDebug).Info("No rate limiter has capacity")
				continue
			}

			candidatesName = append(candidatesName, node.Name)

			if !runner.dryRun {
				logForNode.Info("Adding drain candidate taint")
				if _, errTaint := k8sclient.AddNLATaint(ctx, runner.client, node, runner.clock.Now(), k8sclient.TaintDrainCandidate); errTaint != nil {
					logForNode.Error(errTaint, "Failed to taint node")
					continue // let's try next node, maybe this one has a problem
				}
			} else {
				logForNode.Info("Dry-Run: skip adding drain candidate taint")
			}
			remainCandidateSlot--
			if remainCandidateSlot <= 0 {
				break
			}
		}
		if len(candidatesName) > 0 {
			dataInfo.LastCandidates = candidatesName
			dataInfo.LastCandidatesTime = runner.clock.Now()
		}
		dataInfo.lastNodeIterator = nodeProvider
		dataInfo.LastNodeIteratorTime = runner.clock.Now()
		runner.logger.V(logs.ZapDebug).Info("Remain slot after drain candidate analysis", "count", remainCandidateSlot)
		dataInfo.CurrentCandidates = append(dataInfo.CurrentCandidates, candidatesName...)

	}, runner.runEvery)
	return nil
}

// hasConditionRateLimitingCapacity will iterate over all the node's conditions and try to get a token from each rate limiter.
// It will return true when it receives the first token and returns false if it cannot get any token.
func (runner *candidateRunner) hasConditionRateLimitingCapacity(node *corev1.Node) bool {
	conditions := kubernetes.GetNodeOffendingConditions(node, runner.suppliedConditions)
	for _, condition := range conditions {
		if runner.rateLimiter.TryAccept(string(condition.Type)) {
			return true
		}
	}
	return false
}

// checkAlreadyCandidates keep only the nodes that are not candidate. If maxSimultaneousCandidates>0, then we check against the max. If max is reached a nil slice is returned and the boolean returned is true
func (runner *candidateRunner) checkAlreadyCandidates(nodes []*corev1.Node) (remainingNodes, alreadyCandidateNodes []*corev1.Node, maxCandidateReached bool) {
	remainingNodes = make([]*corev1.Node, 0, len(nodes)) // high probability that all nodes are to be kept
	alreadyCandidateNodes = make([]*corev1.Node, 0, runner.maxSimultaneousCandidates)
	for _, n := range nodes {
		if _, hasTaint := k8sclient.GetNLATaint(n); !hasTaint {
			remainingNodes = append(remainingNodes, n)
		} else {
			alreadyCandidateNodes = append(alreadyCandidateNodes, n)
			if runner.maxSimultaneousCandidates > 0 {
				if len(alreadyCandidateNodes) >= runner.maxSimultaneousCandidates {
					return nil, alreadyCandidateNodes, true
				}
			}
		}
	}
	return remainingNodes, alreadyCandidateNodes, false
}

func (runner *candidateRunner) GetNodeIterator(nodes []*corev1.Node) scheduler.ItemProvider[*corev1.Node] {
	return runner.nodeIteratorFactory(nodes, runner.nodeSorters)
}

// handleRetryFlagOnNodes checks if a node has the drain-failed retry annotation and if so it will reset the retry wall
func (runner *candidateRunner) handleRetryFlagOnNodes(ctx context.Context, nodes []*corev1.Node) error {
	span, ctx := tracer.StartSpanFromContext(ctx, "ResetRetries")
	defer span.Finish()

	var errors []error
	for _, node := range nodes {
		// We don't want to mutate the node while it's in the draining phase
		if _, exist := k8sclient.GetNLATaint(node); exist {
			continue
		}
		var err error
		if val, exist := node.Annotations[drainRetryFailedAnnotationKey]; exist && val == drainRetryRestartAnnotationValue {
			if runner.retryWall.GetDrainRetryAttemptsCount(node) != 0 {
				node, err = runner.retryWall.ResetRetryCount(ctx, node)
				if err != nil {
					errors = append(errors, fmt.Errorf("cannot reset retry wall on node '%s': %v", node.Name, err))
				}
			}
			if errAnnotation := kubernetes.PatchDeleteNodeAnnotationKeyCR(ctx, runner.client, node, drainRetryFailedAnnotationKey); errAnnotation != nil {
				errors = append(errors, fmt.Errorf("cannot remove retry wall annotation from node '%s': %v", node.Name, errAnnotation))
			}

			// At this point we have a node that used to be a drain candidate, but the drain failed at some point.
			// Now, the user want's to reset everything to start a new drain attempt, so we are also resetting the node replacement pre process.
			_, exist := node.Annotations[kubernetes.NodeLabelKeyReplaceRequest]
			if exist {
				err = kubernetes.PatchDeleteNodeLabelKeyCR(ctx, runner.client, node, kubernetes.NodeLabelKeyReplaceRequest)
				if err != nil {
					errors = append(errors, err)
				}
			}
		}
	}
	if len(errors) == 0 {
		return nil
	}
	return utils.JoinErrors(errors, "|")
}

// runCleanupWithContext perform cleanup activities on nodes of the group
// - handleRetryFlagOnNodes
// -
func (runner *candidateRunner) runCleanupWithContext(ctx context.Context, info *groups.RunnerInfo) {
	// start the cleanup shifted compare to main runner to spread CPU consumption
	time.Sleep(runner.runEvery / 2)
	// run an endless loop until there are no drain candidates left
	wait.UntilWithContext(ctx, func(ctx context.Context) {
		span, ctx := tracer.StartSpanFromContext(ctx, "RunCleanupWithContext")
		defer span.Finish()

		start := runner.clock.Now()
		var dataInfo DataInfoForCleanupActivity
		defer func() {
			dataInfo.CleanupLastTime = runner.clock.Now()
			dataInfo.CleanupProcessingDuration = runner.clock.Now().Sub(start)

			info.Data.Set(CandidateRunnerInfoCleanupKey, dataInfo)
		}()

		nodes, err := index.GetFromIndex[corev1.Node](ctx, runner.sharedIndexInformer, groups.SchedulingGroupIdx, string(info.Key))
		// in case of an error we'll just try it again
		if err != nil {
			runner.logger.Error(err, "cannot get nodes for group")
			return
		}

		// remove retry wall from nodes that have drain-retry-failed=restart annotation
		if err := runner.handleRetryFlagOnNodes(ctx, nodes); err != nil {
			runner.logger.Error(err, "failed to remove retry wall from nodes that have retry annotation")
		}
	},
		runner.runEvery*4) // run less often

	runner.logger.Info("End of runCleanupWithContext")
}
