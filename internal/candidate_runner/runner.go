package candidate_runner

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/planetlabs/draino/internal/candidate_runner/filters"

	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	"github.com/planetlabs/draino/internal/kubernetes/utils"
	"github.com/planetlabs/draino/internal/protector"
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
	pvProtector         protector.PVProtector
	retryWall           drain.RetryWall
	filter              filters.Filter

	maxSimultaneousCandidates int
	dryRun                    bool

	nodeSorters         NodeSorters
	nodeIteratorFactory NodeIteratorFactory
	drainSimulator      drain.DrainSimulator
}

func (runner *candidateRunner) Run(info *groups.RunnerInfo) error {
	ctx, cancel := context.WithCancel(info.Context)

	// TODO if we add metrics associated with that key, when the group is closed we should purge all the series associated with that key (cleanup-gauges with groupKey=...)?
	runner.logger = runner.logger.WithValues("groupKey", info.Key)
	// run an endless loop until there are no drain candidates left
	wait.UntilWithContext(ctx, func(ctx context.Context) {

		start := runner.clock.Now()

		var dataInfo DataInfo

		defer func() {
			dataInfo.LastTime = runner.clock.Now()
			dataInfo.ProcessingDuration = runner.clock.Now().Sub(start).String()
			info.Data.Set(CandidateRunnerInfo, dataInfo)
		}()

		nodes, err := index.GetFromIndex[corev1.Node](ctx, runner.sharedIndexInformer, groups.SchedulingGroupIdx, string(info.Key))
		// in case of an error we'll just try it again
		if err != nil {
			runner.logger.Error(err, "cannot get nodes for group")
			return
		}

		dataInfo.NodeCount = len(nodes)

		// TODO add metric to track amount of nodes in the group
		if len(nodes) == 0 {
			// If there are no candidates left, we'll stop the loop
			runner.logger.Info("no nodes in group left, stopping.")
			cancel()
			return
		}

		// remove retry wall from nodes that have drain-retry-failed=restart annotation
		if err := runner.handleRetryFlagOnNodes(ctx, nodes); err != nil {
			runner.logger.Error(err, "failed to remove retry wall from nodes that have retry annotation")
		}

		// filter nodes that are already candidate
		nodes, alreadyCandidateNodes, maxReached := runner.checkAlreadyCandidates(nodes)
		if maxReached {
			dataInfo.Slots = fmt.Sprintf("0/%d", runner.maxSimultaneousCandidates)
			runner.logger.Info("Max candidate already reached", "count", runner.maxSimultaneousCandidates, "nodes", strings.Join(utils.NodesNames(alreadyCandidateNodes), ","))
			return
		}
		remainCandidateSlot := runner.maxSimultaneousCandidates - len(alreadyCandidateNodes)

		evaluatedCount := len(nodes)
		nodes = runner.filter.Filter(nodes)
		dataInfo.FilteredOutCount = evaluatedCount - len(nodes)

		nodeProvider := runner.GetNodeIterator(nodes)
		for node, ok := nodeProvider.Next(); ok && remainCandidateSlot > 0; node, ok = nodeProvider.Next() {
			logForNode := runner.logger.WithValues("node", node.Name)
			// check that the node can be drained
			canDrain, reasons, errDrainSimulation := runner.drainSimulator.SimulateDrain(ctx, node)
			if errDrainSimulation != nil {
				logForNode.Error(errDrainSimulation, "Failed to simulate drain")
				continue
			}
			if !canDrain {
				logForNode.Info("Rejected by drain simulation", "reason", strings.Join(reasons, ";"))
				continue
			}

			logForNode.Info("Adding drain candidate taint")
			if !runner.dryRun {

				if blockingPods, errPvProtection := runner.pvProtector.GetUnscheduledPodsBoundToNodeByPV(node); errPvProtection != nil {
					logForNode.Error(err, "Failed to run PV protection")
					continue
				} else if len(blockingPods) > 0 {
					kubernetes.LogrForVerboseNode(runner.logger, node, "Node can't become drain candidate: Pod needs to be scheduled on node due to PV binding", "pod", blockingPods[0].Namespace+"/"+blockingPods[0].Name)
					continue
				}

				if _, errTaint := k8sclient.AddNLATaint(ctx, runner.client, node, runner.clock.Now(), k8sclient.TaintDrainCandidate); errTaint != nil {
					logForNode.Error(errTaint, "Failed to taint node")
					continue // let's try next node, maybe this one has a problem
				}
			}
			remainCandidateSlot--
		}
		runner.logger.Info("Remain slot after drain candidate analysis", "count", remainCandidateSlot)
		dataInfo.Slots = fmt.Sprintf("%d/%d", remainCandidateSlot, runner.maxSimultaneousCandidates)

	}, runner.runEvery)
	return nil
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
	var errors []error
	for _, node := range nodes {
		if val, exist := node.Annotations[drainRetryFailedAnnotationKey]; exist && val == drainRetryRestartAnnotationValue {
			if err := runner.retryWall.ResetRetryCount(ctx, node); err != nil {
				errors = append(errors, fmt.Errorf("cannot reset retry wall on node '%s': %v", node.Name, err))
			}
		}
	}
	return utils.JoinErrors(errors, "|")
}
