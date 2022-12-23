package candidate_runner

import (
	"context"
	"fmt"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	"github.com/planetlabs/draino/internal/kubernetes/utils"
	"github.com/planetlabs/draino/internal/protector"
	"github.com/planetlabs/draino/internal/scheduler"
	"strings"
	"time"

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

// Make sure that the drain runner is implementing the group runner interface
var _ groups.Runner = &candidateRunner{}

type NodeSorters []scheduler.LessFunc[*corev1.Node]
type NodeIteratorFactory func([]*corev1.Node, NodeSorters) scheduler.ItemProvider[*corev1.Node]

// candidateRunner implements the groups.Runner interface and will be used to drain nodes of the given group configuration
type candidateRunner struct {
	client               client.Client
	logger               logr.Logger
	clock                clock.Clock
	retryWall            drain.RetryWall
	sharedIndexInformer  index.GetSharedIndexInformer
	runEvery             time.Duration
	cordonFilter         kubernetes.PodFilterFunc
	eventRecorder        kubernetes.EventRecorder
	objectsStore         kubernetes.RuntimeObjectStore
	nodeLabelsFilterFunc kubernetes.NodeLabelFilterFunc
	globalConfig         kubernetes.GlobalConfig
	pvProtector          protector.PVProtector

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
		nodes, err := index.GetFromIndex[corev1.Node](ctx, runner.sharedIndexInformer, groups.SchedulingGroupIdx, string(info.Key))
		// in case of an error we'll just try it again
		if err != nil {
			runner.logger.Error(err, "cannot get nodes for group")
			return
		}

		// TODO add metric to track amount of nodes in the group
		if len(nodes) == 0 {
			// If there are no candidates left, we'll stop the loop
			runner.logger.Info("no nodes in group left, stopping.")
			cancel()
			return
		}

		// TODO add metrics after each filter ?
		// TODO is there a better order for filter that would use less time/resources ?

		// TODO formalize an interface for filters and register filters into the runner
		// filter nodes that are already candidate
		nodes, alreadyCandidateNodes, maxReached := runner.checkAlreadyCandidates(nodes)
		if maxReached {
			runner.logger.Info("Max candidate already reached", "count", runner.maxSimultaneousCandidates, "nodes", strings.Join(utils.NodesNames(alreadyCandidateNodes), ","))
			return
		}
		remainCandidateSlot := runner.maxSimultaneousCandidates - len(alreadyCandidateNodes)

		// check SuppliedConditions
		nodes = runner.checkNodesHaveAtLeastOneCondition(nodes)
		if len(nodes) == 0 {
			runner.logger.Info("No node with condition")
			return
		}

		// check that the node has the correct labels to be in scope
		nodes = runner.checkNodesLabels(nodes)
		if len(nodes) == 0 {
			runner.logger.Info("No node with expected labels")
			return
		}

		// filter nodes that are not in scope
		nodes = runner.checkCordonFilters(nodes)
		if len(nodes) == 0 {
			runner.logger.Info("No node that accept cordon filters")
			return
		}

		// filter nodes that have retry wall in the future
		nodes, _ = runner.checkNodesRetryWall(nodes)
		if len(nodes) == 0 {
			runner.logger.Info("No node after retryWall check")
			return
		}

		// filter nodes that are terminating
		nodes, _ = runner.checkNodesTerminating(nodes)
		if len(nodes) == 0 {
			runner.logger.Info("No node after Terminating node check")
			return
		}

		nodeProvider := runner.nodeIteratorFactory(nodes, runner.nodeSorters)
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

	}, runner.runEvery)
	return nil
}

func (runner *candidateRunner) checkNodesRetryWall(nodes []*corev1.Node) (keep, filterOut []*corev1.Node) {
	keep = make([]*corev1.Node, 0, len(nodes)) // high probability that all nodes are to be kept
	filterOut = make([]*corev1.Node, 0, 10)    // it is possible that we have some nodes with retryWalls
	for _, n := range nodes {
		if runner.retryWall.GetRetryWallTimestamp(n).After(runner.clock.Now()) {
			filterOut = append(filterOut, n)
		} else {
			keep = append(keep, n)
		}
	}
	return keep, filterOut
}

// checkNodesStatus remove nodes that are terminating
func (runner *candidateRunner) checkNodesTerminating(nodes []*corev1.Node) (keep, filterOut []*corev1.Node) {
	keep = make([]*corev1.Node, 0, len(nodes)) // high probability that all nodes are to be kept
	filterOut = make([]*corev1.Node, 0, 10)    // it is possible that we have some nodes with retryWalls
	for _, n := range nodes {
		if n.DeletionTimestamp != nil && !n.DeletionTimestamp.IsZero() {
			filterOut = append(filterOut, n)
		} else {
			keep = append(keep, n)
		}
	}
	return keep, filterOut
}

func (runner *candidateRunner) checkNodesLabels(nodes []*corev1.Node) []*corev1.Node {
	// TODO add tracing to see how much expensive this is. There is an expression evaluation at each call.
	var remainingNodes []*corev1.Node
	for _, n := range nodes {
		if runner.CheckNodeLabels(n).InScope {
			remainingNodes = append(remainingNodes, n)
		}
	}
	return remainingNodes
}

type NodeLabelResult struct {
	Node    *corev1.Node
	InScope bool
}

func (runner *candidateRunner) CheckNodeLabels(n *corev1.Node) NodeLabelResult {
	if runner.nodeLabelsFilterFunc == nil {
		return NodeLabelResult{
			Node:    n,
			InScope: false,
		}
	}

	return NodeLabelResult{
		Node: n,
		// TODO: Check how expensive this is and build a cache if needed
		InScope: runner.nodeLabelsFilterFunc(n),
	}
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

// checkCordonFilters return true if the filtering is ok to proceed
// if the node is labeled with `node-lifecycle.datadoghq.com/enabled` we do not check the pod and use the value set on the node
func (runner *candidateRunner) checkCordonFilters(nodes []*corev1.Node) []*corev1.Node {
	remainingNodes := make([]*corev1.Node, 0, len(nodes)) // high probability that all nodes are to be kept
	for _, n := range nodes {
		if runner.CheckCordonFiltersForNode(n).CanCordon {
			remainingNodes = append(remainingNodes, n)
		}
	}
	return remainingNodes
}

type CordonFilterResult struct {
	Node      *corev1.Node
	CanCordon bool
	Reason    string
	Pod       *corev1.Pod
}

// CheckCordonFiltersForNode check if a node can be cordon.
// TODO call this function every X minutes to diagnostic and report event EventReasonCordonSkip on pod and nodes
// TODO can also be called from the CLI to deliver diagnostics
func (runner *candidateRunner) CheckCordonFiltersForNode(n *corev1.Node) CordonFilterResult {
	if runner.cordonFilter == nil || runner.objectsStore == nil || runner.objectsStore.Pods() == nil {
		return CordonFilterResult{
			Node:      n,
			Reason:    "runner not correctly configured to perform checks",
			CanCordon: false,
		}
	}

	pods, err := runner.objectsStore.Pods().ListPodsForNode(n.Name)
	if err != nil {
		runner.logger.Error(err, "failed to list pods on node", "node", n.Name)
		return CordonFilterResult{
			Node:      n,
			CanCordon: false,
			Reason:    err.Error(),
		}
	}
	for _, pod := range pods {
		ok, reason, err := runner.cordonFilter(*pod)
		kubernetes.LogrForVerboseNode(runner.logger, n, "Cordon Filter", "pod", pod.Name, "reason", reason, "ok", ok)
		if err != nil {
			runner.logger.Error(err, "failed run cordon filter on pod", "node", n.Name, "pod", pod.Name, "namespace", n.Name)
			return CordonFilterResult{
				Node:      n,
				CanCordon: false,
				Reason:    err.Error(),
			}
		}
		if !ok {
			return CordonFilterResult{
				Node:      n,
				CanCordon: false,
				Reason:    reason,
				Pod:       pod,
			}
		}
	}
	return CordonFilterResult{
		Node:      n,
		CanCordon: true,
	}
}

func (runner *candidateRunner) checkNodesHaveAtLeastOneCondition(nodes []*corev1.Node) []*corev1.Node {
	remainingNodes := make([]*corev1.Node, 0, len(nodes)/10) // Low probability that all nodes are to be kept
	for _, n := range nodes {
		r := runner.CheckNodeConditions(n)
		if len(r.Conditions) > 0 && !r.Rejected {
			remainingNodes = append(remainingNodes, n)
		}
	}
	return remainingNodes
}

type NodeConditionsResult struct {
	Node       *corev1.Node
	Conditions []kubernetes.SuppliedCondition
	Rejected   bool
}

// CheckNodeConditions check if a node can be cordon.
// TODO call this function every X minutes to diagnostic and report event eventReasonConditionFiltered on nodes
// TODO can also be called from the CLI to deliver diagnostics
func (runner *candidateRunner) CheckNodeConditions(n *corev1.Node) NodeConditionsResult {

	badConditions := kubernetes.GetNodeOffendingConditions(n, runner.globalConfig.SuppliedConditions)
	kubernetes.LogrForVerboseNode(runner.logger, n, fmt.Sprintf("Offending conditions count %d", len(badConditions)))
	if len(badConditions) == 0 {
		return NodeConditionsResult{
			Node: n,
		}
	}
	badConditionsStr := kubernetes.GetConditionsTypes(badConditions)
	if !kubernetes.AtLeastOneConditionAcceptedByTheNode(badConditionsStr, n) {
		kubernetes.LogrForVerboseNode(runner.logger, n, "Conditions filter rejects that node")
		return NodeConditionsResult{
			Node:       n,
			Conditions: badConditions,
			Rejected:   true,
		}
	}
	return NodeConditionsResult{
		Node:       n,
		Conditions: badConditions,
		Rejected:   false,
	}
}
