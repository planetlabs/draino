package drain_runner

import (
	"context"
	"strings"
	"time"

	"github.com/go-logr/logr"
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

// TODO is this a good value?
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

	preprocessors []DrainPreProzessor
}

func (runner *drainRunner) Run(info *groups.RunnerInfo) error {
	ctx, cancel := context.WithCancel(info.Context)
	// run an endless loop until there are no drain candidates left
	wait.UntilWithContext(ctx, func(ctx context.Context) {
		candidates, err := runner.getDrainCandidates(ctx, info.Key)
		// in case of an error we'll just try it again
		if err != nil {
			runner.logger.Error(err, "cannot get drain candidates for group", "group_key", info.Key)
			return
		}
		// TODO add metric to track amount of candidates
		if len(candidates) == 0 {
			// If there are no candidates left, we'll stop the loop
			runner.logger.Info("no candidates in group left, stopping.", "group_key", info.Key)
			cancel()
			return
		}

		for _, candidate := range candidates {
			if err := runner.handleCandidate(info.Context, candidate); err != nil {
				runner.logger.Error(err, "error during candidate evaluation", "node_name", candidate.Name)
			}
		}
	}, runner.runEvery)
	return nil
}

func (runner *drainRunner) handleCandidate(ctx context.Context, candidate *corev1.Node) error {
	pods, err := runner.pvProtector.GetUnscheduledPodsBoundToNodeByPV(candidate)
	if err != nil {
		return err
	}

	// TODO add metric to track how often this happens
	if len(pods) > 0 {
		runner.logger.Info("Removing candidate status, because node is hosting PV for pods", "node_name", candidate.Name, "pods", strings.Join(utils.GetPodNames(pods), "; "))
		_, err := k8sclient.RemoveNLATaint(ctx, runner.client, candidate)
		return err
	}

	allPreprocessorsDone := runner.checkPreprocessors(candidate)
	if !allPreprocessorsDone {
		runner.logger.Info("waiting for preprocessors to be done before draining", "node_name", candidate.Name)
		return nil
	}

	runner.logger.Info("all preprocessors of candidate are done; will start draining", "node_name", candidate.Name)

	// Draining a node is a blocking operation. This makes sure that one drain does not affect the other by taking PDB budget.
	// TODO add metric to show how many drains are successful / failed
	err = runner.drainCandidate(ctx, candidate)
	if err != nil {
		runner.logger.Error(err, "failed to drain node", "node_name", candidate.Name)
		return runner.removeFailedCandidate(ctx, candidate, err.Error())
	}

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

func (runner *drainRunner) drainCandidate(ctx context.Context, candidate *corev1.Node) error {
	// TODO add metric about to track the runtime of this method
	candidate, err := k8sclient.AddNLATaint(ctx, runner.client, candidate, runner.clock.Now(), k8sclient.TaintDraining)
	if err != nil {
		return err
	}

	// This will make sure that the individual drain, will not block the loop forever
	drainContext, cancel := context.WithTimeout(ctx, DrainTimeout)
	defer cancel()

	err = runner.drainer.Drain(drainContext, candidate)
	if err != nil {
		return err
	}

	_, err = k8sclient.AddNLATaint(ctx, runner.client, candidate, runner.clock.Now(), k8sclient.TaintDrained)
	return err
}

func (runner *drainRunner) removeFailedCandidate(ctx context.Context, candidate *corev1.Node, reason string) error {
	err := runner.retryWall.SetNewRetryWallTimestamp(ctx, candidate, reason, runner.clock.Now())
	if err != nil {
		return err
	}

	_, err = k8sclient.RemoveNLATaint(ctx, runner.client, candidate)
	return err
}

func (runner *drainRunner) getDrainCandidates(ctx context.Context, key groups.GroupKey) ([]*corev1.Node, error) {
	nodes, err := index.GetFromIndex[corev1.Node](ctx, runner.sharedIndexInformer, groups.SchedulingGroupIdx, string(key))
	if err != nil {
		return nil, err
	}

	candidates := make([]*corev1.Node, 0)
	for _, node := range nodes {
		taint, exist := k8sclient.GetNLATaint(node)
		// if the node doesn't have the draino taint, it should not be processed
		if !exist {
			continue
		}
		// We only care about nodes that are "drain candidates" all the other states should be ignored
		if taint.Value != k8sclient.TaintDrainCandidate {
			continue
		}
		candidates = append(candidates, node.DeepCopy())
	}

	return candidates, nil
}
