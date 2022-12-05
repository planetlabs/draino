package groups

import (
	"context"
	"sync"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/util/wait"
)

type RunnerInfo struct {
	Context context.Context
	Key     GroupKey
}

// Runner is in charge of a set of nodes for a given group
// the runner exit normally if there is no more nodes in the group
type Runner interface {
	Run(r *RunnerInfo) error
}

type RunnerFactory interface {
	BuildRunner() Runner
}

type GroupsRunner struct {
	sync.RWMutex
	parentContext context.Context
	running       map[GroupKey]*RunnerInfo
	factory       RunnerFactory
	logger        logr.Logger
}

func NewGroupsRunner(ctx context.Context, factory RunnerFactory, logger logr.Logger, groupName string) *GroupsRunner {
	gr := &GroupsRunner{
		parentContext: ctx,
		running:       map[GroupKey]*RunnerInfo{},
		factory:       factory,
		logger:        logger.WithValues("group_name", groupName),
	}

	go gr.observe()

	return gr
}

func (g *GroupsRunner) RunForGroup(key GroupKey) {
	// do nothing if it is already running for that group
	g.RLock()
	if _, alReadyRunning := g.running[key]; alReadyRunning {
		g.RUnlock()
		return
	}
	g.RUnlock()
	g.Lock()
	g.running[key] = g.runForGroup(key)
	g.Unlock()
}

func (g *GroupsRunner) runForGroup(key GroupKey) *RunnerInfo {
	ctx, cancel := context.WithCancel(g.parentContext)
	r := &RunnerInfo{
		Key:     key,
		Context: ctx,
	}
	go func(runInfo *RunnerInfo, cancel context.CancelFunc) {
		defer cancel()
		g.logger.Info("Scheduling group opened", "groupKey", key)
		err := g.factory.BuildRunner().Run(runInfo)
		if err != nil {
			g.logger.Error(err, "Runner stopped with error", "groupKey", key)
		}

		g.Lock()
		delete(g.running, runInfo.Key)
		g.logger.Info("Scheduling group closed", "groupKey", key)
		g.Unlock()
	}(r, cancel)
	return r
}

func (g *GroupsRunner) countRunners() int {
	g.RLock()
	c := len(g.running)
	g.RUnlock()
	return c
}

func (g *GroupsRunner) observe() {
	wait.UntilWithContext(g.parentContext,
		func(ctx context.Context) {
			MetricsActiveRunner.SetAndPlanCleanup(float64(g.countRunners()), []string{}, false, 4*publicationPeriod, false)
		},
		publicationPeriod)
}
