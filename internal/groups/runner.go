package groups

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/planetlabs/draino/internal/kubernetes/utils"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/util/wait"
)

type RunnerInfo struct {
	Context context.Context
	Key     GroupKey
	Data    *utils.DataMap
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
	parentContext       context.Context
	running             map[GroupKey]*RunnerInfo
	factory             RunnerFactory
	logger              logr.Logger
	maxRandomStartDelay time.Duration
}

func NewGroupsRunner(ctx context.Context, factory RunnerFactory, logger logr.Logger, groupName string, maxRandomStartDelay time.Duration) *GroupsRunner {
	gr := &GroupsRunner{
		parentContext:       ctx,
		running:             map[GroupKey]*RunnerInfo{},
		factory:             factory,
		logger:              logger.WithValues("group_name", groupName),
		maxRandomStartDelay: maxRandomStartDelay,
	}

	go gr.observe()

	return gr
}

func (g *GroupsRunner) RunForGroup(key GroupKey) {
	// On startup, draino processes all the nodes with two threads simultaneously and opens the corresponding drain groups.
	// This means that, in the worst case, they are calling this function for the same groupKey at the same time.
	// If using an RLock, both would read the map simultaneously and notice that there is no runner, so they will go on and create new ones,
	// which means that we'll have two goroutines for the same group.
	// Said this, we are using Lock at the beginning of the function to ensure that only one thread can execute this function simultaneously.
	g.Lock()
	defer g.Unlock()
	if _, alReadyRunning := g.running[key]; alReadyRunning {
		return
	}
	g.running[key] = g.runForGroup(key)
}

func (g *GroupsRunner) runForGroup(key GroupKey) *RunnerInfo {
	ctx, cancel := context.WithCancel(g.parentContext)
	r := &RunnerInfo{
		Key:     key,
		Context: ctx,
		Data:    utils.NewDataMap(),
	}
	go func(runInfo *RunnerInfo, cancel context.CancelFunc) {
		defer cancel()
		var delay time.Duration
		if g.maxRandomStartDelay > 0 {
			delay = time.Duration(rand.Int31n(int32(g.maxRandomStartDelay.Seconds()))) * time.Second
		}
		g.logger.Info("Scheduling group opening", "groupKey", key, "delay", delay)
		time.Sleep(delay)
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
