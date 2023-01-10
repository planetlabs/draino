package groups

import (
	"context"
	"github.com/planetlabs/draino/internal/kubernetes/utils"
	"math/rand"
	"sync"
	"time"

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
