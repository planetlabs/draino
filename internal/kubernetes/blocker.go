package kubernetes

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	corev1 "k8s.io/api/core/v1"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"k8s.io/apimachinery/pkg/util/wait"
)

type GlobalBlocker interface {
	IsBlocked() (bool, string)
	AddBlocker(name string, checkFunc ComputeBlockStateFunction, period time.Duration) error
	GetBlockStateCacheAccessor() map[string]GetBlockStateFunction
	Run(stopCh <-chan struct{})
	Start(context.Context) error
}

// ComputeBlockStateFunction a function that would analyse the system state and return true if we should lock draino to prevent any cordon/drain activity
type ComputeBlockStateFunction func() bool
type ComputeBlockStateFunctionFactory func(idx *index.Indexer, logger logr.Logger) ComputeBlockStateFunction

// GetBlockStateFunction a function that would return the current state of the lock using the cached value (no analysis) true=blocked
type GetBlockStateFunction func() bool

type blocker struct {
	name       string
	checkFunc  ComputeBlockStateFunction
	period     time.Duration
	blockState bool
}

func (l *blocker) updateBlockState() {
	l.blockState = l.checkFunc()
}

var _ GlobalBlocker = &GlobalBlocksRunner{}

type GlobalBlocksRunner struct {
	sync.Mutex
	blockers []*blocker
	started  bool
	logger   logr.Logger
}

func NewGlobalBlocker(logger logr.Logger) *GlobalBlocksRunner {
	return &GlobalBlocksRunner{
		logger: logger,
	}
}

var (
	MeasureBlocker = stats.Int64("draino/global_block", "GlobalBlock indicator.", stats.UnitDimensionless)
)

func (g *GlobalBlocksRunner) Start(ctx context.Context) error {
	g.Run(ctx.Done())
	return nil
}

func (g *GlobalBlocksRunner) Run(stopCh <-chan struct{}) {
	if g.started {
		g.logger.Info("GlobalBlocker run twice")
		<-stopCh
		return
	}

	if len(g.blockers) == 0 {
		g.logger.Info("No blocker to run")
		<-stopCh
		return
	}

	tagBlock, _ := tag.NewKey("block")
	blockerView := &view.View{
		Name:        "global_block",
		Measure:     MeasureBlocker,
		Description: "State of global blocks",
		Aggregation: view.LastValue(),
		TagKeys:     []tag.Key{tagBlock},
	}
	view.Register(blockerView)
	g.Lock()
	defer g.Unlock()
	g.started = true

	var wg sync.WaitGroup
	for i := range g.blockers {
		wg.Add(1)
		go func(b *blocker) {
			defer wg.Done()
			wait.Until(
				func() {
					// Perform Check
					b.updateBlockState()
					val := int64(0)
					if b.blockState {
						val = 1
					}
					// Observability
					tag, _ := tag.New(context.Background(), tag.Upsert(tagBlock, b.name))
					stats.Record(tag, MeasureBlocker.M(val))
				},
				b.period,
				stopCh)
		}(g.blockers[i])
	}
	wg.Wait()
}

func (g *GlobalBlocksRunner) AddBlocker(name string, checkFunc ComputeBlockStateFunction, period time.Duration) error {
	g.Lock()
	defer g.Unlock()
	if g.started {
		return errors.New("Can't add a Blocker once the GlobalBlocker has been started")
	}
	g.blockers = append(g.blockers, &blocker{
		name:       name,
		checkFunc:  checkFunc,
		period:     period,
		blockState: false,
	})
	return nil
}

func (g *GlobalBlocksRunner) GetBlockStateCacheAccessor() map[string]GetBlockStateFunction {
	m := map[string]GetBlockStateFunction{}
	for i := range g.blockers {
		l := g.blockers[i]
		m[l.name] = func() bool {
			return l.blockState
		}
	}
	return m
}

func (g *GlobalBlocksRunner) IsBlocked() (bool, string) {
	for _, l := range g.blockers {
		if l.blockState {
			return true, l.name
		}
	}
	return false, ""
}

func MaxNotReadyNodesCheckFunc(max int, percent bool, idx *index.Indexer, logger logr.Logger) ComputeBlockStateFunction {
	return func() bool {
		nodes, err := idx.GetAllNodes()
		if err != nil {
			return false
		}

		notReadyCount := 0
		for _, n := range nodes {
			if ready, _ := GetReadinessState(n); !ready {
				notReadyCount++
			}
		}
		blocked := false
		if percent {
			blocked = math.Ceil(100*float64(notReadyCount)/float64(len(nodes))) > float64(max)
		} else {
			blocked = notReadyCount >= max
		}
		return blocked
	}
}

func MaxPendingPodsCheckFunc(max int, percent bool, idx *index.Indexer, logger logr.Logger) ComputeBlockStateFunction {
	return func() bool {
		totalPodCount, err := idx.GetPodCount(context.Background())
		if err != nil {
			return false
		}

		pendingPods, err := idx.GetPodsByPhase(context.Background(), corev1.PodPending)
		if err != nil {
			return false
		}

		pendingCount := len(pendingPods)

		blocked := false
		if percent {
			blocked = math.Ceil(100*float64(pendingCount)/float64(totalPodCount)) > float64(max)
		} else {
			blocked = pendingCount >= max
		}
		return blocked
	}
}
