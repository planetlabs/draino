package drain

import (
	"time"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	"github.com/planetlabs/draino/internal/kubernetes/utils"
	"github.com/planetlabs/draino/internal/limit"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/clock"
)

type FakeSimulatorOptions struct {
	Chan            chan struct{}
	CleanupDuration *time.Duration
	CacheTTL        *time.Duration
	RateLimiter     limit.RateLimiter
	Clock           clock.Clock

	Objects   []runtime.Object
	PodFilter kubernetes.PodFilterFunc
}

func (opts *FakeSimulatorOptions) applyDefaults() {
	if opts.Clock == nil {
		opts.Clock = clock.RealClock{}
	}
	if opts.CacheTTL == nil {
		opts.CacheTTL = utils.DurationPtr(time.Minute)
	}
	if opts.CleanupDuration == nil {
		opts.CleanupDuration = utils.DurationPtr(10 * time.Second)
	}
	if opts.RateLimiter == nil {
		opts.RateLimiter = limit.NewRateLimiter(opts.Clock, 100, 100)
	}
}

func NewFakeDrainSimulator(opts *FakeSimulatorOptions) (DrainSimulator, error) {
	opts.applyDefaults()

	wrapper, err := k8sclient.NewFakeClient(k8sclient.FakeConf{Objects: opts.Objects})
	if err != nil {
		return nil, err
	}

	fakeIndexer, err := index.New(wrapper.GetManagerClient(), wrapper.GetCache(), logr.Discard())
	if err != nil {
		return nil, err
	}

	wrapper.Start(opts.Chan)

	simulator := &drainSimulatorImpl{
		podIndexer:     fakeIndexer,
		pdbIndexer:     fakeIndexer,
		client:         wrapper.GetManagerClient(),
		podResultCache: utils.NewTTLCache[simulationResult](*opts.CacheTTL, *opts.CleanupDuration),
		skipPodFilter:  opts.PodFilter,
		eventRecorder:  kubernetes.NoopEventRecorder{},
		rateLimiter:    opts.RateLimiter,
		logger:         logr.Discard(),
	}

	return simulator, nil
}
