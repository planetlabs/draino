package drain

import (
	"time"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	"github.com/planetlabs/draino/internal/kubernetes/utils"
	"k8s.io/apimachinery/pkg/runtime"
)

type FakeSimulatorOptions struct {
	Chan            chan struct{}
	CleanupDuration *time.Duration
	CacheTTL        *time.Duration

	Objects   []runtime.Object
	PodFilter kubernetes.PodFilterFunc
}

func (opts *FakeSimulatorOptions) applyDefaults() {
	if opts.CacheTTL == nil {
		opts.CacheTTL = utils.DurationPtr(time.Minute)
	}
	if opts.CleanupDuration == nil {
		opts.CleanupDuration = utils.DurationPtr(10 * time.Second)
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
	}

	return simulator, nil
}
