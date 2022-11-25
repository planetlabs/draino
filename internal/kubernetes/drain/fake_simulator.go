package drain

import (
	"time"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	"github.com/planetlabs/draino/internal/kubernetes/utils"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type FakeSimulatorOptions struct {
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

func NewFakeDrainSimulator(ch chan struct{}, opts *FakeSimulatorOptions) (DrainSimulator, error) {
	opts.applyDefaults()

	fakeIndexer, err := index.NewFakeIndexer(ch, opts.Objects, logr.Discard())
	if err != nil {
		return nil, err
	}

	fakeClient := fake.NewFakeClient(opts.Objects...)

	simulator := &drainSimulatorImpl{
		podIndexer:     fakeIndexer,
		pdbIndexer:     fakeIndexer,
		client:         fakeClient,
		podResultCache: utils.NewTTLCache[simulationResult](*opts.CacheTTL, *opts.CleanupDuration),
		skipPodFilter:  opts.PodFilter,
	}

	return simulator, nil
}
