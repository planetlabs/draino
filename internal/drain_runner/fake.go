package drain_runner

import (
	"context"
	"fmt"
	"time"

	"k8s.io/client-go/tools/record"

	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-logr/logr"
	v1 "k8s.io/api/core/v1"
	"k8s.io/utils/clock"

	"github.com/planetlabs/draino/internal/candidate_runner/filters"
	drainbuffer "github.com/planetlabs/draino/internal/drain_buffer"
	preprocessor "github.com/planetlabs/draino/internal/drain_runner/pre_processor"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/drain"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
)

type FakeOptions struct {
	// Chan is used to start and keep the informers running
	Chan          chan struct{}
	ClientWrapper *k8sclient.FakeClientWrapper
	Preprocessors []preprocessor.DrainPreProcessor
	Logger        *logr.Logger
	RerunEvery    time.Duration
	Filter        filters.Filter
	DrainBuffer   drainbuffer.DrainBuffer
	NodeReplacer  *preprocessor.NodeReplacer

	Clock clock.Clock

	Drainer       kubernetes.Drainer
	RetryStrategy drain.RetryStrategy
}

func (opts *FakeOptions) ApplyDefaults() error {
	if opts.ClientWrapper == nil {
		return fmt.Errorf("Please pass client wrapper to fake runner")
	}

	if opts.Chan == nil {
		opts.Chan = make(chan struct{})
	}
	if opts.Preprocessors == nil {
		opts.Preprocessors = make([]preprocessor.DrainPreProcessor, 0)
	}
	if opts.Logger == nil {
		discard := logr.Discard()
		opts.Logger = &discard
	}
	if opts.RerunEvery == 0 {
		opts.RerunEvery = 5 * time.Millisecond
	}
	if opts.Clock == nil {
		opts.Clock = clock.RealClock{}
	}
	if opts.Drainer == nil {
		opts.Drainer = &kubernetes.NoopDrainer{}
	}
	if opts.RetryStrategy == nil {
		opts.RetryStrategy = &drain.StaticRetryStrategy{Delay: time.Second, AlertThreashold: 5}
	}
	if opts.NodeReplacer == nil {
		opts.NodeReplacer = preprocessor.NewNodeReplacer(opts.ClientWrapper.GetManagerClient(), *opts.Logger)
	}
	if opts.Filter == nil {
		opts.Filter = filters.FilterFromFunction("always_true", func(ctx context.Context, n *v1.Node) bool { return true })
	}
	if opts.DrainBuffer == nil {
		fakeClient := fake.NewSimpleClientset()
		configMapClient := fakeClient.CoreV1().ConfigMaps("default")
		persistor := drainbuffer.NewConfigMapPersistor(configMapClient, "fake-buffer", "default")
		recorder := kubernetes.NewEventRecorder(record.NewFakeRecorder(1000))
		opts.DrainBuffer = drainbuffer.NewDrainBuffer(context.Background(), persistor, opts.Clock, *opts.Logger, recorder, nil, nil, kubernetes.DefaultDrainBuffer)
	}
	return nil
}

// NewFakeRunner will create an instances of the drain runner with mocked dependencies.
// It will return an error if the given configuration is invalid or incomplete.
func NewFakeRunner(opts *FakeOptions) (*drainRunner, error) {
	if err := opts.ApplyDefaults(); err != nil {
		return nil, err
	}

	fakeIndexer, err := index.New(context.Background(), opts.ClientWrapper.GetManagerClient(), opts.ClientWrapper.GetCache(), *opts.Logger)
	if err != nil {
		return nil, err
	}

	// Start the informers and wait for them to sync
	opts.ClientWrapper.Start(opts.Chan)

	retryWall, err := drain.NewRetryWall(opts.ClientWrapper.GetManagerClient(), *opts.Logger, opts.RetryStrategy)
	if err != nil {
		return nil, err
	}

	err = opts.DrainBuffer.Initialize(context.Background())
	if err != nil {
		return nil, err
	}

	return &drainRunner{
		client:              opts.ClientWrapper.GetManagerClient(),
		logger:              *opts.Logger,
		clock:               opts.Clock,
		retryWall:           retryWall,
		sharedIndexInformer: fakeIndexer,
		drainer:             opts.Drainer,
		runEvery:            opts.RerunEvery,
		preprocessors:       opts.Preprocessors,
		eventRecorder:       &kubernetes.NoopEventRecorder{},
		filter:              opts.Filter,
		drainBuffer:         opts.DrainBuffer,
		nodeReplacer:        opts.NodeReplacer,

		durationWithDrainedStatusBeforeReplacement: time.Hour,
	}, nil
}
