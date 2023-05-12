package candidate_runner

import (
	"errors"
	"time"

	"github.com/planetlabs/draino/internal/candidate_runner/filters"
	"github.com/planetlabs/draino/internal/limit"

	"github.com/planetlabs/draino/internal/scheduler"
	corev1 "k8s.io/api/core/v1"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/drain"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// WithOption is used to pass an option to the factory
type WithOption = func(conf *Config)

// Config configuration passed to the drain runner
type Config struct {
	// Have to be set
	logger              *logr.Logger
	kubeClient          client.Client
	sharedIndexInformer index.GetSharedIndexInformer
	eventRecorder       kubernetes.EventRecorder
	drainSimulator      drain.DrainSimulator
	nodeSorters         NodeSorters
	retryWall           drain.RetryWall
	filter              filters.Filter
	rateLimiter         limit.TypedRateLimiter
	suppliedCondition   []kubernetes.SuppliedCondition

	// With defaults
	clock                     clock.Clock
	rerunEvery                time.Duration
	maxSimultaneousCandidates int
	maxSimultaneousDrained    int
	dryRun                    bool
	nodeIteratorFactory       NodeIteratorFactory
}

// NewConfig returns a pointer to a new drain runner configuration
func NewConfig() *Config {
	return &Config{
		clock:                     clock.RealClock{},
		rerunEvery:                time.Second,
		dryRun:                    true,
		maxSimultaneousCandidates: 1,
		maxSimultaneousDrained:    5,
		nodeIteratorFactory: func(nodes []*corev1.Node, sorters NodeSorters) scheduler.ItemProvider[*corev1.Node] {
			return scheduler.NewSortingTreeWithInitialization(nodes, sorters)
		},
	}
}

// Validate validates the configuration and will return an error in case of misconfiguration
func (conf *Config) Validate() error {
	if conf.logger == nil {
		return errors.New("logger should be set")
	}
	if conf.kubeClient == nil {
		return errors.New("kube client should be set")
	}
	if conf.sharedIndexInformer == nil {
		return errors.New("get shared index informer should be set")
	}
	if conf.eventRecorder == nil {
		return errors.New("event recorder should be set")
	}
	if conf.drainSimulator == nil {
		return errors.New("drain simulator not defined")
	}
	if conf.nodeSorters == nil {
		return errors.New("node sorters are note defined")
	}
	if conf.filter == nil {
		return errors.New("filter is not set")
	}
	if conf.retryWall == nil {
		return errors.New("retry wall is not set")
	}
	if conf.rateLimiter == nil {
		return errors.New("rate limiter is not set")
	}
	if len(conf.suppliedCondition) == 0 {
		return errors.New("global config is not set")
	}

	return nil
}

func WithKubeClient(client client.Client) WithOption {
	return func(conf *Config) {
		conf.kubeClient = client
	}
}

func WithLogger(logger logr.Logger) WithOption {
	return func(conf *Config) {
		conf.logger = &logger
	}
}

func WithRerun(rerun time.Duration) WithOption {
	return func(conf *Config) {
		conf.rerunEvery = rerun
	}
}

func WithClock(c clock.Clock) WithOption {
	return func(conf *Config) {
		conf.clock = c
	}
}

func WithSharedIndexInformer(inf index.GetSharedIndexInformer) WithOption {
	return func(conf *Config) {
		conf.sharedIndexInformer = inf
	}
}

func WithEventRecorder(er kubernetes.EventRecorder) WithOption {
	return func(conf *Config) {
		conf.eventRecorder = er
	}
}

func WithMaxSimultaneousCandidates(maxSimultaneousCandidates int) WithOption {
	return func(conf *Config) {
		conf.maxSimultaneousCandidates = maxSimultaneousCandidates
	}
}

func WithMaxSimultaneousDrained(maxSimultaneousDrained int) WithOption {
	return func(conf *Config) {
		conf.maxSimultaneousDrained = maxSimultaneousDrained
	}
}

func WithDrainSimulator(drainSimulator drain.DrainSimulator) WithOption {
	return func(conf *Config) {
		conf.drainSimulator = drainSimulator
	}
}

func WithDryRun(dryRun bool) WithOption {
	return func(conf *Config) {
		conf.dryRun = dryRun
	}
}

func WithNodeSorters(sorters NodeSorters) WithOption {
	return func(conf *Config) {
		conf.nodeSorters = sorters
	}
}

func WithNodeIteratorFactory(nodeIteratorFactory NodeIteratorFactory) WithOption {
	return func(conf *Config) {
		conf.nodeIteratorFactory = nodeIteratorFactory
	}
}

func WithFilter(filter filters.Filter) WithOption {
	return func(conf *Config) {
		conf.filter = filter
	}
}

func WithRetryWall(retryWall drain.RetryWall) WithOption {
	return func(conf *Config) {
		conf.retryWall = retryWall
	}
}

func WithRateLimiter(limiter limit.TypedRateLimiter) WithOption {
	return func(conf *Config) {
		conf.rateLimiter = limiter
	}
}

func WithGlobalConfig(globalConfig kubernetes.GlobalConfig) WithOption {
	return func(conf *Config) {
		conf.suppliedCondition = globalConfig.SuppliedConditions
	}
}
