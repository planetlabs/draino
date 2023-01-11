package diagnostics

import (
	"errors"
	"github.com/planetlabs/draino/internal/candidate_runner"
	"github.com/planetlabs/draino/internal/candidate_runner/filters"
	drainbuffer "github.com/planetlabs/draino/internal/drain_buffer"
	"github.com/planetlabs/draino/internal/groups"
	"github.com/planetlabs/draino/internal/kubernetes/analyser"
	"github.com/planetlabs/draino/internal/scheduler"
	corev1 "k8s.io/api/core/v1"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/drain"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// WithOption is used to pass an option to the factory
type WithOption = func(conf *Config)

// Config configuration passed to the drain runner
type Config struct {
	// Have to be set
	logger                 *logr.Logger
	kubeClient             client.Client
	drainSimulator         drain.DrainSimulator
	nodeSorters            candidate_runner.NodeSorters
	retryWall              drain.RetryWall
	filter                 filters.Filter
	keyGetter              groups.GroupKeyGetter
	drainBuffer            drainbuffer.DrainBuffer
	suppliedCondition      []kubernetes.SuppliedCondition
	stabilityPeriodChecker analyser.StabilityPeriodChecker

	// With defaults
	clock               clock.Clock
	nodeIteratorFactory candidate_runner.NodeIteratorFactory
}

// NewConfig returns a pointer to a new drain runner configuration
func NewConfig() *Config {
	return &Config{
		clock: clock.RealClock{},
		nodeIteratorFactory: func(nodes []*corev1.Node, sorters candidate_runner.NodeSorters) scheduler.ItemProvider[*corev1.Node] {
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
	if conf.stabilityPeriodChecker == nil {
		return errors.New("stability period checker should be set")
	}
	if conf.keyGetter == nil {
		return errors.New("keyGetter is not set")
	}
	if conf.drainBuffer == nil {
		return errors.New("drainBuffer is not set")
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

func WithClock(c clock.Clock) WithOption {
	return func(conf *Config) {
		conf.clock = c
	}
}

func WithDrainSimulator(drainSimulator drain.DrainSimulator) WithOption {
	return func(conf *Config) {
		conf.drainSimulator = drainSimulator
	}
}

func WithNodeSorters(sorters candidate_runner.NodeSorters) WithOption {
	return func(conf *Config) {
		conf.nodeSorters = sorters
	}
}

func WithNodeIteratorFactory(nodeIteratorFactory candidate_runner.NodeIteratorFactory) WithOption {
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

func WithGlobalConfig(globalConfig kubernetes.GlobalConfig) WithOption {
	return func(conf *Config) {
		conf.suppliedCondition = globalConfig.SuppliedConditions
	}
}

func WithKeyGetter(keyGetter groups.GroupKeyGetter) WithOption {
	return func(conf *Config) {
		conf.keyGetter = keyGetter
	}
}

func WithDrainBuffer(drainBuffer drainbuffer.DrainBuffer) WithOption {
	return func(conf *Config) {
		conf.drainBuffer = drainBuffer
	}
}

func WithStabilityPeriodChecker(checker analyser.StabilityPeriodChecker) WithOption {
	return func(conf *Config) {
		conf.stabilityPeriodChecker = checker
	}
}
