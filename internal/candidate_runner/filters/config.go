package filters

import (
	"errors"
	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/drain"
	"k8s.io/utils/clock"
)

// WithOption is used to pass an option to the factory
type WithOption = func(conf *Config)

// Config configuration passed to the drain runner
type Config struct {
	// Have to be set
	logger              *logr.Logger
	retryWall           drain.RetryWall
	objectsStore        kubernetes.RuntimeObjectStore
	cordonFilter        kubernetes.PodFilterFunc
	nodeLabelFilterFunc kubernetes.NodeLabelFilterFunc
	globalConfig        kubernetes.GlobalConfig

	// With defaults
	clock clock.Clock
}

// NewConfig returns a pointer to a new drain runner configuration
func NewConfig() *Config {
	return &Config{
		clock: clock.RealClock{},
	}
}

// Validate validates the configuration and will return an error in case of misconfiguration
func (conf *Config) Validate() error {
	if conf.logger == nil {
		return errors.New("logger should be set")
	}
	if conf.retryWall == nil {
		return errors.New("retry wall should be set")
	}
	if conf.objectsStore == nil {
		return errors.New("runtime object store should be set")
	}
	if conf.nodeLabelFilterFunc == nil {
		return errors.New("node labels filtering function is not set")
	}
	if conf.cordonFilter == nil {
		return errors.New("node cordon filtering function is not set")
	}
	if conf.globalConfig.ConfigName == "" {
		return errors.New("globalConfig.ConfigName is not set")
	}
	if len(conf.globalConfig.SuppliedConditions) == 0 {
		return errors.New("globalConfig.SuppliedConditions is empty")
	}

	return nil
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

func WithRetryWall(wall drain.RetryWall) WithOption {
	return func(conf *Config) {
		conf.retryWall = wall
	}
}

func WithRuntimeObjectStore(store kubernetes.RuntimeObjectStore) WithOption {
	return func(conf *Config) {
		conf.objectsStore = store
	}
}

func WithNodeLabelsFilterFunction(filter kubernetes.NodeLabelFilterFunc) WithOption {
	return func(conf *Config) {
		conf.nodeLabelFilterFunc = filter
	}
}

func WithGlobalConfig(gc kubernetes.GlobalConfig) WithOption {
	return func(conf *Config) {
		conf.globalConfig = gc
	}
}

// WithCordonPodFilter configures a filter that may prevent to cordon nodes
// to avoid further impossible eviction when draining.
func WithCordonPodFilter(f kubernetes.PodFilterFunc) WithOption {
	return func(conf *Config) {
		conf.cordonFilter = f
	}
}
