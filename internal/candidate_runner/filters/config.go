package filters

import (
	"errors"

	"github.com/go-logr/logr"
	drainbuffer "github.com/planetlabs/draino/internal/drain_buffer"
	"github.com/planetlabs/draino/internal/groups"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/analyser"
	"github.com/planetlabs/draino/internal/kubernetes/drain"
	"k8s.io/utils/clock"
)

// WithOption is used to pass an option to the factory
type WithOption = func(conf *Config)

// Config configuration passed to the drain runner
type Config struct {
	// Have to be set
	logger                 *logr.Logger
	retryWall              drain.RetryWall
	objectsStore           kubernetes.RuntimeObjectStore
	cordonFilter           kubernetes.PodFilterFunc
	nodeLabelFilterFunc    kubernetes.NodeLabelFilterFunc
	globalConfig           kubernetes.GlobalConfig
	stabilityPeriodChecker analyser.StabilityPeriodChecker
	groupKeyGetter         groups.GroupKeyGetter
	drainBuffer            drainbuffer.DrainBuffer
	globalBlocker          kubernetes.GlobalBlocker

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
	if conf.stabilityPeriodChecker == nil {
		return errors.New("stability period checker should be set")
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
	if conf.groupKeyGetter == nil {
		return errors.New("group key getter is not set")
	}
	if conf.drainBuffer == nil {
		return errors.New("drain buffer is not set")
	}
	if conf.globalBlocker == nil {
		return errors.New("global blocker is not set")
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

func WithStabilityPeriodChecker(checker analyser.StabilityPeriodChecker) WithOption {
	return func(conf *Config) {
		conf.stabilityPeriodChecker = checker
	}
}

func WithGroupKeyGetter(getter groups.GroupKeyGetter) WithOption {
	return func(conf *Config) {
		conf.groupKeyGetter = getter
	}
}

func WithDrainBuffer(drainBuffer drainbuffer.DrainBuffer) WithOption {
	return func(conf *Config) {
		conf.drainBuffer = drainBuffer
	}
}

func WithGlobalBlocker(globalBlocker kubernetes.GlobalBlocker) WithOption {
	return func(conf *Config) {
		conf.globalBlocker = globalBlocker
	}
}
