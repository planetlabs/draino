package candidate_runner

import (
	"errors"
	"github.com/planetlabs/draino/internal/protector"
	"github.com/planetlabs/draino/internal/scheduler"
	corev1 "k8s.io/api/core/v1"
	"time"

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
	retryWall           drain.RetryWall
	sharedIndexInformer index.GetSharedIndexInformer
	eventRecorder       kubernetes.EventRecorder
	objectsStore        kubernetes.RuntimeObjectStore
	drainSimulator      drain.DrainSimulator
	nodeSorters         NodeSorters
	cordonFilter        kubernetes.PodFilterFunc
	nodeLabelFilterFunc kubernetes.NodeLabelFilterFunc
	globalConfig        kubernetes.GlobalConfig
	pvProtector         protector.PVProtector

	// With defaults
	clock                     clock.Clock
	rerunEvery                time.Duration
	maxSimultaneousCandidates int
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
		nodeIteratorFactory: func(nodes []*corev1.Node, sorters NodeSorters) scheduler.ItemProvider[*corev1.Node] {
			return scheduler.NewSortingTreeWithInitialization[*corev1.Node](nodes, sorters)
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
	if conf.retryWall == nil {
		return errors.New("retry wall should be set")
	}
	if conf.sharedIndexInformer == nil {
		return errors.New("get shared index informer should be set")
	}
	if conf.eventRecorder == nil {
		return errors.New("event recorder should be set")
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
	if conf.drainSimulator == nil {
		return errors.New("drain simulator not defined")
	}
	if conf.nodeSorters == nil {
		return errors.New("node sorters are note defined")
	}
	if conf.pvProtector == nil {
		return errors.New("pv protector should be set")
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

func WithRetryWall(wall drain.RetryWall) WithOption {
	return func(conf *Config) {
		conf.retryWall = wall
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

func WithMaxSimultaneousCandidates(maxSimultaneousCandidates int) WithOption {
	return func(conf *Config) {
		conf.maxSimultaneousCandidates = maxSimultaneousCandidates
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

func WithPVProtector(protector protector.PVProtector) WithOption {
	return func(conf *Config) {
		conf.pvProtector = protector
	}
}
