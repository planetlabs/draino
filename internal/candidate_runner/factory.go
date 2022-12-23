package candidate_runner

import (
	"github.com/planetlabs/draino/internal/groups"
)

// CandidateRunnerFactory can create new instances of drain runners
type CandidateRunnerFactory struct {
	conf *Config
}

// NewFactory will return a new group runner factory for the drain runner.
// It will return an error if the given configuration is invalid or incomplete.
func NewFactory(withOptions ...WithOption) (groups.RunnerFactory, error) {
	conf := NewConfig()
	for _, opt := range withOptions {
		opt(conf)
	}

	if err := conf.Validate(); err != nil {
		return nil, err
	}

	return &CandidateRunnerFactory{conf: conf}, nil
}

func (factory *CandidateRunnerFactory) BuildRunner() groups.Runner {
	return &candidateRunner{
		client:                    factory.conf.kubeClient,
		logger:                    factory.conf.logger.WithName("CandidateRunner"),
		clock:                     factory.conf.clock,
		retryWall:                 factory.conf.retryWall,
		sharedIndexInformer:       factory.conf.sharedIndexInformer,
		runEvery:                  factory.conf.rerunEvery,
		eventRecorder:             factory.conf.eventRecorder,
		objectsStore:              factory.conf.objectsStore,
		cordonFilter:              factory.conf.cordonFilter,
		nodeLabelsFilterFunc:      factory.conf.nodeLabelFilterFunc,
		globalConfig:              factory.conf.globalConfig,
		maxSimultaneousCandidates: factory.conf.maxSimultaneousCandidates,
		drainSimulator:            factory.conf.drainSimulator,
		dryRun:                    factory.conf.dryRun,
		nodeSorters:               factory.conf.nodeSorters,
		nodeIteratorFactory:       factory.conf.nodeIteratorFactory,
		pvProtector:               factory.conf.pvProtector,
	}
}
