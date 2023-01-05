package candidate_runner

import (
	"github.com/planetlabs/draino/internal/groups"
)

// CandidateRunnerFactory can create new instances of drain runners
type CandidateRunnerFactory struct {
	conf *Config
}

type CandidateRunnerFactoryInterface interface {
	groups.RunnerFactory
	BuildCandidateInfo() CandidateInfo
}

// NewFactory will return a new group runner factory for the drain runner.
// It will return an error if the given configuration is invalid or incomplete.
func NewFactory(withOptions ...WithOption) (CandidateRunnerFactoryInterface, error) {
	conf := NewConfig()
	for _, opt := range withOptions {
		opt(conf)
	}

	if err := conf.Validate(); err != nil {
		return nil, err
	}

	return &CandidateRunnerFactory{conf: conf}, nil
}

func (factory *CandidateRunnerFactory) build() *candidateRunner {
	return &candidateRunner{
		client:                    factory.conf.kubeClient,
		logger:                    factory.conf.logger.WithName("CandidateRunner"),
		clock:                     factory.conf.clock,
		sharedIndexInformer:       factory.conf.sharedIndexInformer,
		runEvery:                  factory.conf.rerunEvery,
		eventRecorder:             factory.conf.eventRecorder,
		maxSimultaneousCandidates: factory.conf.maxSimultaneousCandidates,
		drainSimulator:            factory.conf.drainSimulator,
		dryRun:                    factory.conf.dryRun,
		nodeSorters:               factory.conf.nodeSorters,
		nodeIteratorFactory:       factory.conf.nodeIteratorFactory,
		filter:                    factory.conf.filter,
		retryWall:                 factory.conf.retryWall,
	}
}
func (factory *CandidateRunnerFactory) BuildRunner() groups.Runner {
	return factory.build()
}

func (factory *CandidateRunnerFactory) BuildCandidateInfo() CandidateInfo {
	return factory.build()
}
