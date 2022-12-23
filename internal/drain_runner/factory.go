package drain_runner

import (
	"github.com/planetlabs/draino/internal/groups"
)

// DrainRunnerFactory can create new instances of drain runners
type DrainRunnerFactory struct {
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

	return &DrainRunnerFactory{conf: conf}, nil
}

func (factory *DrainRunnerFactory) BuildRunner() groups.Runner {
	return &drainRunner{
		client:              factory.conf.kubeClient,
		logger:              *factory.conf.logger,
		clock:               factory.conf.clock,
		retryWall:           factory.conf.retryWall,
		drainer:             factory.conf.drainer,
		sharedIndexInformer: factory.conf.sharedIndexInformer,
		runEvery:            factory.conf.rerunEvery,
		pvProtector:         factory.conf.pvProtector,
		eventRecorder:       factory.conf.eventRecorder,
		preprocessors:       factory.conf.preprocessors,
	}
}
