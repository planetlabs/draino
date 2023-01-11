package diagnostics

// CandidateRunnerFactory can create new instances of drain runners
type Factory struct {
	conf *Config
}

// NewFactory will return a new group runner factory for the drain runner.
// It will return an error if the given configuration is invalid or incomplete.
func NewFactory(withOptions ...WithOption) (*Factory, error) {
	conf := NewConfig()
	for _, opt := range withOptions {
		opt(conf)
	}

	if err := conf.Validate(); err != nil {
		return nil, err
	}

	return &Factory{conf: conf}, nil
}

func (factory *Factory) build() *Diagnostics {
	return &Diagnostics{
		client:              factory.conf.kubeClient,
		logger:              factory.conf.logger.WithName("Diagnostics"),
		clock:               factory.conf.clock,
		drainSimulator:      factory.conf.drainSimulator,
		nodeSorters:         factory.conf.nodeSorters,
		nodeIteratorFactory: factory.conf.nodeIteratorFactory,
		filter:              factory.conf.filter,
		retryWall:           factory.conf.retryWall,
		suppliedConditions:  factory.conf.suppliedCondition,
		keyGetter:           factory.conf.keyGetter,
		drainBuffer:         factory.conf.drainBuffer,
		stabilityPeriod:     factory.conf.stabilityPeriodChecker,
	}
}
func (factory *Factory) BuildDiagnosticWriter() Diagnostician {
	return factory.build()
}
