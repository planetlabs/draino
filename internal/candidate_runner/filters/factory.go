package filters

// FilterFactory factory to build a composite filter with all the known filters
type FilterFactory struct {
	conf *Config
}

// NewFactory will return a new filter factory
// It will return an error if the given configuration is invalid or incomplete.
func NewFactory(withOptions ...WithOption) (*FilterFactory, error) {
	conf := NewConfig()
	for _, opt := range withOptions {
		opt(conf)
	}

	if err := conf.Validate(); err != nil {
		return nil, err
	}
	return &FilterFactory{conf: conf}, nil
}

func (factory *FilterFactory) BuildCandidateFilter() Filter {
	f := &CompositeFilter{
		logger: factory.conf.logger.WithName("CandidateFilter"),
	}

	f.filters = []Filter{
		NewNodeWithConditionFilter(factory.conf.globalConfig.SuppliedConditions),
		NewNodeWithLabelFilter(factory.conf.nodeLabelFilterFunc),
		NewPodFilter(*factory.conf.logger, factory.conf.cordonFilter, factory.conf.objectsStore),
		NewRetryWallFilter(factory.conf.clock, factory.conf.retryWall),
		NewNodeTerminatingFilter(),
		NewStabilityPeriodFilter(factory.conf.stabilityPeriodChecker, factory.conf.clock),
		NewDrainBufferFilter(factory.conf.drainBuffer, factory.conf.clock, factory.conf.groupKeyGetter),
		NewGlobalBlockerFilter(factory.conf.globalBlocker),
		NewPVCBoundFilter(factory.conf.pvcProtector, factory.conf.eventRecorder),
		NewFailedNodeReplacementFilter(),
	}
	return f
}
