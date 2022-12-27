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

func (factory *FilterFactory) Build() Filter {
	f := &CompositeFilter{
		logger: factory.conf.logger.WithName("CompositeFilter"),
	}

	f.filters = []Filter{
		NewNodeWithConditionFilter(factory.conf.globalConfig.SuppliedConditions),
		NewNodeWithLabelFilter(factory.conf.nodeLabelFilterFunc),
		NewPodFilter(*factory.conf.logger, factory.conf.cordonFilter, factory.conf.objectsStore),
		NewRetryWallFilter(factory.conf.clock, factory.conf.retryWall),
		NewNodeTerminatingFilter(),
	}
	return f
}
