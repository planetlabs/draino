package kubernetes

type GlobalConfig struct {
	// ConfigName name of the configuration running this controller, we can have different configuration and multiple draino pods (one per config)
	ConfigName string

	// PVCManagementEnableIfNoEvictionUrl PVC management is enabled by default if there is no evictionURL defined
	PVCManagementEnableIfNoEvictionUrl bool

	// SuppliedConditions List of conditions that the controller should react on
	SuppliedConditions []SuppliedCondition
}
