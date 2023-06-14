package metrics

const (
	GlobalSubsystem          = "global"
	RetryWallSubsystem       = "retry_wall"
	FiltersSubsystem         = "filters"
	RunnerSubsystem          = "group_runner"
	CandidateRunnerSubsystem = "candidate_runner"
)

const (
	TagConditionAnyValue = "any"

	TagNodegroupName      = "nodegroup_name"
	TagNodegroupNamespace = "nodegroup_namespace"
	TagNodeName           = "node_name"
	TagGroupKey           = "group_key"
	TagRunnerName         = "runner_name"
	TagComponentName      = "component"
	TagReason             = "reason"
	TagDryRun             = "dry_run"
	TagFilter             = "filter"
	TagPreProcessor       = "pre_processor"
	TagTeam               = "team"
	TagService            = "service"
)
