package kubernetes

import (
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

// Opencensus measurements.
var (
	MeasureNodesCordoned           = stats.Int64("draino/nodes_cordoned", "Number of nodes cordoned.", stats.UnitDimensionless)
	MeasureNodesUncordoned         = stats.Int64("draino/nodes_uncordoned", "Number of nodes uncordoned.", stats.UnitDimensionless)
	MeasureNodesDrained            = stats.Int64("draino/nodes_drained", "Number of nodes drained.", stats.UnitDimensionless)
	MeasureNodesDrainScheduled     = stats.Int64("draino/nodes_drainScheduled", "Number of nodes drain scheduled.", stats.UnitDimensionless)
	MeasureLimitedCordon           = stats.Int64("draino/cordon_limited", "Number of cordon activities that have been blocked due to limits.", stats.UnitDimensionless)
	MeasureSkippedCordon           = stats.Int64("draino/cordon_skipped", "Number of cordon activities that have been skipped due filtering.", stats.UnitDimensionless)
	MeasureNodesReplacementRequest = stats.Int64("draino/nodes_replacement_request", "Number of nodes replacement requested.", stats.UnitDimensionless)
	MeasurePreprovisioningLatency  = stats.Float64("draino/nodes_preprovisioning_latency", "Latency to get a node preprovisioned", stats.UnitMilliseconds)

	TagNodeName, _                        = tag.NewKey("node_name")
	TagConditions, _                      = tag.NewKey("conditions")
	TagTeam, _                            = tag.NewKey("team")
	TagNodegroupName, _                   = tag.NewKey("nodegroup_name")
	TagNodegroupNamePrefix, _             = tag.NewKey("nodegroup_name_prefix")
	TagNodegroupNamespace, _              = tag.NewKey("nodegroup_namespace")
	TagResult, _                          = tag.NewKey("result")
	TagReason, _                          = tag.NewKey("reason")
	TagFailureCause, _                    = tag.NewKey("failure_cause")
	TagInScope, _                         = tag.NewKey("in_scope")
	TagDrainStatus, _                     = tag.NewKey("drain_status")
	TagPreprovisioning, _                 = tag.NewKey("preprovisioning")
	TagPVCManagement, _                   = tag.NewKey("pvc_management")
	TagDrainRetry, _                      = tag.NewKey("drain_retry")
	TagDrainRetryFailed, _                = tag.NewKey("drain_retry_failed")
	TagDrainRetryCustomMaxAttempt, _      = tag.NewKey("drain_retry_custom_max_attempt")
	TagUserOptOutViaPodAnnotation, _      = tag.NewKey("user_opt_out_via_pod_annotation")
	TagUserOptInViaPodAnnotation, _       = tag.NewKey("user_opt_in_via_pod_annotation")
	TagUserAllowedConditionsAnnotation, _ = tag.NewKey("user_allowed_conditions_annotation")
	TagUserEvictionURL, _                 = tag.NewKey("eviction_url")
	TagOverdue, _                         = tag.NewKey("overdue")
)
