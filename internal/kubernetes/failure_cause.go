package kubernetes

import (
	"errors"
	"fmt"
)

type FailureCause string

const (
	OverlappingPodDisruptionBudgets FailureCause = "overlapping_pod_disruption_budgets"
	PodEvictionTimeout              FailureCause = "pod_eviction_timeout"
	PodDeletionTimeout              FailureCause = "pod_deletion_timeout"
	VolumeCleanup                   FailureCause = "volume_cleanup"
	NodePreprovisioning             FailureCause = "node_preprovisioning_timeout"
	AudienceNotFound                FailureCause = "audience_not_found"
)

func GetFailureCause(err error) FailureCause {
	if errors.As(err, &NodePreprovisioningTimeoutError{}) {
		return NodePreprovisioning
	}
	if errors.As(err, &OverlappingDisruptionBudgetsError{}) {
		return OverlappingPodDisruptionBudgets
	}
	var peErr PodEvictionTimeoutError
	if errors.As(err, &peErr) {
		cause := PodEvictionTimeout
		if peErr.isEvictionPP {
			cause += "_evictionpp"
		} else {
			cause += "_kubeapi"
		}
		return cause
	}
	if errors.As(err, &PodDeletionTimeoutError{}) {
		return PodDeletionTimeout
	}
	if errors.As(err, &VolumeCleanupError{}) {
		return VolumeCleanup
	}
	var eeErr EvictionEndpointError
	if errors.As(err, &eeErr) {
		cause := "eviction_endpoint"
		if eeErr.IsRequestTimeout {
			cause += "_request_timeout"
		}
		if eeErr.StatusCode > 0 {
			cause += fmt.Sprintf("_%d", eeErr.StatusCode)
		}
		return FailureCause(cause)
	}
	if errors.As(err, &AudienceNotFoundError{}) {
		return AudienceNotFound
	}

	return ""
}
