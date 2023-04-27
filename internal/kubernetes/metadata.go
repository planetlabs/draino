package kubernetes

import "time"

const (
	// DefaultDrainBuffer is the default minimum time between node drains.
	DefaultDrainBuffer               = 10 * time.Minute
	DefaultDurationBeforeReplacement = 1 * time.Hour

	EventReasonPendingPodWithLocalPV = "PodBoundToNodeViaLocalPV"

	EventReasonDrainStarting  = "DrainStarting"
	EventReasonDrainSucceeded = "DrainSucceeded"
	EventReasonDrainFailed    = "DrainFailed"
	eventReasonDrainConfig    = "DrainConfig"

	eventReasonNodePreprovisioning          = "NodePreprovisioning"
	eventReasonNodePreprovisioningCompleted = "NodePreprovisioningCompleted"

	tagResultSucceeded = "succeeded"
	tagResultFailed    = "failed"

	newNodeRequestReasonPreprovisioning = "preprovisioning"
	newNodeRequestReasonReplacement     = "replacement"

	drainRetryAnnotationKey         = "draino/drain-retry"
	drainRetryAnnotationValue       = "true"
	drainRetryFailedAnnotationKey   = "draino/drain-retry-failed"
	drainRetryFailedAnnotationValue = "failed"

	drainoConditionsAnnotationKey = "draino.planet.com/conditions"

	NodeNLAEnableLabelKey = "node-lifecycle.datadoghq.com/enabled"
)
