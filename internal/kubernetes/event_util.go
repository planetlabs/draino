package kubernetes

import (
	"k8s.io/apimachinery/pkg/types"

	core "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
)

// This interface centralizes all k8s event interaction for this project.
// See also https://datadoghq.atlassian.net/wiki/spaces/~960205474/pages/2251949026/Draino+and+Node+Problem+Detector+Event+Inventory
// which is a datadog specific catalog of all events emit by NLA serving as documentation for users. Changes to events in this project should be reflected in that page.
type EventRecorder interface {
	NodeEventf(obj *core.Node, eventtype, reason, messageFmt string, args ...interface{})
	PodEventf(obj *core.Pod, eventtype, reason, messageFmt string, args ...interface{})
	PersistentVolumeEventf(obj *core.PersistentVolume, eventtype, reason, messageFmt string, args ...interface{})
	PersistentVolumeClaimEventf(obj *core.PersistentVolumeClaim, eventtype, reason, messageFmt string, args ...interface{})
}

type eventRecorder struct {
	eventRecorder record.EventRecorder
}

// NewEventRecorder returns a new record.EventRecorder for the given client.
func NewEventRecorder(k8sEventRecorder record.EventRecorder) EventRecorder {
	return &eventRecorder{
		eventRecorder: k8sEventRecorder,
	}
}

func (e *eventRecorder) NodeEventf(node *core.Node, eventType, reason, messageFmt string, args ...interface{}) {
	// Events must be associated with this object reference, rather than the
	// node itself, in order to appear under `kubectl describe node` due to the
	// way that command is implemented.
	// https://github.com/kubernetes/kubernetes/blob/17740a2/pkg/printers/internalversion/describe.go#L2711
	nodeReference := &core.ObjectReference{Kind: "Node", Name: node.GetName(), UID: types.UID(node.GetName())}
	e.eventRecorder.Eventf(nodeReference, eventType, reason, messageFmt, args...)
}

func (e *eventRecorder) PodEventf(obj *core.Pod, eventType, reason, messageFmt string, args ...interface{}) {
	e.eventRecorder.Eventf(obj, eventType, reason, messageFmt, args...)
}

func (e *eventRecorder) PersistentVolumeEventf(obj *core.PersistentVolume, eventType, reason, messageFmt string, args ...interface{}) {
	e.eventRecorder.Eventf(obj, eventType, reason, messageFmt, args...)
}

func (e *eventRecorder) PersistentVolumeClaimEventf(obj *core.PersistentVolumeClaim, eventType, reason, messageFmt string, args ...interface{}) {
	e.eventRecorder.Eventf(obj, eventType, reason, messageFmt, args...)
}
