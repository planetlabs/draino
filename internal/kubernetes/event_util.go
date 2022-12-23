package kubernetes

import (
	"context"
	"fmt"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"k8s.io/apimachinery/pkg/types"

	core "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
)

// This interface centralizes all k8s event interaction for this project.
// See also https://datadoghq.atlassian.net/wiki/spaces/~960205474/pages/2251949026/Draino+and+Node+Problem+Detector+Event+Inventory
// which is a datadog specific catalog of all events emit by NLA serving as documentation for users. Changes to events in this project should be reflected in that page.
type EventRecorder interface {
	NodeEventf(ctx context.Context, obj *core.Node, eventtype, reason, messageFmt string, args ...interface{})
	PodEventf(ctx context.Context, obj *core.Pod, eventtype, reason, messageFmt string, args ...interface{})
	PersistentVolumeEventf(ctx context.Context, obj *core.PersistentVolume, eventtype, reason, messageFmt string, args ...interface{})
	PersistentVolumeClaimEventf(ctx context.Context, obj *core.PersistentVolumeClaim, eventtype, reason, messageFmt string, args ...interface{})
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

func createSpan(ctx context.Context, operationName string, name string, eventType, reason, messageFmt string, args ...interface{}) (tracer.Span, context.Context) {
	span, ctx := tracer.StartSpanFromContext(ctx, operationName)

	span.SetTag("name", name)
	span.SetTag("eventType", eventType)
	span.SetTag("reason", reason)
	span.SetTag("message", fmt.Sprintf(messageFmt, args...))
	return span, ctx
}

func (e *eventRecorder) NodeEventf(ctx context.Context, obj *core.Node, eventType, reason, messageFmt string, args ...interface{}) {
	span, _ := createSpan(ctx, "NodeEvent", obj.GetName(), eventType, reason, messageFmt, args...)
	defer span.Finish()

	// Events must be associated with this object reference, rather than the
	// node itself, in order to appear under `kubectl describe node` due to the
	// way that command is implemented.
	// https://github.com/kubernetes/kubernetes/blob/17740a2/pkg/printers/internalversion/describe.go#L2711
	nodeReference := &core.ObjectReference{Kind: "Node", Name: obj.GetName(), UID: types.UID(obj.GetName())}
	e.eventRecorder.Eventf(nodeReference, eventType, reason, messageFmt, args...)
}

func (e *eventRecorder) PodEventf(ctx context.Context, obj *core.Pod, eventType, reason, messageFmt string, args ...interface{}) {
	span, _ := createSpan(ctx, "PodEvent", obj.GetName(), eventType, reason, messageFmt, args...)
	defer span.Finish()

	e.eventRecorder.Eventf(obj, eventType, reason, messageFmt, args...)
}

func (e *eventRecorder) PersistentVolumeEventf(ctx context.Context, obj *core.PersistentVolume, eventType, reason, messageFmt string, args ...interface{}) {
	span, _ := createSpan(ctx, "PesistentVolumeEvent", obj.GetName(), eventType, reason, messageFmt, args...)
	defer span.Finish()

	e.eventRecorder.Eventf(obj, eventType, reason, messageFmt, args...)
}

func (e *eventRecorder) PersistentVolumeClaimEventf(ctx context.Context, obj *core.PersistentVolumeClaim, eventType, reason, messageFmt string, args ...interface{}) {
	span, _ := createSpan(ctx, "PersistentVolumeClaim", obj.GetName(), eventType, reason, messageFmt, args...)
	defer span.Finish()

	e.eventRecorder.Eventf(obj, eventType, reason, messageFmt, args...)
}

type NoopEventRecorder struct{}

func (n NoopEventRecorder) NodeEventf(ctx context.Context, obj *core.Node, eventtype, reason, messageFmt string, args ...interface{}) {
}
func (n NoopEventRecorder) PodEventf(ctx context.Context, obj *core.Pod, eventtype, reason, messageFmt string, args ...interface{}) {
}
func (n NoopEventRecorder) PersistentVolumeEventf(ctx context.Context, obj *core.PersistentVolume, eventtype, reason, messageFmt string, args ...interface{}) {
}
func (n NoopEventRecorder) PersistentVolumeClaimEventf(ctx context.Context, obj *core.PersistentVolumeClaim, eventtype, reason, messageFmt string, args ...interface{}) {
}

var _ EventRecorder = &NoopEventRecorder{}
