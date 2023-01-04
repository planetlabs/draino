package kubernetes

import (
	"context"
	"fmt"
	"github.com/go-logr/logr"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	client "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"
	"sync"
	"time"

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

var computeLRUSizeOnce sync.Once
var lruSize = int(5000 * (6 + 1) * 1.10) // default value 5000 nodes with 6 pods (with +10%)

func BuildEventRecorder(logger logr.Logger, cs *client.Clientset, aggregationPeriod time.Duration, excludedPodsPerNode int, alsoLogEvents bool) (EventRecorder, record.EventRecorder) {
	logger = logger.WithName("EventRecorder")
	computeLRUSizeOnce.Do(func() {
		podsMax := 0
		nodeList, err := cs.CoreV1().Nodes().List(context.Background(), v1.ListOptions{})
		if err != nil {
			logger.Error(err, "can't make good estimation for LRU cache size. Using default value", "LRUCacheSize", lruSize)
			return
		}
		for _, n := range nodeList.Items {
			podsMax += int(n.Status.Capacity.Pods().Value()) - excludedPodsPerNode // excludedPodsPerNode represents the pod that won't get any events. Pods that are excluded, mostly DS.
		}
		lruSize = int(float64(podsMax+len(nodeList.Items)) * 1.10)
		logger.Info("Setting EventRecorder LRU cache size", "LRUCacheSize", lruSize, "maxPodEstimation", podsMax, "nodeCount", len(nodeList.Items))
	})

	options := record.CorrelatorOptions{
		LRUCacheSize: lruSize, // used for nodes and pods
		BurstSize:    1,
		QPS:          float32(1 / aggregationPeriod.Seconds()),
		MessageFunc:  func(event *core.Event) string { return event.Message }, // do not put any combined notification. It is useless since it appears for each message.

		// if we see the same event that varies only by message
		// more than 1 times in a 'aggregationPeriod'' period, aggregate the event
		MaxEvents:            1,
		MaxIntervalInSeconds: int(aggregationPeriod.Seconds()),

		SpamKeyFunc: getSpamKey,
	}
	b := record.NewBroadcasterWithCorrelatorOptions(options)

	b.StartRecordingToSink(&customEventSink{
		base:      &typedcore.EventSinkImpl{Interface: typedcore.New(cs.CoreV1().RESTClient()).Events("")},
		logger:    logger,
		logEvents: alsoLogEvents,
	})
	k8sEventRecorder := b.NewRecorder(scheme.Scheme, core.EventSource{Component: Component})
	eventRecorder := NewEventRecorder(k8sEventRecorder)
	return eventRecorder, k8sEventRecorder
}

// getSpamKey builds unique event key based on source, involvedObject
func getSpamKey(event *core.Event) string {
	s, _ := record.EventAggregatorByReasonFunc(event)
	return s
}

type customEventSink struct {
	base      record.EventSink
	logger    logr.Logger
	logEvents bool
}

func (c *customEventSink) Create(e *core.Event) (*core.Event, error) {
	if c.logEvents {
		c.logger.Info("Event occurred", "verb", "create", "namespace", e.InvolvedObject.Namespace, "name", e.InvolvedObject.Name, "fieldPath", e.InvolvedObject.FieldPath, "kind", e.InvolvedObject.Kind, "apiVersion", e.InvolvedObject.APIVersion, "type", e.Type, "reason", e.Reason, "message", e.Message, "count", e.Count)
	}
	return c.base.Create(e)
}

func (c *customEventSink) Update(e *core.Event) (*core.Event, error) {
	if c.logEvents {
		c.logger.Info("Event occurred", "verb", "update", "namespace", e.InvolvedObject.Namespace, "name", e.InvolvedObject.Name, "fieldPath", e.InvolvedObject.FieldPath, "kind", e.InvolvedObject.Kind, "apiVersion", e.InvolvedObject.APIVersion, "type", e.Type, "reason", e.Reason, "message", e.Message, "count", e.Count)
	}
	return c.base.Update(e)
}

func (c *customEventSink) Patch(eOld *core.Event, data []byte) (*core.Event, error) {
	e, err := c.base.Patch(eOld, data)
	if c.logEvents {
		if err != nil {
			c.logger.Error(err, "can't patch event", "verb", "patch", "namespace", eOld.InvolvedObject.Namespace, "name", eOld.InvolvedObject.Name, "fieldPath", eOld.InvolvedObject.FieldPath, "kind", eOld.InvolvedObject.Kind, "apiVersion", eOld.InvolvedObject.APIVersion, "type", eOld.Type, "reason", eOld.Reason, "message", eOld.Message, "count", eOld.Count)
		} else {
			c.logger.Info("Event occurred", "verb", "patch", "namespace", e.InvolvedObject.Namespace, "name", e.InvolvedObject.Name, "fieldPath", e.InvolvedObject.FieldPath, "kind", e.InvolvedObject.Kind, "apiVersion", e.InvolvedObject.APIVersion, "type", e.Type, "reason", e.Reason, "message", e.Message, "count", e.Count)
		}
	}
	return e, err
}
