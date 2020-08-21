/*
Copyright 2018 Planet Labs Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing permissions
and limitations under the License.
*/

package kubernetes

import (
	"context"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.uber.org/zap"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
)

const (
	// DefaultDrainBuffer is the default minimum time between node drains.
	DefaultDrainBuffer = 10 * time.Minute

	eventReasonCordonStarting  = "CordonStarting"
	eventReasonCordonSucceeded = "CordonSucceeded"
	eventReasonCordonFailed    = "CordonFailed"

	eventReasonDrainScheduled        = "DrainScheduled"
	eventReasonDrainSchedulingFailed = "DrainSchedulingFailed"
	eventReasonDrainStarting         = "DrainStarting"
	eventReasonDrainSucceeded        = "DrainSucceeded"
	eventReasonDrainFailed           = "DrainFailed"

	tagResultSucceeded = "succeeded"
	tagResultFailed    = "failed"

	drainRetryAnnotationKey   = "draino/drain-retry"
	drainRetryAnnotationValue = "true"
)

// Opencensus measurements.
var (
	MeasureNodesCordoned       = stats.Int64("draino/nodes_cordoned", "Number of nodes cordoned.", stats.UnitDimensionless)
	MeasureNodesDrained        = stats.Int64("draino/nodes_drained", "Number of nodes drained.", stats.UnitDimensionless)
	MeasureNodesDrainScheduled = stats.Int64("draino/nodes_drainScheduled", "Number of nodes drain scheduled.", stats.UnitDimensionless)

	TagNodeName, _ = tag.NewKey("node_name")
	TagResult, _   = tag.NewKey("result")
)

// A DrainingResourceEventHandler cordons and drains any added or updated nodes.
type DrainingResourceEventHandler struct {
	logger         *zap.Logger
	cordonDrainer  CordonDrainer
	eventRecorder  record.EventRecorder
	drainScheduler DrainScheduler

	lastDrainScheduledFor time.Time
	buffer                time.Duration
}

// DrainingResourceEventHandlerOption configures an DrainingResourceEventHandler.
type DrainingResourceEventHandlerOption func(d *DrainingResourceEventHandler)

// WithLogger configures a DrainingResourceEventHandler to use the supplied
// logger.
func WithLogger(l *zap.Logger) DrainingResourceEventHandlerOption {
	return func(h *DrainingResourceEventHandler) {
		h.logger = l
	}
}

// WithDrainBuffer configures the minimum time between scheduled drains.
func WithDrainBuffer(d time.Duration) DrainingResourceEventHandlerOption {
	return func(h *DrainingResourceEventHandler) {
		h.buffer = d
	}
}

// NewDrainingResourceEventHandler returns a new DrainingResourceEventHandler.
func NewDrainingResourceEventHandler(d CordonDrainer, e record.EventRecorder, ho ...DrainingResourceEventHandlerOption) *DrainingResourceEventHandler {
	h := &DrainingResourceEventHandler{
		logger:                zap.NewNop(),
		cordonDrainer:         d,
		eventRecorder:         e,
		lastDrainScheduledFor: time.Now(),
		buffer:                DefaultDrainBuffer,
	}
	for _, o := range ho {
		o(h)
	}
	h.drainScheduler = NewDrainSchedules(d, e, h.buffer, h.logger)
	return h
}

// OnAdd cordons and drains the added node.
func (h *DrainingResourceEventHandler) OnAdd(obj interface{}) {
	n, ok := obj.(*core.Node)
	if !ok {
		return
	}
	h.HandleNode(n)
}

// OnUpdate cordons and drains the updated node.
func (h *DrainingResourceEventHandler) OnUpdate(_, newObj interface{}) {
	h.OnAdd(newObj)
}

// OnDelete does nothing. There's no point cordoning or draining deleted nodes.

func (h *DrainingResourceEventHandler) OnDelete(obj interface{}) {
	n, ok := obj.(*core.Node)
	if !ok {
		d, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		h.drainScheduler.DeleteSchedule(d.Key)
	}

	h.drainScheduler.DeleteSchedule(n.GetName())
}

func (h *DrainingResourceEventHandler) HandleNode(n *core.Node) {
	// First cordon the node if it is not yet cordonned
	if !n.Spec.Unschedulable {
		h.cordon(n)
		return
	}

	// Let's ensure that a drain is scheduled
	hasSChedule, failedDrain := h.drainScheduler.HasSchedule(n.GetName())
	if !hasSChedule {
		h.scheduleDrain(n)
		return
	}

	// Is there a request to retry a failed drain activity. If yes reschedule drain
	if failedDrain && HasDrainRetryAnnotation(n) {
		h.drainScheduler.DeleteSchedule(n.GetName())
		h.scheduleDrain(n)
		return
	}
}

// TODO(negz): Ideally we'd record which node condition caused us to cordon
// and drain the node, but that information doesn't make it down to this level.
func (h *DrainingResourceEventHandler) cordon(n *core.Node) {
	log := h.logger.With(zap.String("node", n.GetName()))
	tags, _ := tag.New(context.Background(), tag.Upsert(TagNodeName, n.GetName())) // nolint:gosec
	// Events must be associated with this object reference, rather than the
	// node itself, in order to appear under `kubectl describe node` due to the
	// way that command is implemented.
	// https://github.com/kubernetes/kubernetes/blob/17740a2/pkg/printers/internalversion/describe.go#L2711
	nr := &core.ObjectReference{Kind: "Node", Name: n.GetName(), UID: types.UID(n.GetName())}

	log.Debug("Cordoning")
	h.eventRecorder.Event(nr, core.EventTypeWarning, eventReasonCordonStarting, "Cordoning node")
	if err := h.cordonDrainer.Cordon(n); err != nil {
		log.Info("Failed to cordon", zap.Error(err))
		tags, _ = tag.New(tags, tag.Upsert(TagResult, tagResultFailed)) // nolint:gosec
		stats.Record(tags, MeasureNodesCordoned.M(1))
		h.eventRecorder.Eventf(nr, core.EventTypeWarning, eventReasonCordonFailed, "Cordoning failed: %v", err)
		return
	}
	log.Info("Cordoned")
	tags, _ = tag.New(tags, tag.Upsert(TagResult, tagResultSucceeded)) // nolint:gosec
	stats.Record(tags, MeasureNodesCordoned.M(1))
	h.eventRecorder.Event(nr, core.EventTypeWarning, eventReasonCordonSucceeded, "Cordoned node")
}

// drain schedule the draining activity
func (h *DrainingResourceEventHandler) scheduleDrain(n *core.Node) {
	log := h.logger.With(zap.String("node", n.GetName()))
	tags, _ := tag.New(context.Background(), tag.Upsert(TagNodeName, n.GetName())) // nolint:gosec
	nr := &core.ObjectReference{Kind: "Node", Name: n.GetName(), UID: types.UID(n.GetName())}
	log.Debug("Scheduling drain")
	when, err := h.drainScheduler.Schedule(n)
	if err != nil {
		if IsAlreadyScheduledError(err) {
			return
		}
		log.Info("Failed to schedule the drain activity", zap.Error(err))
		tags, _ = tag.New(tags, tag.Upsert(TagResult, tagResultFailed)) // nolint:gosec
		stats.Record(tags, MeasureNodesDrainScheduled.M(1))
		h.eventRecorder.Eventf(nr, core.EventTypeWarning, eventReasonDrainSchedulingFailed, "Drain scheduling failed: %v", err)
		return
	}
	log.Info("Drain scheduled ", zap.Time("after", when))
	tags, _ = tag.New(tags, tag.Upsert(TagResult, tagResultSucceeded)) // nolint:gosec
	stats.Record(tags, MeasureNodesDrainScheduled.M(1))
	h.eventRecorder.Eventf(nr, core.EventTypeWarning, eventReasonDrainScheduled, "Will drain node after %s", when.Format(time.RFC3339Nano))
}

func HasDrainRetryAnnotation(n *core.Node) bool {
	return n.GetAnnotations()[drainRetryAnnotationKey] == drainRetryAnnotationValue
}
