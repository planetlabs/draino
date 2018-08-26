package kubernetes

import (
	"context"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.uber.org/zap"
	core "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
)

const (
	eventReasonCordonStarting  = "CordonStarting"
	eventReasonCordonSucceeded = "CordonSucceeded"
	eventReasonCordonFailed    = "CordonFailed"

	eventReasonDrainScheduled = "DrainScheduled"
	eventReasonDrainStarting  = "DrainStarting"
	eventReasonDrainSucceeded = "DrainSucceeded"
	eventReasonDrainFailed    = "DrainFailed"

	tagResultSucceeded = "succeeded"
	tagResultFailed    = "failed"
)

// Opencensus measurements.
var (
	MeasureNodesCordoned = stats.Int64("draino/nodes_cordoned", "Number of nodes cordoned.", stats.UnitDimensionless)
	MeasureNodesDrained  = stats.Int64("draino/nodes_drained", "Number of nodes drained.", stats.UnitDimensionless)

	TagNodeName, _ = tag.NewKey("node_name")
	TagResult, _   = tag.NewKey("result")
)

// A DrainingResourceEventHandler cordons and drains any added or updated nodes.
type DrainingResourceEventHandler struct {
	l *zap.Logger
	d CordonDrainer
	e record.EventRecorder

	lastDrainStarted time.Time
	bufferTime       time.Duration
}

// DrainingResourceEventHandlerOption configures an DrainingResourceEventHandler.
type DrainingResourceEventHandlerOption func(d *DrainingResourceEventHandler)

// WithLogger configures a DrainingResourceEventHandler to use the supplied
// logger.
func WithLogger(l *zap.Logger) DrainingResourceEventHandlerOption {
	return func(d *DrainingResourceEventHandler) {
		d.l = l
	}
}

// NewDrainingResourceEventHandler returns a new DrainingResourceEventHandler.
func NewDrainingResourceEventHandler(d CordonDrainer, e record.EventRecorder, ho ...DrainingResourceEventHandlerOption) *DrainingResourceEventHandler {
	h := &DrainingResourceEventHandler{l: zap.NewNop(), d: d, e: e}
	for _, o := range ho {
		o(h)
	}
	return h
}

// OnAdd cordons and drains the added node.
func (h *DrainingResourceEventHandler) OnAdd(obj interface{}) {
	n, ok := obj.(*core.Node)
	if !ok {
		return
	}
	h.cordonAndDrain(n)
}

// OnUpdate cordons and drains the updated node.
func (h *DrainingResourceEventHandler) OnUpdate(_, newObj interface{}) {
	h.OnAdd(newObj)
}

// OnDelete does nothing. There's no point cordoning or draining deleted nodes.
func (h *DrainingResourceEventHandler) OnDelete(_ interface{}) {
	return
}

// TODO(negz): Ideally we'd record which node condition caused us to cordon
// and drain the node, but that information doesn't make it down to this level.
func (h *DrainingResourceEventHandler) cordonAndDrain(n *core.Node) {
	log := h.l.With(zap.String("node", n.GetName()))
	tags, _ := tag.New(context.Background(), tag.Upsert(TagNodeName, n.GetName())) // nolint:gosec

	log.Debug("Cordoning")
	h.e.Event(n, core.EventTypeWarning, eventReasonCordonStarting, "Cordoning node")
	if err := h.d.Cordon(n); err != nil {
		log.Info("Failed to cordon", zap.Error(err))
		tags, _ = tag.New(tags, tag.Upsert(TagResult, tagResultFailed)) // nolint:gosec
		stats.Record(tags, MeasureNodesCordoned.M(1))
		h.e.Eventf(n, core.EventTypeWarning, eventReasonCordonFailed, "Cordoning failed: %v", err)
		return
	}
	log.Info("Cordoned")
	tags, _ = tag.New(tags, tag.Upsert(TagResult, tagResultSucceeded)) // nolint:gosec
	stats.Record(tags, MeasureNodesCordoned.M(1))
	h.e.Event(n, core.EventTypeWarning, eventReasonCordonSucceeded, "Cordoned node")

	t := time.Now()
	d := h.lastDrainStarted.Sub(t) + h.bufferTime
	a := t.Add(d)

	log.Info("Scheduled drain", zap.Time("after", a))
	h.e.Eventf(n, core.EventTypeWarning, eventReasonDrainScheduled, "Will drain node after %s", a)
	time.AfterFunc(d, func() {
		h.lastDrainStarted = time.Now()
		log.Debug("Draining")
		h.e.Event(n, core.EventTypeWarning, eventReasonDrainStarting, "Draining node")
		if err := h.d.Drain(n); err != nil {
			log.Info("Failed to drain", zap.Error(err))
			tags, _ = tag.New(tags, tag.Upsert(TagResult, tagResultFailed)) // nolint:gosec
			stats.Record(tags, MeasureNodesDrained.M(1))
			h.e.Eventf(n, core.EventTypeWarning, eventReasonDrainFailed, "Cordoning failed: %v", err)
			return
		}
		log.Info("Drained")
		tags, _ = tag.New(tags, tag.Upsert(TagResult, tagResultSucceeded)) // nolint:gosec
		stats.Record(tags, MeasureNodesDrained.M(1))
		h.e.Event(n, core.EventTypeWarning, eventReasonDrainSucceeded, "Drained node")
	})
}
