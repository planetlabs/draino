package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	registerOnce sync.Once

	drainoInternalError = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: GlobalSubsystem,
		Name:      "draino_internal_error",
		Help:      "Number of internal errors accross different draino components.",
	}, []string{TagComponentName, TagReason, TagGroupKey, TagNodeName})
)

func RegisterMetrics(registry *prometheus.Registry) {
	registerOnce.Do(func() {
		// Global Subsystem
		registry.MustRegister(drainoInternalError)
	})
}

func IncInternalError(component, reason, group, node string) {
	drainoInternalError.WithLabelValues(component, reason, group, node).Inc()
}
