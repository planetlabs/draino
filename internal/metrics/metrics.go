package metrics

import (
	"strconv"
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

	drainoRunning = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: GlobalSubsystem,
		Name:      "draino_running",
		Help:      "Indicates that draino is running.",
	}, []string{TagComponentName, TagDryRun})
)

func RegisterMetrics(registry *prometheus.Registry) {
	registerOnce.Do(func() {
		// Global Subsystem
		registry.MustRegister(drainoInternalError)
		registry.MustRegister(drainoRunning)
	})
}

func IncInternalError(component, reason, group, node string) {
	drainoInternalError.WithLabelValues(component, reason, group, node).Inc()
}

func DrainoRunning(component string, dryrun bool) {
	drainoRunning.WithLabelValues(component, strconv.FormatBool(dryrun)).Set(1)
}
