package circuitbreaker

import (
	"sync"

	"github.com/DataDog/compute-go/ddclient/monitor"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	circuitBreaker = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "circuit_breaker",
		Name:      "state",
		Help:      "current state of the circuit breaker",
	}, []string{"name"})
	circuitBreakerGroupCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "circuit_breaker",
		Name:      "monitor_group_count",
		Help:      "number of monitor group evaluated",
	}, []string{"name"})
)

const (
	openGaugeValue     = 1.0
	halfOpenGaugeValue = 0.5
	closedGaugeValue   = 0.
)

var registerOnlyOnce sync.Once

func RegisterMetrics(registry prometheus.Registerer) {
	registerOnlyOnce.Do(func() {
		registry.MustRegister(circuitBreaker)
		registry.MustRegister(circuitBreakerGroupCount)
		monitor.RegisterMetrics(registry)
	})
}

func generateStateMetric(cb NamedCircuitBreaker) {
	g := circuitBreaker.WithLabelValues(cb.Name())
	switch cb.State() {
	case Open:
		g.Set(openGaugeValue)
	case HalfOpen:
		g.Set(halfOpenGaugeValue)
	case Closed:
		g.Set(closedGaugeValue)
	}
}
