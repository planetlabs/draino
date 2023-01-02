package observability

import (
	"sync"
	"time"

	gmetrics "github.com/DataDog/compute-go/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	RetryWallSubsystem = "retry_wall"
	NewTagNodeName     = "node_name"
	NewTagGroupKey     = "group_key"
)

var (
	registerMetricsOnce sync.Once

	nodeRetriesTags = []string{NewTagNodeName, NewTagGroupKey}
	nodeRetries     = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: RetryWallSubsystem,
		Name:      "node_retries",
		Help:      "Number of retries for each node",
	}, nodeRetriesTags)
	nodeRetriesCleaner gmetrics.GaugeCleaner
)

func initGaugeCleaner(cleanupPeriod time.Duration) {
	nodeRetriesCleaner = gmetrics.NewGaugeCleaner(nodeRetries, nodeRetriesTags, cleanupPeriod)
}

func RegisterNewMetrics(registry *prometheus.Registry, cleanupPeriod time.Duration) {
	registerMetricsOnce.Do(func() {
		initGaugeCleaner(cleanupPeriod)
		registry.MustRegister(nodeRetries)
	})
}
