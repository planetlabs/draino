package observability

import (
	"sync"
	"time"

	gmetrics "github.com/DataDog/compute-go/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	RetryWallSubsystem = "retry_wall"
	RunnerSubsystem    = "group_runner"

	NewTagNodeName = "node_name"
	NewTagGroupKey = "group_key"

	NewTagRunnerName = "runner_name"
)

var (
	registerMetricsOnce sync.Once

	// Retry Wall Subsystem
	nodeRetriesTags = []string{NewTagNodeName, NewTagGroupKey}
	nodeRetries     = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: RetryWallSubsystem,
		Name:      "node_retries",
		Help:      "Number of retries for each node",
	}, nodeRetriesTags)
	nodeRetriesCleaner gmetrics.GaugeCleaner

	// Runner Subsystem
	groupRunnerLoopDurationTags = []string{NewTagGroupKey, NewTagRunnerName}
	groupRunnerLoopDuration     = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: RunnerSubsystem,
		Name:      "loop_duration",
		Help:      "Loop duration in microseconds",
	}, groupRunnerLoopDurationTags)
	groupRunnerLoopDurationCleaner gmetrics.GaugeCleaner
)

func initGaugeCleaner(cleanupPeriod time.Duration) {
	// Retry Wall Subsystem
	nodeRetriesCleaner = gmetrics.NewGaugeCleaner(nodeRetries, nodeRetriesTags, cleanupPeriod)

	// Runner Subsystem
	groupRunnerLoopDurationCleaner = gmetrics.NewGaugeCleaner(groupRunnerLoopDuration, groupRunnerLoopDurationTags, cleanupPeriod)
}

func RegisterNewMetrics(registry *prometheus.Registry, cleanupPeriod time.Duration) {
	registerMetricsOnce.Do(func() {
		initGaugeCleaner(cleanupPeriod)

		// Retry Wall Subsystem
		registry.MustRegister(nodeRetries)

		//Runner Subsystem
		registry.MustRegister(groupRunnerLoopDuration)
	})
}
