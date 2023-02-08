package observability

import (
	"sync"
	"time"

	gmetrics "github.com/DataDog/compute-go/metrics"
	"github.com/planetlabs/draino/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	registerMetricsOnce sync.Once

	// Retry Wall Subsystem
	nodeRetriesTags = []string{metrics.TagNodeName, metrics.TagGroupKey}
	nodeRetries     = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: metrics.RetryWallSubsystem,
		Name:      "node_retries",
		Help:      "Number of retries for each node",
	}, nodeRetriesTags)
	nodeRetriesCleaner gmetrics.GaugeCleaner

	// Runner Subsystem
	groupRunnerLoopDurationTags = []string{metrics.TagGroupKey, metrics.TagRunnerName}
	groupRunnerLoopDuration     = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: metrics.RunnerSubsystem,
		Name:      "loop_duration",
		Help:      "Loop duration in microseconds",
	}, groupRunnerLoopDurationTags)
	groupRunnerLoopDurationCleaner gmetrics.GaugeCleaner

	// Candidate Runner Subsystem
	candidateRunnerTags       = []string{metrics.TagGroupKey}
	candidateRunnerTotalNodes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: metrics.CandidateRunnerSubsystem,
		Name:      "total_nodes",
		Help:      "Total amount of nodes in this group",
	}, candidateRunnerTags)
	candidateRunnerTotalNodesCleaner gmetrics.GaugeCleaner

	candidateRunnerFilteredOutNodes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: metrics.CandidateRunnerSubsystem,
		Name:      "filtered_out_nodes",
		Help:      "Amount of nodes that were filtered out",
	}, candidateRunnerTags)
	candidateRunnerFilteredOutNodesCleaner gmetrics.GaugeCleaner

	candidateRunnerTotalSlots = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: metrics.CandidateRunnerSubsystem,
		Name:      "total_slots",
		Help:      "Total amount of available drain candidate slots",
	}, candidateRunnerTags)
	candidateRunnerTotalSlotsCleaner gmetrics.GaugeCleaner

	candidateRunnerRemainingSlots = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: metrics.CandidateRunnerSubsystem,
		Name:      "remaining_slots",
		Help:      "Current remaining drain candidate slots",
	}, candidateRunnerTags)
	candidateRunnerRemainingSlotsCleaner gmetrics.GaugeCleaner

	candidateRunnerSimulationRejections = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: metrics.CandidateRunnerSubsystem,
		Name:      "simulation_rejections",
		Help:      "Amount of nodes that faild the drain simulation",
	}, candidateRunnerTags)
	candidateRunnerSimulationRejectionsCleaner gmetrics.GaugeCleaner

	candidateRunnerConditionRateLimited = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: metrics.CandidateRunnerSubsystem,
		Name:      "condition_rate_limited",
		Help:      "Amount of nodes that cannot be drained because of missing rate limiting budget on conditions",
	}, candidateRunnerTags)
	candidateRunnerConditionRateLimitedCleaner gmetrics.GaugeCleaner

	candidateRunnerRunRateLimited = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: metrics.CandidateRunnerSubsystem,
		Name:      "run_rate_limited",
		Help:      "Indicates if the run was stopped because of client side rate limiting. 1 = Yes",
	}, candidateRunnerTags)
	candidateRunnerRunRateLimitedCleaner gmetrics.GaugeCleaner
)

func initGaugeCleaner(cleanupPeriod time.Duration) {
	// Retry Wall Subsystem
	nodeRetriesCleaner = gmetrics.NewGaugeCleaner(nodeRetries, nodeRetriesTags, cleanupPeriod)

	// Runner Subsystem
	groupRunnerLoopDurationCleaner = gmetrics.NewGaugeCleaner(groupRunnerLoopDuration, groupRunnerLoopDurationTags, cleanupPeriod)

	// Candidate Runner Subsystem
	candidateRunnerTotalNodesCleaner = gmetrics.NewGaugeCleaner(candidateRunnerTotalNodes, candidateRunnerTags, cleanupPeriod)
	candidateRunnerFilteredOutNodesCleaner = gmetrics.NewGaugeCleaner(candidateRunnerFilteredOutNodes, candidateRunnerTags, cleanupPeriod)
	candidateRunnerTotalSlotsCleaner = gmetrics.NewGaugeCleaner(candidateRunnerTotalSlots, candidateRunnerTags, cleanupPeriod)
	candidateRunnerRemainingSlotsCleaner = gmetrics.NewGaugeCleaner(candidateRunnerRemainingSlots, candidateRunnerTags, cleanupPeriod)
	candidateRunnerSimulationRejectionsCleaner = gmetrics.NewGaugeCleaner(candidateRunnerSimulationRejections, candidateRunnerTags, cleanupPeriod)
	candidateRunnerConditionRateLimitedCleaner = gmetrics.NewGaugeCleaner(candidateRunnerConditionRateLimited, candidateRunnerTags, cleanupPeriod)
	candidateRunnerRunRateLimitedCleaner = gmetrics.NewGaugeCleaner(candidateRunnerRunRateLimited, candidateRunnerTags, cleanupPeriod)
}

func RegisterNewMetrics(registry *prometheus.Registry, cleanupPeriod time.Duration) {
	registerMetricsOnce.Do(func() {
		initGaugeCleaner(cleanupPeriod)

		// Retry Wall Subsystem
		registry.MustRegister(nodeRetries)

		//Runner Subsystem
		registry.MustRegister(groupRunnerLoopDuration)

		// Candidate Runner Subsystem
		registry.MustRegister(candidateRunnerTotalNodes, candidateRunnerFilteredOutNodes, candidateRunnerTotalSlots, candidateRunnerRemainingSlots, candidateRunnerSimulationRejections, candidateRunnerConditionRateLimited)
	})
}
