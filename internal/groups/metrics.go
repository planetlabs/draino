package groups

import (
	gmetrics "github.com/DataDog/compute-go/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"sync"
	"time"
)

const (
	publicationPeriod = 20 * time.Second
)

var (
	MetricsActiveRunnerTags gmetrics.Tags
	MetricsActiveRunner     gmetrics.GaugeCleaner

	registerOnceGroupsMetrics sync.Once
)

// RegisterMetrics registers metrics for the scheduling groups runners
func RegisterMetrics(registry *prometheus.Registry) {
	registerOnceGroupsMetrics.Do(func() {
		MetricsActiveRunnerTags = gmetrics.NewTags() // TODO we can add tags based on the content of RunnerInfo later. Not doing a tag for the group key
		MetricsActiveRunnerGaugeVec := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "active_group_runners", Help: "active_group_runners count all the active runners"}, MetricsActiveRunnerTags)
		MetricsActiveRunner = gmetrics.NewGaugeCleaner(MetricsActiveRunnerGaugeVec, MetricsActiveRunnerTags, 4*publicationPeriod)
	})
}
