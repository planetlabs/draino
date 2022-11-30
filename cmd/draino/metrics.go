package main

import (
	"contrib.go.opencensus.io/exporter/prometheus"
	"github.com/planetlabs/draino/internal/kubernetes"
	prom "github.com/prometheus/client_golang/prometheus"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"go.uber.org/zap"
	"gopkg.in/alecthomas/kingpin.v2"
	"net/http"
)

func DrainoLegacyMetrics(options *Options, logger *zap.Logger) {

	var (
		nodesCordoned = &view.View{
			Name:        "cordoned_nodes_total",
			Measure:     kubernetes.MeasureNodesCordoned,
			Description: "Number of nodes cordoned.",
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{kubernetes.TagResult, kubernetes.TagConditions, kubernetes.TagNodegroupName, kubernetes.TagNodegroupNamePrefix, kubernetes.TagNodegroupNamespace, kubernetes.TagTeam},
		}
		nodesUncordoned = &view.View{
			Name:        "uncordoned_nodes_total",
			Measure:     kubernetes.MeasureNodesUncordoned,
			Description: "Number of nodes uncordoned.",
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{kubernetes.TagResult},
		}
		nodesDrained = &view.View{
			Name:        "drained_nodes_total",
			Measure:     kubernetes.MeasureNodesDrained,
			Description: "Number of nodes drained.",
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{kubernetes.TagResult, kubernetes.TagFailureCause, kubernetes.TagConditions, kubernetes.TagNodegroupName, kubernetes.TagNodegroupNamePrefix, kubernetes.TagNodegroupNamespace, kubernetes.TagTeam},
		}
		nodesDrainScheduled = &view.View{
			Name:        "drain_scheduled_nodes_total",
			Measure:     kubernetes.MeasureNodesDrainScheduled,
			Description: "Number of nodes scheduled for drain.",
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{kubernetes.TagResult, kubernetes.TagConditions, kubernetes.TagNodegroupName, kubernetes.TagNodegroupNamePrefix, kubernetes.TagNodegroupNamespace, kubernetes.TagTeam},
		}
		limitedCordon = &view.View{
			Name:        "limited_cordon_total",
			Measure:     kubernetes.MeasureLimitedCordon,
			Description: "Number of limited cordon encountered.",
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{kubernetes.TagReason, kubernetes.TagConditions, kubernetes.TagNodegroupName, kubernetes.TagNodegroupNamePrefix, kubernetes.TagNodegroupNamespace, kubernetes.TagTeam},
		}
		skippedCordon = &view.View{
			Name:        "skipped_cordon_total",
			Measure:     kubernetes.MeasureSkippedCordon,
			Description: "Number of skipped cordon encountered.",
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{kubernetes.TagReason, kubernetes.TagConditions, kubernetes.TagNodegroupName, kubernetes.TagNodegroupNamePrefix, kubernetes.TagNodegroupNamespace, kubernetes.TagTeam},
		}
		nodesReplacement = &view.View{
			Name:        "node_replacement_request_total",
			Measure:     kubernetes.MeasureNodesReplacementRequest,
			Description: "Number of nodes replacement requested.",
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{kubernetes.TagResult, kubernetes.TagReason, kubernetes.TagNodegroupName, kubernetes.TagNodegroupNamePrefix, kubernetes.TagNodegroupNamespace, kubernetes.TagTeam},
		}
		nodesPreprovisioningLatency = &view.View{
			Name:        "node_preprovisioning_latency",
			Measure:     kubernetes.MeasurePreprovisioningLatency,
			Description: "Latency to get preprovisioned node",
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{kubernetes.TagResult, kubernetes.TagReason, kubernetes.TagNodegroupName, kubernetes.TagNodegroupNamePrefix, kubernetes.TagNodegroupNamespace, kubernetes.TagTeam},
		}
	)

	kingpin.FatalIfError(view.Register(nodesCordoned, nodesUncordoned, nodesDrained, nodesDrainScheduled, limitedCordon, skippedCordon, nodesReplacement, nodesPreprovisioningLatency), "cannot create metrics")

	promOptions := prometheus.Options{Namespace: kubernetes.Component, Registry: prom.NewRegistry()}
	kubernetes.InitWorkqueueMetrics(promOptions.Registry)
	p, err := prometheus.NewExporter(promOptions)
	kingpin.FatalIfError(err, "cannot export metrics")
	view.RegisterExporter(p)

	web := &httpRunner{address: options.listen, logger: logger, h: map[string]http.Handler{
		"/metrics": p,
		"/healthz": http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { r.Body.Close() }), // nolint:errcheck // no err management in health check
	}}

	go func() {
		logger.Info("web server is running", zap.String("listen", options.listen))
		kingpin.FatalIfError(kubernetes.Await(web), "error serving")
	}()

}
