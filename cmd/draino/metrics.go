package main

import (
	"context"
	"net/http"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/planetlabs/draino/internal/drain_runner"
	"github.com/planetlabs/draino/internal/metrics"

	"contrib.go.opencensus.io/exporter/prometheus"
	prom "github.com/prometheus/client_golang/prometheus"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/planetlabs/draino/internal/groups"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/observability"
)

func DrainoLegacyMetrics(ctx context.Context, options *Options, logger logr.Logger) manager.Runnable {
	var (
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

	if options.noLegacyNodeHandler {
		// removing: nodesDrained
		kingpin.FatalIfError(view.Register(nodesDrainScheduled, nodesReplacement, nodesPreprovisioningLatency), "cannot create metrics")
	} else {
		kingpin.FatalIfError(view.Register(nodesDrained, nodesDrainScheduled, nodesReplacement, nodesPreprovisioningLatency), "cannot create metrics")
	}

	promOptions := prometheus.Options{Namespace: kubernetes.Component, Registry: prom.NewRegistry()}
	kubernetes.InitWorkqueueMetrics(promOptions.Registry)
	p, err := prometheus.NewExporter(promOptions)
	kingpin.FatalIfError(err, "cannot export metrics")
	view.RegisterExporter(p)

	web := &HttpRunner{address: options.listen, logger: logger, h: map[string]http.Handler{
		"/metrics": p,
		"/healthz": http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { r.Body.Close() }), // nolint:errcheck // no err management in health check
	}}

	groups.RegisterMetrics(promOptions.Registry)
	observability.RegisterNewMetrics(promOptions.Registry, options.scopeAnalysisPeriod)
	metrics.RegisterMetrics(promOptions.Registry)

	if options.noLegacyNodeHandler {
		DrainoMetrics(promOptions.Registry)
	}

	return web
}

func DrainoMetrics(promExporter prom.Registerer) {
	drain_runner.RegisterMetrics(promExporter)
}
