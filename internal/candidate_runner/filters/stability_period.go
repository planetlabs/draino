package filters

import (
	"context"

	"github.com/planetlabs/draino/internal/kubernetes/analyser"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/clock"
)

func NewStabilityPeriodFilter(checker analyser.StabilityPeriodChecker, clock clock.Clock) Filter {
	return FilterFromFunction(
		"stability_period",
		func(ctx context.Context, n *corev1.Node) bool {
			span, ctx := tracer.StartSpanFromContext(ctx, "StabilityPeriodFilter")
			defer span.Finish()
			return checker.StabilityPeriodAcceptsDrain(context.Background(), n, clock.Now())
		},
	)
}
