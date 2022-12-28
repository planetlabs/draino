package filters

import (
	"context"

	"github.com/planetlabs/draino/internal/kubernetes/analyser"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/clock"
)

func NewStabilityPeriodFilter(checker analyser.StabilityPeriodChecker, clock clock.Clock) Filter {
	return FilterFromFunction(
		"stability_period",
		func(n *corev1.Node) bool {
			return checker.StabilityPeriodAcceptsDrain(context.Background(), n, clock.Now())
		},
	)
}
