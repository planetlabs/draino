package filters

import (
	"github.com/planetlabs/draino/internal/kubernetes/drain"
	v1 "k8s.io/api/core/v1"
	"k8s.io/utils/clock"
)

func NewRetryWallFilter(clock clock.Clock, retryWall drain.RetryWall) Filter {
	return FilterFromFunction("retry",
		func(n *v1.Node) bool {
			return retryWall.GetRetryWallTimestamp(n).Before(clock.Now())
		},
	)
}
