package k8sclient

import (
	"github.com/stretchr/testify/assert"
	"testing"

	"k8s.io/client-go/util/flowcontrol"
)

func TestRateLimitWithMetric(t *testing.T) {
	rateLimiter := flowcontrol.NewTokenBucketRateLimiter(1, 10)
	subject := NewRateLimiterWithMetric("test", rateLimiter)
	accepted := subject.TryAccept()
	assert.Equal(t, true, accepted)
	tokens := subject.tokens()
	assert.Equal(t, float64(9), tokens)
}
