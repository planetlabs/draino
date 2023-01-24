package limit

import (
	"context"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/utils/clock"
)

type RateLimiter interface {
	// TryAccept returns true if a token is taken immediately. Otherwise,
	// it returns false.
	TryAccept() bool
}

type rateLimiterImpl struct {
	limiter flowcontrol.RateLimiter
}

func NewRateLimiter(clock clock.Clock, qps float32, burst int) RateLimiter {
	return &rateLimiterImpl{
		limiter: flowcontrol.NewTokenBucketRateLimiterWithClock(qps, burst, clock),
	}
}

func (limiter *rateLimiterImpl) TryAccept() bool {
	return limiter.limiter.TryAccept()
}

// TypedRateLimiter is a rate limiter that implements different rate limiters based on the given type t.
type TypedRateLimiter interface {
	// TryAccept takes a type t and returns true if a token is taken immediately.
	// Otherwise, it returns false.
	TryAccept(t string) bool
	// Wait takes a type t and returns nil if a token is taken before the Context is done.
	Wait(ctx context.Context, t string) error
}

type RateLimiterConfiguration struct {
	QPS   *float32
	Burst *int
}

// typedRateLimiterImpl is a wrapper to abstract the flowcontrol rate limiter to the other interal parts of the code
type typedRateLimiterImpl struct {
	clock        clock.Clock
	defaultQPS   float32
	defaultBurst int
	rlConfigs    map[string]RateLimiterConfiguration
	rateLimiters map[string]flowcontrol.RateLimiter
}

func NewTypedRateLimiter(clock clock.Clock, configurations map[string]RateLimiterConfiguration, defaultQPS float32, defaultBurst int) TypedRateLimiter {
	return &typedRateLimiterImpl{
		defaultQPS:   defaultQPS,
		defaultBurst: defaultBurst,
		rlConfigs:    configurations,
		clock:        clock,
		rateLimiters: map[string]flowcontrol.RateLimiter{},
	}
}

func (limit *typedRateLimiterImpl) Wait(ctx context.Context, t string) error {
	rateLimiter := limit.getRateLimiter(t)
	return rateLimiter.Wait(ctx)
}

func (limit *typedRateLimiterImpl) TryAccept(t string) bool {
	rateLimiter := limit.getRateLimiter(t)
	return rateLimiter.TryAccept()
}

func (limit *typedRateLimiterImpl) getRateLimiter(t string) flowcontrol.RateLimiter {
	if _, exist := limit.rateLimiters[t]; !exist {
		cfg, ok := limit.rlConfigs[t]
		if !ok {
			cfg.QPS = &limit.defaultQPS
			cfg.Burst = &limit.defaultBurst
		}
		if cfg.QPS == nil {
			cfg.QPS = &limit.defaultQPS
		}
		if cfg.Burst == nil {
			cfg.Burst = &limit.defaultBurst
		}
		limit.rateLimiters[t] = flowcontrol.NewTokenBucketRateLimiterWithClock(*cfg.QPS, *cfg.Burst, limit.clock)
	}
	return limit.rateLimiters[t]
}
