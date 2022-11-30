package k8sclient

import (
	"context"
	"fmt"
	"github.com/DataDog/compute-go/kubeclient"
	"reflect"
	"sync"
	"unsafe"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"golang.org/x/time/rate"
	"k8s.io/client-go/util/flowcontrol"
)

var (
	tagClientName                   tag.Key
	k8sClientTokensRemainingMeasure *stats.Float64Measure
)

func init() {
	var err error
	tagClientName, err = tag.NewKey("client_name")
	if err != nil {
		panic(err)
	}
	k8sClientTokensMetricName := "k8s_client_tokens_remaining"
	description := "Number of remaining tokens in the k8s client rate limiter"
	k8sClientTokensRemainingMeasure = stats.Float64(k8sClientTokensMetricName, description, stats.UnitDimensionless)
	v := &view.View{
		Name:        k8sClientTokensMetricName,
		Measure:     k8sClientTokensRemainingMeasure,
		Description: description,
		Aggregation: view.LastValue(),
		TagKeys:     []tag.Key{tagClientName},
	}
	view.Register(v)
}

func DecorateWithRateLimiter(config *kubeclient.Config, name string) error {
	if config.QPS == 0 {
		return fmt.Errorf("rest.Config qps must be set")
	}
	if config.Burst == 0 {
		return fmt.Errorf("rest.Config burst must be set")
	}
	rateLimiter := flowcontrol.NewTokenBucketRateLimiter(config.QPS, config.Burst)
	config.CustomRateLimiter = NewRateLimiterWithMetric(name, rateLimiter)
	return nil
}

type mutexRateLimiter struct {
	rateLimiter flowcontrol.RateLimiter
	sync.Mutex
}

type rateLimiterWithMetric struct {
	name        string
	rateLimiter mutexRateLimiter
	limiter     *rate.Limiter
}

var _ flowcontrol.RateLimiter = &rateLimiterWithMetric{}

func NewRateLimiterWithMetric(name string, rateLimiter flowcontrol.RateLimiter) *rateLimiterWithMetric {
	// reflection hacks from https://stackoverflow.com/questions/42664837/how-to-access-unexported-struct-fields/43918797#43918797
	limiterField := reflect.Indirect(reflect.ValueOf(rateLimiter)).FieldByName("limiter")
	limiterField = reflect.NewAt(limiterField.Type(), unsafe.Pointer(limiterField.UnsafeAddr())).Elem()
	limiter := limiterField.Interface().(*rate.Limiter)
	return &rateLimiterWithMetric{
		name: name,
		rateLimiter: mutexRateLimiter{
			rateLimiter: rateLimiter,
		},
		limiter: limiter,
	}
}

func (r *rateLimiterWithMetric) Wait(ctx context.Context) error {
	return r.limiter.Wait(ctx)
}

func (r *rateLimiterWithMetric) TryAccept() bool {
	r.rateLimiter.Lock()
	defer r.rateLimiter.Unlock()
	defer r.updateMetric()
	return r.rateLimiter.rateLimiter.TryAccept()
}

func (r *rateLimiterWithMetric) Accept() {
	r.rateLimiter.Lock()
	defer r.rateLimiter.Unlock()
	defer r.updateMetric()
	r.rateLimiter.rateLimiter.Accept()
}

func (r *rateLimiterWithMetric) Stop() {
	r.rateLimiter.Lock()
	defer r.rateLimiter.Unlock()
	defer r.updateMetric()
	r.rateLimiter.rateLimiter.Stop()
}

func (r *rateLimiterWithMetric) QPS() float32 {
	r.rateLimiter.Lock()
	defer r.rateLimiter.Unlock()
	return r.rateLimiter.rateLimiter.QPS()
}

func (r *rateLimiterWithMetric) updateMetric() {
	tokens := r.tokens()
	allTags, _ := tag.New(context.Background(), tag.Upsert(tagClientName, r.name))
	stats.Record(allTags, k8sClientTokensRemainingMeasure.M(tokens))
}

func (r *rateLimiterWithMetric) tokens() float64 {
	// tokens is not exposed, filed https://github.com/golang/go/issues/50035
	tokensField := reflect.Indirect(reflect.ValueOf(r.limiter)).FieldByName("tokens")
	tokensField = reflect.NewAt(tokensField.Type(), unsafe.Pointer(tokensField.UnsafeAddr())).Elem()
	tokens := tokensField.Interface().(float64)
	return tokens
}
