package drain

import (
	"testing"
	"time"

	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestExponentialRetryStrategy(t *testing.T) {
	tests := []struct {
		Name          string
		Retries       int
		BaseDelay     time.Duration
		ExpectedDelay time.Duration
	}{
		{
			Name:          "Should return 0 with retry 0",
			Retries:       0,
			BaseDelay:     time.Second,
			ExpectedDelay: 0,
		},
		{
			Name:          "Should return 1 * duration with retry 1",
			Retries:       1,
			BaseDelay:     time.Second,
			ExpectedDelay: time.Second,
		},
		{
			Name:          "Should return 2 * duration with retry 2",
			Retries:       2,
			BaseDelay:     time.Second,
			ExpectedDelay: 2 * time.Second,
		},
		{
			Name:          "Should return 4 * duration with retry 3",
			Retries:       3,
			BaseDelay:     time.Second,
			ExpectedDelay: 4 * time.Second,
		},
		{
			Name:          "Should return 8 * duration with retry 4",
			Retries:       4,
			BaseDelay:     time.Second,
			ExpectedDelay: 8 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			strategy := &ExponentialRetryStrategy{AlertThreashold: 10, Delay: tt.BaseDelay}
			assert.Equal(t, tt.ExpectedDelay, strategy.GetDelay(tt.Retries))
		})
	}
}

func TestNodeAnnotationRetryStrategy(t *testing.T) {
	tests := []struct {
		Name            string
		Node            *corev1.Node
		DefaultStrategy RetryStrategy

		ShouldReturnError bool
		ExpectedRetries   int
		ExpectedDelay     time.Duration
	}{
		{
			Name:            "Shloud use max retries from node",
			Node:            &corev1.Node{ObjectMeta: v1.ObjectMeta{Annotations: map[string]string{kubernetes.CustomRetryMaxAttemptAnnotation: "5"}}},
			DefaultStrategy: &StaticRetryStrategy{Delay: time.Minute, AlertThreashold: 10},
			ExpectedRetries: 5,
			ExpectedDelay:   time.Minute,
		},
		{
			Name:            "Shloud use retry delay from node",
			Node:            &corev1.Node{ObjectMeta: v1.ObjectMeta{Annotations: map[string]string{kubernetes.CustomRetryBackoffDelayAnnotation: "10s"}}},
			DefaultStrategy: &StaticRetryStrategy{Delay: time.Minute, AlertThreashold: 10},
			ExpectedRetries: 10,
			ExpectedDelay:   10 * time.Second,
		},
		{
			Name: "Shloud use retry delay & max retries from node",
			Node: &corev1.Node{ObjectMeta: v1.ObjectMeta{Annotations: map[string]string{
				kubernetes.CustomRetryBackoffDelayAnnotation: "10s",
				kubernetes.CustomRetryMaxAttemptAnnotation:   "5",
			}}},
			DefaultStrategy: &StaticRetryStrategy{Delay: time.Minute, AlertThreashold: 10},
			ExpectedRetries: 5,
			ExpectedDelay:   10 * time.Second,
		},
		{
			Name:              "Shloud use default retry delay if node annotation is not parsable",
			Node:              &corev1.Node{ObjectMeta: v1.ObjectMeta{Annotations: map[string]string{kubernetes.CustomRetryBackoffDelayAnnotation: "10seconds"}}},
			DefaultStrategy:   &StaticRetryStrategy{Delay: time.Minute, AlertThreashold: 10},
			ShouldReturnError: true,
			ExpectedRetries:   10,
			ExpectedDelay:     time.Minute,
		},
		{
			Name:              "Shloud use default if no annoation is set",
			Node:              &corev1.Node{ObjectMeta: v1.ObjectMeta{Annotations: map[string]string{"foo": "bar"}}},
			DefaultStrategy:   &StaticRetryStrategy{Delay: time.Minute, AlertThreashold: 10},
			ShouldReturnError: false,
			ExpectedRetries:   10,
			ExpectedDelay:     time.Minute,
		},
		{
			Name: "Shloud use node annotation retry even if one of the settings is not parsable",
			Node: &corev1.Node{ObjectMeta: v1.ObjectMeta{Annotations: map[string]string{
				kubernetes.CustomRetryBackoffDelayAnnotation: "10seconds",
				kubernetes.CustomRetryMaxAttemptAnnotation:   "5",
			}}},
			DefaultStrategy:   &StaticRetryStrategy{Delay: time.Minute, AlertThreashold: 10},
			ShouldReturnError: true,
			ExpectedRetries:   5,
			ExpectedDelay:     time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			strategy, err := buildNodeAnnotationRetryStrategy(tt.Node, tt.DefaultStrategy)

			if tt.ShouldReturnError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, strategy.GetAlertThreashold(), tt.ExpectedRetries)
			assert.Equal(t, strategy.GetDelay(1), tt.ExpectedDelay)
		})
	}
}
