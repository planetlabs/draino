package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type TestCacheObject struct {
	TTL      time.Duration
	Key      string
	Value    string
	Expected bool
}

func TestCache(t *testing.T) {
	tests := []struct {
		Name        string
		DefaultTTL  time.Duration
		WaitFor     time.Duration
		SkipCleanup bool
		Objects     []TestCacheObject
	}{
		{
			Name:       "Should get object after time",
			DefaultTTL: time.Minute,
			WaitFor:    10 * time.Second,
			Objects: []TestCacheObject{
				{TTL: 0, Key: "foo", Value: "bar", Expected: true},
			},
		},
		{
			Name:       "Should delete an outdated object",
			DefaultTTL: time.Minute,
			WaitFor:    2 * time.Minute,
			Objects: []TestCacheObject{
				{TTL: 0, Key: "foo", Value: "bar", Expected: false},
			},
		},
		{
			Name:        "Should not return outdated object even if cleanup was not executed",
			DefaultTTL:  time.Minute,
			WaitFor:     2 * time.Minute,
			SkipCleanup: true,
			Objects: []TestCacheObject{
				{TTL: 0, Key: "foo", Value: "bar", Expected: false},
			},
		},
		{
			Name:       "Should only return objects that are not outdated yet",
			DefaultTTL: time.Minute,
			WaitFor:    10 * time.Second,
			Objects: []TestCacheObject{
				{TTL: time.Second, Key: "outdated", Value: "bar", Expected: false},
				{TTL: time.Minute, Key: "still-there", Value: "bar", Expected: true},
				{TTL: 2 * time.Minute, Key: "also-there", Value: "bar", Expected: true},
				{TTL: 7 * time.Second, Key: "not-found-either", Value: "bar", Expected: false},
			},
		},
		{
			Name:        "Should only return objects that are not outdated yet (even without cleanup)",
			DefaultTTL:  time.Minute,
			WaitFor:     10 * time.Second,
			SkipCleanup: true,
			Objects: []TestCacheObject{
				{TTL: time.Second, Key: "outdated", Value: "bar", Expected: false},
				{TTL: time.Minute, Key: "still-there", Value: "bar", Expected: true},
				{TTL: 2 * time.Minute, Key: "also-there", Value: "bar", Expected: true},
				{TTL: 7 * time.Second, Key: "not-found-either", Value: "bar", Expected: false},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			cache := NewTTLCache[string](tt.DefaultTTL, time.Minute)
			until := time.Now().Add(tt.WaitFor)

			for _, obj := range tt.Objects {
				if obj.TTL == 0 {
					cache.Add(obj.Key, obj.Value)
				} else {
					cache.AddCustomTTL(obj.Key, obj.Value, obj.TTL)
				}
			}

			if !tt.SkipCleanup {
				cache.Cleanup(until)
			}

			for _, obj := range tt.Objects {
				_, exist := cache.Get(obj.Key, until)
				assert.Equal(t, obj.Expected, exist, "Object is not expected here")
			}
		})
	}
}
