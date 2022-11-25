package utils

import "time"

// DurationPtr returns a pointer to the given duration
func DurationPtr(dur time.Duration) *time.Duration {
	return &dur
}
