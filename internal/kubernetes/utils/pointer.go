package utils

import "time"

// DurationPtr returns a pointer to the given duration
func DurationPtr(dur time.Duration) *time.Duration {
	return &dur
}

func AsInterfaces[T any](errs []T) []interface{} {
	if len(errs) == 0 {
		return nil
	}
	r := make([]interface{}, len(errs), len(errs))
	for i := range errs {
		r[i] = errs[i]
	}
	return r
}
