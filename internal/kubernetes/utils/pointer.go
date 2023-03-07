package utils

import "time"

// DurationPtr returns a pointer to the given duration
func DurationPtr(dur time.Duration) *time.Duration {
	return &dur
}

// StrPtr returns a pointer to the given string
func StrPtr(str string) *string {
	return &str
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
