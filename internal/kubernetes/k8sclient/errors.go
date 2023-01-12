package k8sclient

type ClientSideRateLimit struct{}

func (c ClientSideRateLimit) Error() string {
	return "ClientSideRateLimiting"
}

var _ error = &ClientSideRateLimit{}

func IsClientSideRateLimiting(err error) bool {
	_, ok := err.(*ClientSideRateLimit)
	return ok
}
