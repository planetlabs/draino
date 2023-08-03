package circuitbreaker

import "sigs.k8s.io/controller-runtime/pkg/manager"

type State string

const (
	Closed   State = "closed"
	Open     State = "open"
	HalfOpen State = "half-open"
)

type CircuitBreaker interface {
	// State return the current state of the circuit breaker
	State() State

	// IsOpen , when the circuit is open the functionality is blocked
	IsOpen() bool

	// IsHalfOpen , when the circuit is half open the functionality can be retried
	IsHalfOpen() bool

	//IsClose returns true when everything is fine and that we can proceed with the functionality
	IsClose() bool

	// HalfOpenTry replies true if it is acceptable to make an attempt
	HalfOpenTry() bool
}

type NamedCircuitBreaker interface {
	CircuitBreaker
	manager.Runnable
	Name() string
}
