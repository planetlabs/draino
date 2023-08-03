package circuitbreaker

import (
	"context"
	"fmt"
	"time"

	"github.com/DataDog/compute-go/ddclient/monitor"
	"github.com/DataDog/compute-go/logs"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV1"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/flowcontrol"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

const (
	DefaultRateLimitQPS   = float32(1. / (15. * 60.)) // one every 15 min
	DefaultRateLimitBurst = 1
)

type monitorBasedCircuitBreaker struct {
	name         string
	period       time.Duration
	groupsSearch *monitor.GroupsSearch
	limiter      flowcontrol.RateLimiter
	logger       logr.Logger
	defaultState State

	currentState State
}

// implement 2 interfaces
var _ manager.Runnable = &monitorBasedCircuitBreaker{}
var _ NamedCircuitBreaker = &monitorBasedCircuitBreaker{}

func (m *monitorBasedCircuitBreaker) Name() string {
	return m.name
}

func (m *monitorBasedCircuitBreaker) State() State {
	if m.currentState == "" {
		return m.defaultState
	}
	return m.currentState
}

func (m *monitorBasedCircuitBreaker) IsOpen() bool {
	return m.State() == Open
}

func (m *monitorBasedCircuitBreaker) IsHalfOpen() bool {
	return m.State() == HalfOpen
}

func (m *monitorBasedCircuitBreaker) IsClose() bool {
	return m.State() == Closed
}

func (m *monitorBasedCircuitBreaker) HalfOpenTry() bool {
	return m.limiter.TryAccept()
}

func NewMonitorBasedCircuitBreaker(name string, logger logr.Logger, period time.Duration, groupSearch *monitor.GroupsSearch, limiter flowcontrol.RateLimiter, defaultState State) (*monitorBasedCircuitBreaker, error) {
	if defaultState == "" {
		defaultState = Open
	}

	monitorCircuitBreaker := &monitorBasedCircuitBreaker{
		name:         name,
		period:       period,
		groupsSearch: groupSearch,
		limiter:      limiter,
		logger:       logger.WithName("CircuitBreaker").WithName(name),
		defaultState: defaultState,
	}

	return monitorCircuitBreaker, nil
}

// Start run the Circuit breaker to the context is Done, blocking call
func (m *monitorBasedCircuitBreaker) Start(ctx context.Context) error {
	m.logger.Info("starting")
	generateStateMetric(m)
	wait.Until(func() { m.runCircuitBreaker() }, m.period, ctx.Done())
	return nil
}

func (m *monitorBasedCircuitBreaker) runCircuitBreaker() {
	resultGroups, err := m.groupsSearch.ListMonitorGroups()
	if err != nil {
		m.logger.Error(err, "can't get monitors associated with circuit breaker. The circuit breaker will be opened")
		m.setState(Open)
		return
	}
	circuitBreakerGroupCount.WithLabelValues(m.Name()).Set(float64(len(resultGroups)))
	if len(resultGroups) == 0 {
		m.logger.Error(fmt.Errorf("no monitor found for circuit breaker"), "Circuit Breaker will be left in default state", "default_state", m.defaultState)
		m.setState(m.defaultState)
		return
	}
	m.logger.V(logs.ZapDebug).Info("Count of monitor groups found", "count", len(resultGroups))

	var oneWarning bool
	for _, grp := range resultGroups {
		status := grp.GetStatus()
		if status != datadogV1.MONITOROVERALLSTATES_OK {
			if status == datadogV1.MONITOROVERALLSTATES_WARN {
				oneWarning = true
				continue
			}
			m.setState(Open)
			return
		}
	}
	if oneWarning {
		m.setState(HalfOpen)
		return
	}
	m.setState(Closed)
}

func (m *monitorBasedCircuitBreaker) setState(state State) {
	if m.currentState != state {
		m.logger.Info("State change", "state", state, "old_state", m.currentState)
		m.currentState = state
		generateStateMetric(m)
	}
}
