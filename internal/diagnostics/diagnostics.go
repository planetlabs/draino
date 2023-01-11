package diagnostics

import (
	"context"
	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/candidate_runner"
	"github.com/planetlabs/draino/internal/candidate_runner/filters"
	drainbuffer "github.com/planetlabs/draino/internal/drain_buffer"
	"github.com/planetlabs/draino/internal/groups"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/analyser"
	"github.com/planetlabs/draino/internal/kubernetes/drain"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"
)

type Diagnostics struct {
	client              client.Client
	logger              logr.Logger
	clock               clock.Clock
	retryWall           drain.RetryWall
	filter              filters.Filter
	suppliedConditions  []kubernetes.SuppliedCondition
	drainBuffer         drainbuffer.DrainBuffer
	stabilityPeriod     analyser.StabilityPeriodChecker
	nodeSorters         candidate_runner.NodeSorters
	nodeIteratorFactory candidate_runner.NodeIteratorFactory
	drainSimulator      drain.DrainSimulator

	keyGetter groups.GroupKeyGetter
}

var _ Diagnostician = &Diagnostics{}

func (diag *Diagnostics) GetName() string {

	return "NodeDiagnostic"
}

func (diag *Diagnostics) GetNodeDiagnostic(ctx context.Context, nodeName string) interface{} {
	var node v1.Node
	if err := diag.client.Get(ctx, types.NamespacedName{Name: nodeName}, &node); err != nil {
		return NodeDiagnostics{Errors: []error{err}}
	}
	groupKey := diag.keyGetter.GetGroupKey(&node)

	var drainBufferAt *time.Time
	nextDrainBufferTime, _ := diag.drainBuffer.NextDrain(groupKey)
	if diag.drainBuffer.IsReady() && !nextDrainBufferTime.IsZero() {
		drainBufferAt = &nextDrainBufferTime
	}

	var dsr DrainSimulationResult
	dsr.CanDrain, dsr.Reasons, dsr.Errors = diag.drainSimulator.SimulateDrain(ctx, &node)

	return NodeDiagnostics{
		GroupKey:          string(groupKey),
		Filters:           diag.filter.FilterNode(ctx, &node).OnlyFailingChecks(),
		Retry:             diag.getRetryDiagnostics(&node),
		DrainBufferAt:     drainBufferAt,
		DrainSimulation:   dsr,
		Conditions:        kubernetes.GetNodeOffendingConditions(&node, diag.suppliedConditions),
		StabilityPeriodOk: diag.stabilityPeriod.StabilityPeriodAcceptsDrain(ctx, &node, diag.clock.Now()),
	}
}

type RetryDiagnostics struct {
	NextAttemptAfter time.Time `json:",omitempty"`
	RetryCount       int       `json:",omitempty"`
	Warning          bool      `json:",omitempty"`
}

func (diag *Diagnostics) getRetryDiagnostics(node *v1.Node) *RetryDiagnostics {

	count := diag.retryWall.GetDrainRetryAttemptsCount(node)
	if count == 0 {
		return nil
	}
	return &RetryDiagnostics{
		NextAttemptAfter: diag.retryWall.GetRetryWallTimestamp(node),
		RetryCount:       count,
		Warning:          diag.retryWall.IsAboveAlertingThreshold(node),
	}
}

type DrainSimulationResult struct {
	CanDrain bool
	Reasons  []string `json:",omitempty"`
	Errors   []error  `json:",omitempty"`
}
type DrainBufferDiagnostics struct {
	NextAttemptAfter time.Time `json:",omitempty"`
	RetryCount       int       `json:",omitempty"`
	Warning          bool      `json:",omitempty"`
}

type NodeDiagnostics struct {
	Errors            []error                        `json:",omitempty"`
	GroupKey          string                         `json:",omitempty"`
	DrainBufferAt     *time.Time                     `json:",omitempty"`
	Filters           filters.FilterOutput           `json:",omitempty"`
	Retry             *RetryDiagnostics              `json:",omitempty"`
	Conditions        []kubernetes.SuppliedCondition `json:",omitempty"`
	DrainSimulation   DrainSimulationResult          `json:",omitempty"`
	StabilityPeriodOk bool
}
