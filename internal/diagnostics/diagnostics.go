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
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	"github.com/planetlabs/draino/internal/kubernetes/utils"
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
		return NodeDiagnostics{Errors: []interface{}{err}}
	}
	groupKey := diag.keyGetter.GetGroupKey(&node)

	var drainBufferAt *time.Time
	nextDrainBufferTime, _ := diag.drainBuffer.NextDrain(groupKey)
	if diag.drainBuffer.IsReady() && !nextDrainBufferTime.IsZero() {
		drainBufferAt = &nextDrainBufferTime
	}
	drainBufferConfig, _ := diag.drainBuffer.GetDrainBufferConfigurationDetails(ctx, &node)

	var dsr DrainSimulationResult
	var errs []error
	dsr.CanDrain, dsr.Reasons, errs = diag.drainSimulator.SimulateDrain(ctx, &node)
	dsr.Errors = utils.AsInterfaces(errs)
	ng := node.Labels["nodegroups.datadoghq.com/name"]
	ngns := node.Labels["nodegroups.datadoghq.com/namespace"]
	zone := node.Labels["topology.ebs.csi.aws.com/zone"]

	nlaTaint := ""
	if v, hasTaint := k8sclient.GetNLATaint(&node); hasTaint {
		nlaTaint = v.Value
	}
	return NodeDiagnostics{
		Node:              node.Name,
		Nodegroup:         ng,
		Namespace:         ngns,
		Zone:              zone,
		TaintNLA:          nlaTaint,
		GroupKey:          string(groupKey),
		Filters:           diag.filter.FilterNode(ctx, &node).OnlyFailingChecks(),
		Retry:             diag.getRetryDiagnostics(&node),
		DrainBufferAt:     drainBufferAt,
		DrainBufferConfig: drainBufferConfig,
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
	Reasons  []string      `json:",omitempty"`
	Errors   []interface{} `json:",omitempty"`
}
type DrainBufferDiagnostics struct {
	NextAttemptAfter time.Time `json:",omitempty"`
	RetryCount       int       `json:",omitempty"`
	Warning          bool      `json:",omitempty"`
}

type NodeDiagnostics struct {
	Node              string                                    `json:"node"`
	Nodegroup         string                                    `json:"nodegroup,omitempty"`
	Namespace         string                                    `json:"namespace,omitempty"`
	Zone              string                                    `json:"zone,omitempty"`
	TaintNLA          string                                    `json:"taintNLA,omitempty"`
	Errors            []interface{}                             `json:"errors,omitempty"`
	GroupKey          string                                    `json:"groupKey,omitempty"`
	DrainBufferAt     *time.Time                                `json:"drainBufferAt,omitempty"`
	DrainBufferConfig *kubernetes.MetadataSearch[time.Duration] `json:"drainBufferConfig,omitempty"`
	Filters           filters.FilterOutput                      `json:"filters,omitempty"`
	Retry             *RetryDiagnostics                         `json:"retry,omitempty"`
	Conditions        []kubernetes.SuppliedCondition            `json:"conditions,omitempty"`
	DrainSimulation   DrainSimulationResult                     `json:"drainSimulation,omitempty"`
	StabilityPeriodOk bool                                      `json:"stabilityPeriodOk"`
}
