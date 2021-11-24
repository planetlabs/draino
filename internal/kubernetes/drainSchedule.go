package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opencensus.io/tag"
	"go.uber.org/zap"
	core "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/record"
)

const (
	SetConditionTimeout     = 10 * time.Second
	SetConditionRetryPeriod = 50 * time.Millisecond

	DefaultPreprovisioningTimeout     = 1 * time.Hour
	DefaultPreprovisioningCheckPeriod = 30 * time.Second

	DefaultSchedulingRetryBackoffDelay = 23 * time.Minute

	CustomDrainBufferAnnotation       = "draino/drain-buffer"
	CustomRetryBackoffDelayAnnotation = "draino/retry-delay"
	CustomRetryMaxAttemptAnnotation   = "draino/retry-max-attempt"
	DrainGroupAnnotation              = "draino/drain-group"

	preprovisioningAnnotationKey   = "node-lifecycle.datadoghq.com/provision-new-node-before-drain"
	preprovisioningAnnotationValue = "true"
)

type DrainScheduler interface {
	HasSchedule(node *v1.Node) (has, failed bool)
	Schedule(node *v1.Node, failedCount int32) (time.Time, error)
	DeleteSchedule(node *v1.Node)
	DeleteScheduleByName(nodeName string)
}

type SchedulesGroup struct {
	schedules      map[string]*schedule
	schedulesChain []string
	period         time.Duration
	backoffDelay   time.Duration
	lastSchedule   *schedule
}

type NodePreprovisioningConfiguration struct {
	Timeout     time.Duration
	CheckPeriod time.Duration
}

type DrainSchedules struct {
	sync.Mutex
	labelKeysForGroups     []string
	scheduleGroups         map[string]*SchedulesGroup
	schedulingPeriod       time.Duration
	schedulingBackoffDelay time.Duration

	logger                       *zap.Logger
	drainer                      DrainerNodeReplacer
	preprovisioningConfiguration NodePreprovisioningConfiguration
	eventRecorder                record.EventRecorder
	suppliedConditions           []SuppliedCondition
	globalLocker                 GlobalBlocker
}

func NewDrainSchedules(drainer DrainerNodeReplacer, eventRecorder record.EventRecorder, schedulingPeriod, schedulingBackoffDelay time.Duration, labelKeysForGroups []string, suppliedConditions []SuppliedCondition, preprovisioningCfg NodePreprovisioningConfiguration, logger *zap.Logger, locker GlobalBlocker) DrainScheduler {
	sort.Strings(labelKeysForGroups)
	return &DrainSchedules{
		labelKeysForGroups:           labelKeysForGroups,
		scheduleGroups:               map[string]*SchedulesGroup{},
		schedulingPeriod:             schedulingPeriod,
		schedulingBackoffDelay:       schedulingBackoffDelay,
		logger:                       logger,
		drainer:                      drainer,
		preprovisioningConfiguration: preprovisioningCfg,
		eventRecorder:                eventRecorder,
		suppliedConditions:           suppliedConditions,
		globalLocker:                 locker,
	}
}

func (d *DrainSchedules) getScheduleGroup(node *v1.Node) *SchedulesGroup {
	values := []string{}
	nodeLabels := node.Labels
	if nodeLabels == nil {
		nodeLabels = map[string]string{}
	}
	for _, key := range d.labelKeysForGroups {
		values = append(values, nodeLabels[key])
	}
	if node.Annotations != nil {
		values = append(values, node.Annotations[DrainGroupAnnotation])
	}
	groupKey := strings.Join(values, "#")

	if group, ok := d.scheduleGroups[groupKey]; ok {
		return group
	}
	newGroup := SchedulesGroup{
		schedules:    map[string]*schedule{},
		period:       d.schedulingPeriod,
		backoffDelay: d.schedulingBackoffDelay,
	}
	d.scheduleGroups[groupKey] = &newGroup
	return &newGroup
}

func (d *DrainSchedules) HasSchedule(node *v1.Node) (has, failed bool) {
	d.Lock()
	defer d.Unlock()
	grp := d.getScheduleGroup(node)
	sched, ok := grp.schedules[node.GetName()]
	if !ok {
		return false, false
	}
	return true, sched.isFailed()
}

func (d *DrainSchedules) DeleteSchedule(node *v1.Node) {
	d.Lock()
	defer d.Unlock()
	d.getScheduleGroup(node).removeSchedule(node.Name)

	// Remove the Mark on the node
	if err := d.drainer.MarkDrainDelete(node); err != nil {
		// if we cannot mark the node, let's remove the schedule
		d.logger.Error("Failed to remove mark of schedule", zap.String("node", node.Name))
	}
	return
}

func (d *DrainSchedules) DeleteScheduleByName(name string) {
	d.Lock()
	defer d.Unlock()
	for _, grp := range d.scheduleGroups {
		grp.removeSchedule(name)
	}
}

func (sg *SchedulesGroup) whenNextSchedule(failedCount int32, options *schedulingOptions) time.Time {
	// compute drain schedule time
	sooner := time.Now().Add(SetConditionTimeout + time.Second)
	period := sg.period
	backoffDelay := sg.backoffDelay
	if options != nil && options.customBackoffRetryDelay != nil {
		backoffDelay = *options.customBackoffRetryDelay
	}

	var when time.Time
	var lastSchedule *schedule
	for i := len(sg.schedulesChain) - 1; i >= 0; i-- {
		lastScheduleName := sg.schedulesChain[i]
		var ok bool
		if lastSchedule, ok = sg.schedules[lastScheduleName]; ok {
			if (lastSchedule.failedCount > 0 && failedCount > 0) || (lastSchedule.failedCount == 0 && failedCount == 0) {
				// use the retry schedules or the regular schedules
				break
			}
			lastSchedule = nil
		}
	}

	// If there was no schedule in the schedule chain, use the historical known last schedule in the group
	if lastSchedule == nil && sg.lastSchedule != nil {
		lastSchedule = sg.lastSchedule
	}

	// grab custom values if any
	if lastSchedule != nil {
		if lastSchedule.customDrainBuffer != nil {
			period = *lastSchedule.customDrainBuffer
		}
		// compute next value
		if failedCount > 0 {
			when = lastSchedule.when.Add(backoffDelay)
		} else {
			when = lastSchedule.when.Add(period)
		}
	}

	if when.Before(sooner) {
		when = sooner
		if failedCount > 0 {
			when = when.Add(backoffDelay)
		}
	}
	return when
}

func (sg *SchedulesGroup) addSchedule(node *v1.Node, failedCount int32, options *schedulingOptions, scheduleRunner func(node *v1.Node, when time.Time, failedCount int32, options *schedulingOptions) *schedule) time.Time {
	when := sg.whenNextSchedule(failedCount, options)
	sg.schedulesChain = append(sg.schedulesChain, node.GetName())
	s := scheduleRunner(node, when, failedCount, options)
	sg.schedules[node.GetName()] = s
	sg.lastSchedule = s
	return when
}

func (sg *SchedulesGroup) removeSchedule(name string) {
	s, ok := sg.schedules[name]
	if ok {
		s.timer.Stop()
		delete(sg.schedules, name)
	}
	newScheduleChain := []string{}
	for _, scheduleName := range sg.schedulesChain {
		if scheduleName == name {
			continue
		}
		newScheduleChain = append(newScheduleChain, scheduleName)
	}
	sg.schedulesChain = newScheduleChain

	if len(sg.schedulesChain) == 0 {
		sg.lastSchedule = s
	}
}

func (d *DrainSchedules) Schedule(node *v1.Node, failedCount int32) (time.Time, error) {
	d.Lock()
	scheduleGroup := d.getScheduleGroup(node)
	if sched, ok := scheduleGroup.schedules[node.GetName()]; ok {
		d.Unlock()
		return sched.when, NewAlreadyScheduledError() // we already have a schedule planned
	}

	scheduleOptions, err := newScheduleOptions(node)
	if err != nil {
		nr := &core.ObjectReference{Kind: "Node", Name: node.GetName(), UID: types.UID(node.GetName())}
		d.eventRecorder.Eventf(nr, core.EventTypeWarning, eventReasonDrainConfig, err.Error())
	}

	// compute drain schedule time
	when := scheduleGroup.addSchedule(node, failedCount, scheduleOptions, d.newSchedule)
	d.Unlock()

	// Mark the node with the condition stating that drain is scheduled
	if err := d.drainer.MarkDrain(node, when, time.Time{}, false, failedCount); err != nil {
		// if we cannot mark the node, let's remove the schedule
		d.DeleteSchedule(node)
		return time.Time{}, err
	}
	return when, nil
}

type schedulingOptions struct {
	customDrainBuffer       *time.Duration
	customBackoffRetryDelay *time.Duration
}

func newScheduleOptions(node *v1.Node) (*schedulingOptions, error) {
	options := &schedulingOptions{}
	var err error
	if customDrainBuffer, ok := node.Annotations[CustomDrainBufferAnnotation]; ok {
		durationValue, err := time.ParseDuration(customDrainBuffer)
		if err != nil {
			err = fmt.Errorf("failed to parse custom drain-buffer: %s", err)
		} else {
			options.customDrainBuffer = &durationValue
		}
	}
	if customBackoffRetryDelay, ok := node.Annotations[CustomRetryBackoffDelayAnnotation]; ok {
		durationValue, err := time.ParseDuration(customBackoffRetryDelay)
		if err != nil {
			err = fmt.Errorf("failed to parse custom retry-delay: %s", err)
		} else {
			options.customBackoffRetryDelay = &durationValue
		}
	}
	return options, err
}

type schedule struct {
	when              time.Time
	customDrainBuffer *time.Duration
	failed            int32
	failedCount       int32
	finish            time.Time
	timer             *time.Timer
}

func (s *schedule) setFailed() {
	atomic.StoreInt32(&s.failed, 1)
}

func (s *schedule) isFailed() bool {
	return atomic.LoadInt32(&s.failed) == 1
}

func (d *DrainSchedules) newSchedule(node *v1.Node, when time.Time, failedCount int32, scheduleOptions *schedulingOptions) *schedule {
	nr := &core.ObjectReference{Kind: "Node", Name: node.GetName(), UID: types.UID(node.GetName())}
	sched := &schedule{
		when:        when,
		failedCount: failedCount,
	}
	if scheduleOptions != nil && scheduleOptions.customDrainBuffer != nil {
		sched.customDrainBuffer = scheduleOptions.customDrainBuffer
	}
	sched.timer = time.AfterFunc(time.Until(when), func() {
		log := LoggerForNode(node, d.logger)
		tags, _ := tag.New(context.Background(), tag.Upsert(TagNodeName, node.GetName())) // nolint:gosec
		if d.globalLocker != nil {
			if locked, reason := d.globalLocker.IsBlocked(); locked {
				log.Info("Cancelling drain due to globalLock", zap.String("reason", reason), zap.String("node", node.GetName()))
				d.eventRecorder.Eventf(nr, core.EventTypeWarning, eventReasonDrainFailed, "Drain cancelled due to globalLock: %s", reason)
				return
			}
		}

		// Node preprovisioning
		if d.hasPreprovisioningAnnotation(node) {
			log.Info("Start pre-provisioning before drain")
			replacementRequestEventDone := false // Flag to be sure that we produce the event only once.
			preprovisionStartTime := time.Now()
			tags, _ := tag.New(context.Background(), tag.Upsert(TagReason, newNodeRequestReasonPreprovisioning)) // nolint:gosec
			if err := wait.PollImmediate(
				d.preprovisioningConfiguration.CheckPeriod,
				d.preprovisioningConfiguration.Timeout,
				func() (bool, error) {
					replacementStatus, err := d.drainer.PreprovisionNode(node)
					if err != nil {
						log.Error("Failed to validate node-replacement status", zap.Error(err))
						return false, nil
					}
					if !replacementRequestEventDone {
						d.eventRecorder.Event(nr, core.EventTypeNormal, eventReasonNodePreprovisioning, "Node pre-provisioning before drain: request done.")
						replacementRequestEventDone = true
					}
					if replacementStatus == NodeReplacementStatusDone {
						d.eventRecorder.Event(nr, core.EventTypeNormal, eventReasonNodePreprovisioningCompleted, "Node pre-provisioning before drain: completed.")
						return true, nil
					}
					return false, nil
				},
			); err != nil {
				log.Error("Failed pre-provisioning")
				d.handleDrainFailure(sched, log, &NodePreprovisioningTimeoutError{}, tags, node)
				tags, _ = tag.New(tags, tag.Upsert(TagResult, tagResultFailed)) // nolint:gosec
				StatRecordForNode(tags, node, MeasurePreprovisioningLatency.M(sinceInMilliseconds(preprovisionStartTime)))
				return
			}
			log.Info("Pre-provisioning completed")
			tags, _ = tag.New(tags, tag.Upsert(TagResult, tagResultSucceeded)) // nolint:gosec
			StatRecordForNode(tags, node, MeasurePreprovisioningLatency.M(sinceInMilliseconds(preprovisionStartTime)))
		}

		// Node drain
		d.eventRecorder.Event(nr, core.EventTypeWarning, eventReasonDrainStarting, "Draining node")
		if err := d.drainer.Drain(node); err != nil {
			d.handleDrainFailure(sched, log, err, tags, node)
			return
		}
		sched.finish = time.Now()
		log.Info("Drained")
		tags, _ = tag.New(tags, tag.Upsert(TagResult, tagResultSucceeded)) // nolint:gosec
		StatRecordForEachCondition(tags, node, GetNodeOffendingConditions(node, d.suppliedConditions), MeasureNodesDrained.M(1))
		d.eventRecorder.Event(nr, core.EventTypeWarning, eventReasonDrainSucceeded, "Drained node")
		if err := d.drainer.MarkDrain(node, when, sched.finish, false, failedCount); err != nil {
			d.eventRecorder.Eventf(nr, core.EventTypeWarning, eventReasonDrainFailed, "Failed to place drain condition: %v", err)
			log.Error(fmt.Sprintf("Failed to place condition following drain success : %v", err))
		}
	})
	return sched
}

func (d *DrainSchedules) handleDrainFailure(sched *schedule, log *zap.Logger, drainError error, tags context.Context, node *v1.Node) context.Context {
	nr := &core.ObjectReference{Kind: "Node", Name: node.GetName(), UID: types.UID(node.GetName())}
	sched.finish = time.Now()
	sched.setFailed()
	sched.failedCount++
	log.Info("Failed to drain", zap.Error(drainError))
	tags, _ = tag.New(tags, tag.Upsert(TagResult, tagResultFailed), tag.Upsert(TagFailureCause, string(getFailureCause(drainError)))) // nolint:gosec
	StatRecordForEachCondition(tags, node, GetNodeOffendingConditions(node, d.suppliedConditions), MeasureNodesDrained.M(1))
	d.eventRecorder.Eventf(nr, core.EventTypeWarning, eventReasonDrainFailed, "Draining failed: %v", drainError)
	if err := d.drainer.MarkDrain(node, sched.when, sched.finish, true, sched.failedCount); err != nil {
		log.Error("Failed to place condition following drain failure")
	}
	return tags
}

func (d *DrainSchedules) hasPreprovisioningAnnotation(node *v1.Node) bool {
	return node.Annotations[preprovisioningAnnotationKey] == preprovisioningAnnotationValue
}

type AlreadyScheduledError struct {
	error
}

func NewAlreadyScheduledError() error {
	return &AlreadyScheduledError{
		errors.New("drain schedule is already planned for that node"),
	}
}
func IsAlreadyScheduledError(err error) bool {
	_, ok := err.(*AlreadyScheduledError)
	return ok
}

type FailureCause string

const (
	OverlappingPodDisruptionBudgets FailureCause = "overlapping_pod_disruption_budgets"
	PodEvictionTimeout              FailureCause = "pod_eviction_timeout"
	PodDeletionTimeout              FailureCause = "pod_deletion_timeout"
	VolumeCleanup                   FailureCause = "volume_cleanup"
	NodePreprovisioning             FailureCause = "node_preprovisioning_timeout"
)

func getFailureCause(err error) FailureCause {
	if errors.As(err, &NodePreprovisioningTimeoutError{}) {
		return NodePreprovisioning
	}
	if errors.As(err, &OverlappingDisruptionBudgetsError{}) {
		return OverlappingPodDisruptionBudgets
	}
	if errors.As(err, &PodEvictionTimeoutError{}) {
		return PodEvictionTimeout
	}
	if errors.As(err, &PodDeletionTimeoutError{}) {
		return PodDeletionTimeout
	}
	if errors.As(err, &VolumeCleanupError{}) {
		return VolumeCleanup
	}
	return ""
}

func sinceInMilliseconds(startTime time.Time) float64 {
	return float64(time.Since(startTime).Nanoseconds()) / 1e6
}
