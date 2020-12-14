package kubernetes

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.uber.org/zap"
	core "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
)

const (
	SetConditionTimeout     = 10 * time.Second
	SetConditionRetryPeriod = 50 * time.Millisecond

	CustomDrainBufferAnnotation = "draino/drain-buffer"
	DrainGroupAnnotation        = "draino/drain-group"
)

type DrainScheduler interface {
	HasSchedule(node *v1.Node) (has, failed bool)
	Schedule(node *v1.Node) (time.Time, error)
	DeleteSchedule(node *v1.Node)
	DeleteScheduleByName(nodeName string)
}

type SchedulesGroup struct {
	schedules      map[string]*schedule
	schedulesChain []string
	period         time.Duration
}

type DrainSchedules struct {
	sync.Mutex
	labelKeysForGroups []string
	scheduleGroups     map[string]*SchedulesGroup
	period             time.Duration

	logger        *zap.Logger
	drainer       Drainer
	eventRecorder record.EventRecorder
}

func NewDrainSchedules(drainer Drainer, eventRecorder record.EventRecorder, period time.Duration, labelKeysForGroups []string, logger *zap.Logger) DrainScheduler {
	sort.Strings(labelKeysForGroups)
	return &DrainSchedules{
		labelKeysForGroups: labelKeysForGroups,
		scheduleGroups:     map[string]*SchedulesGroup{},
		period:             period,
		logger:             logger,
		drainer:            drainer,
		eventRecorder:      eventRecorder,
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
		schedules: map[string]*schedule{},
		period:    d.period,
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
}

func (d *DrainSchedules) DeleteScheduleByName(name string) {
	d.Lock()
	defer d.Unlock()
	for _, grp := range d.scheduleGroups {
		grp.removeSchedule(name)
	}
}

func (sg *SchedulesGroup) whenNextSchedule() time.Time {
	// compute drain schedule time
	sooner := time.Now().Add(SetConditionTimeout + time.Second)
	period := sg.period
	var when time.Time
	if len(sg.schedulesChain) > 0 {
		lastScheduleName := sg.schedulesChain[len(sg.schedulesChain)-1]
		if lastSchedule, ok := sg.schedules[lastScheduleName]; ok {
			if lastSchedule.customDrainBuffer != nil {
				fmt.Printf("found custom drain buffer\n")
				period = *lastSchedule.customDrainBuffer
			}
			when = lastSchedule.when.Add(period)
		}
	}
	if when.Before(sooner) {
		when = sooner
	}
	fmt.Printf("sched: %v\n", when)
	return when
}

func (sg *SchedulesGroup) addSchedule(node *v1.Node, scheduleRunner func(node *v1.Node, when time.Time) *schedule) time.Time {
	when := sg.whenNextSchedule()
	sg.schedulesChain = append(sg.schedulesChain, node.GetName())
	sg.schedules[node.GetName()] = scheduleRunner(node, when)
	return when
}

func (sg *SchedulesGroup) removeSchedule(name string) {
	if s, ok := sg.schedules[name]; ok {
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
}

func (d *DrainSchedules) Schedule(node *v1.Node) (time.Time, error) {
	d.Lock()
	scheduleGroup := d.getScheduleGroup(node)
	if sched, ok := scheduleGroup.schedules[node.GetName()]; ok {
		d.Unlock()
		return sched.when, NewAlreadyScheduledError() // we already have a schedule planned
	}

	// compute drain schedule time
	when := scheduleGroup.addSchedule(node, d.newSchedule)
	d.Unlock()

	// Mark the node with the condition stating that drain is scheduled
	if err := RetryWithTimeout(
		func() error {
			return d.drainer.MarkDrain(node, when, time.Time{}, false)
		},
		SetConditionRetryPeriod,
		SetConditionTimeout,
	); err != nil {
		// if we cannot mark the node, let's remove the schedule
		d.DeleteSchedule(node)
		return time.Time{}, err
	}
	return when, nil
}

type schedule struct {
	when              time.Time
	customDrainBuffer *time.Duration
	failed            int32
	finish            time.Time
	timer             *time.Timer
}

func (s *schedule) setFailed() {
	atomic.StoreInt32(&s.failed, 1)
}

func (s *schedule) isFailed() bool {
	return atomic.LoadInt32(&s.failed) == 1
}

func (d *DrainSchedules) newSchedule(node *v1.Node, when time.Time) *schedule {
	nr := &core.ObjectReference{Kind: "Node", Name: node.GetName(), UID: types.UID(node.GetName())}
	sched := &schedule{
		when: when,
	}
	if customDrainBuffer, ok := node.Annotations[CustomDrainBufferAnnotation]; ok {
		durationValue, err := time.ParseDuration(customDrainBuffer)
		if err != nil {
			d.eventRecorder.Eventf(nr, core.EventTypeWarning, eventReasonDrainConfig, "Failed to parse custom drain-buffer: %s", customDrainBuffer)
		}
		sched.customDrainBuffer = &durationValue
	}

	sched.timer = time.AfterFunc(time.Until(when), func() {
		log := d.logger.With(zap.String("node", node.GetName()))
		tags, _ := tag.New(context.Background(), tag.Upsert(TagNodeName, node.GetName())) // nolint:gosec
		d.eventRecorder.Event(nr, core.EventTypeWarning, eventReasonDrainStarting, "Draining node")
		if err := d.drainer.Drain(node); err != nil {
			sched.finish = time.Now()
			sched.setFailed()
			log.Info("Failed to drain", zap.Error(err))
			tags, _ = tag.New(tags, tag.Upsert(TagResult, tagResultFailed)) // nolint:gosec
			stats.Record(tags, MeasureNodesDrained.M(1))
			d.eventRecorder.Eventf(nr, core.EventTypeWarning, eventReasonDrainFailed, "Draining failed: %v", err)
			if err := RetryWithTimeout(
				func() error {
					return d.drainer.MarkDrain(node, when, sched.finish, true)
				},
				SetConditionRetryPeriod,
				SetConditionTimeout,
			); err != nil {
				log.Error("Failed to place condition following drain failure")
			}
			return
		}
		sched.finish = time.Now()
		log.Info("Drained")
		tags, _ = tag.New(tags, tag.Upsert(TagResult, tagResultSucceeded)) // nolint:gosec
		stats.Record(tags, MeasureNodesDrained.M(1))
		d.eventRecorder.Event(nr, core.EventTypeWarning, eventReasonDrainSucceeded, "Drained node")
		if err := RetryWithTimeout(
			func() error {
				return d.drainer.MarkDrain(node, when, sched.finish, false)
			},
			SetConditionRetryPeriod,
			SetConditionTimeout,
		); err != nil {
			d.eventRecorder.Eventf(nr, core.EventTypeWarning, eventReasonDrainFailed, "Failed to place drain condition: %v", err)
			log.Error(fmt.Sprintf("Failed to place condition following drain success : %v", err))
		}
	})
	return sched
}

type AlreadyScheduledError struct {
	error
}

func NewAlreadyScheduledError() error {
	return &AlreadyScheduledError{
		fmt.Errorf("drain schedule is already planned for that node"),
	}
}
func IsAlreadyScheduledError(err error) bool {
	_, ok := err.(*AlreadyScheduledError)
	return ok
}
