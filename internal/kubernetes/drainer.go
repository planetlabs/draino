/*
Copyright 2018 Planet Labs Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing permissions
and limitations under the License.
*/

package kubernetes

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	core "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

// Default pod eviction settings.
const (
	DefaultMaxGracePeriod   time.Duration = 8 * time.Minute
	DefaultEvictionOverhead time.Duration = 30 * time.Second

	kindDaemonSet   = "DaemonSet"
	kindStatefulSet = "StatefulSet"

	ConditionDrainedScheduled = "DrainScheduled"
	DefaultSkipDrain          = false
)

type nodeMutatorFn func(*core.Node)

type errTimeout struct{}

func (e errTimeout) Error() string {
	return "timed out"
}

func (e errTimeout) Timeout() {}

// IsTimeout returns true if the supplied error was caused by a timeout.
func IsTimeout(err error) bool {
	err = errors.Cause(err)
	_, ok := err.(interface {
		Timeout()
	})
	return ok
}

// A Cordoner cordons nodes.
type Cordoner interface {
	// Cordon the supplied node. Marks it unschedulable for new pods.
	Cordon(n *core.Node, mutators ...nodeMutatorFn) error

	// Uncordon the supplied node. Marks it schedulable for new pods.
	Uncordon(n *core.Node, mutators ...nodeMutatorFn) error
}

// A Drainer drains nodes.
type Drainer interface {
	// Drain the supplied node. Evicts the node of all but mirror and DaemonSet pods.
	Drain(n *core.Node) error
	MarkDrain(n *core.Node, when, finish time.Time, failed bool) error
}

// A CordonDrainer both cordons and drains nodes!
type CordonDrainer interface {
	Cordoner
	Drainer
}

// A NoopCordonDrainer does nothing.
type NoopCordonDrainer struct{}

// Cordon does nothing.
func (d *NoopCordonDrainer) Cordon(n *core.Node, mutators ...nodeMutatorFn) error { return nil }

// Uncordon does nothing.
func (d *NoopCordonDrainer) Uncordon(n *core.Node, mutators ...nodeMutatorFn) error { return nil }

// Drain does nothing.
func (d *NoopCordonDrainer) Drain(n *core.Node) error { return nil }

// MarkDrain does nothing.
func (d *NoopCordonDrainer) MarkDrain(n *core.Node, when, finish time.Time, failed bool) error {
	return nil
}

// APICordonDrainer drains Kubernetes nodes via the Kubernetes API.
type APICordonDrainer struct {
	c kubernetes.Interface
	l *zap.Logger

	filter PodFilterFunc

	maxGracePeriod   time.Duration
	evictionHeadroom time.Duration
	skipDrain        bool
}

// SuppliedCondition defines the condition will be watched.
type SuppliedCondition struct {
	Type            core.NodeConditionType
	Status          core.ConditionStatus
	MinimumDuration time.Duration
}

// APICordonDrainerOption configures an APICordonDrainer.
type APICordonDrainerOption func(d *APICordonDrainer)

// MaxGracePeriod configures the maximum time to wait for a pod eviction. Pod
// containers will be allowed this much time to shutdown once they receive a
// SIGTERM before they are sent a SIGKILL.
func MaxGracePeriod(m time.Duration) APICordonDrainerOption {
	return func(d *APICordonDrainer) {
		d.maxGracePeriod = m
	}
}

// EvictionHeadroom configures an amount of time to wait in addition to the
// MaxGracePeriod for the API server to report a pod deleted.
func EvictionHeadroom(h time.Duration) APICordonDrainerOption {
	return func(d *APICordonDrainer) {
		d.evictionHeadroom = h
	}
}

// WithPodFilter configures a filter that may be used to exclude certain pods
// from eviction when draining.
func WithPodFilter(f PodFilterFunc) APICordonDrainerOption {
	return func(d *APICordonDrainer) {
		d.filter = f
	}
}

// WithDrain determines if we're actually going to drain nodes
func WithSkipDrain(b bool) APICordonDrainerOption {
	return func(d *APICordonDrainer) {
		d.skipDrain = b
	}
}

// WithAPICordonDrainerLogger configures a APICordonDrainer to use the supplied
// logger.
func WithAPICordonDrainerLogger(l *zap.Logger) APICordonDrainerOption {
	return func(d *APICordonDrainer) {
		d.l = l
	}
}

// NewAPICordonDrainer returns a CordonDrainer that cordons and drains nodes via
// the Kubernetes API.
func NewAPICordonDrainer(c kubernetes.Interface, ao ...APICordonDrainerOption) *APICordonDrainer {
	d := &APICordonDrainer{
		c:                c,
		l:                zap.NewNop(),
		filter:           NewPodFilters(),
		maxGracePeriod:   DefaultMaxGracePeriod,
		evictionHeadroom: DefaultEvictionOverhead,
		skipDrain:        DefaultSkipDrain,
	}
	for _, o := range ao {
		o(d)
	}
	return d
}

func (d *APICordonDrainer) deleteTimeout() time.Duration {
	return d.maxGracePeriod + d.evictionHeadroom
}

// Cordon the supplied node. Marks it unschedulable for new pods.
func (d *APICordonDrainer) Cordon(n *core.Node, mutators ...nodeMutatorFn) error {
	fresh, err := d.c.CoreV1().Nodes().Get(n.GetName(), meta.GetOptions{})
	if err != nil {
		return errors.Wrapf(err, "cannot get node %s", n.GetName())
	}
	if fresh.Spec.Unschedulable {
		return nil
	}
	fresh.Spec.Unschedulable = true
	for _, m := range mutators {
		m(fresh)
	}
	if _, err := d.c.CoreV1().Nodes().Update(fresh); err != nil {
		return errors.Wrapf(err, "cannot cordon node %s", fresh.GetName())
	}
	return nil
}

// Uncordon the supplied node. Marks it schedulable for new pods.
func (d *APICordonDrainer) Uncordon(n *core.Node, mutators ...nodeMutatorFn) error {
	fresh, err := d.c.CoreV1().Nodes().Get(n.GetName(), meta.GetOptions{})
	if err != nil {
		return errors.Wrapf(err, "cannot get node %s", n.GetName())
	}
	if !fresh.Spec.Unschedulable {
		return nil
	}
	fresh.Spec.Unschedulable = false
	for _, m := range mutators {
		m(fresh)
	}
	if _, err := d.c.CoreV1().Nodes().Update(fresh); err != nil {
		return errors.Wrapf(err, "cannot uncordon node %s", fresh.GetName())
	}
	return nil
}

// MarkDrain set a condition on the node to mark that that drain is scheduled.
func (d *APICordonDrainer) MarkDrain(n *core.Node, when, finish time.Time, failed bool) error {
	// Do nothing if draining is not enabled.
	if d.skipDrain {
		d.l.Debug("Skipping drain because draining is disabled")
		return nil
	}

	nodeName := n.Name
	// Refresh the node object
	freshNode, err := d.c.CoreV1().Nodes().Get(nodeName, meta.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	msgSuffix := ""
	conditionStatus := core.ConditionTrue
	if !finish.IsZero() {
		if failed {
			msgSuffix = fmt.Sprintf(" | Failed: %s", finish.Format(time.RFC3339))
		} else {
			msgSuffix = fmt.Sprintf(" | Completed: %s", finish.Format(time.RFC3339))
		}
		conditionStatus = core.ConditionFalse
	}

	// Create or update the condition associated to the monitor
	now := meta.Time{Time: time.Now()}
	conditionUpdated := false
	for i, condition := range freshNode.Status.Conditions {
		if string(condition.Type) == ConditionDrainedScheduled {
			freshNode.Status.Conditions[i].LastHeartbeatTime = now
			freshNode.Status.Conditions[i].Message = "Drain activity scheduled " + when.Format(time.RFC3339) + msgSuffix
			freshNode.Status.Conditions[i].Status = conditionStatus
			conditionUpdated = true
			break
		}
	}
	if !conditionUpdated { // There was no condition found, let's create one
		freshNode.Status.Conditions = append(freshNode.Status.Conditions,
			core.NodeCondition{
				Type:               core.NodeConditionType(ConditionDrainedScheduled),
				Status:             conditionStatus,
				LastHeartbeatTime:  now,
				LastTransitionTime: now,
				Reason:             "Draino",
				Message:            "Drain activity scheduled " + when.Format(time.RFC3339) + msgSuffix,
			},
		)
	}
	if _, err := d.c.CoreV1().Nodes().UpdateStatus(freshNode); err != nil {
		return err
	}
	return nil
}

func IsMarkedForDrain(n *core.Node) bool {
	for _, condition := range n.Status.Conditions {
		if string(condition.Type) == ConditionDrainedScheduled && condition.Status == core.ConditionTrue {
			return true
		}
	}
	return false
}

// Drain the supplied node. Evicts the node of all but mirror and DaemonSet pods.
func (d *APICordonDrainer) Drain(n *core.Node) error {

	// Do nothing if draining is not enabled.
	if d.skipDrain {
		d.l.Debug("Skipping drain because draining is disabled")
		return nil
	}

	pods, err := d.getPods(n.GetName())
	if err != nil {
		return errors.Wrapf(err, "cannot get pods for node %s", n.GetName())
	}

	abort := make(chan struct{})
	errs := make(chan error, 1)
	for _, pod := range pods {
		go d.evict(pod, abort, errs)
	}
	// This will _eventually_ abort evictions. Evictions may spend up to
	// d.deleteTimeout() in d.awaitDeletion(), or 5 seconds in backoff before
	// noticing they've been aborted.
	defer close(abort)

	deadline := time.After(d.deleteTimeout())
	for range pods {
		select {
		case err := <-errs:
			if err != nil {
				return errors.Wrap(err, "cannot evict all pods")
			}
		case <-deadline:
			return errors.Wrap(errTimeout{}, "timed out waiting for evictions to complete")
		}
	}
	return nil
}

func (d *APICordonDrainer) getPods(node string) ([]core.Pod, error) {
	l, err := d.c.CoreV1().Pods(meta.NamespaceAll).List(meta.ListOptions{
		FieldSelector: fields.SelectorFromSet(fields.Set{"spec.nodeName": node}).String(),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get pods for node %s", node)
	}

	include := make([]core.Pod, 0, len(l.Items))
	for _, p := range l.Items {
		passes, err := d.filter(p)
		if err != nil {
			return nil, errors.Wrap(err, "cannot filter pods")
		}
		if passes {
			include = append(include, p)
		}
	}
	return include, nil
}

func (d *APICordonDrainer) evict(p core.Pod, abort <-chan struct{}, e chan<- error) {
	gracePeriod := int64(d.maxGracePeriod.Seconds())
	if p.Spec.TerminationGracePeriodSeconds != nil && *p.Spec.TerminationGracePeriodSeconds < gracePeriod {
		gracePeriod = *p.Spec.TerminationGracePeriodSeconds
	}
	for {
		select {
		case <-abort:
			e <- errors.New("pod eviction aborted")
			return
		default:
			err := d.c.CoreV1().Pods(p.GetNamespace()).Evict(&policy.Eviction{
				ObjectMeta:    meta.ObjectMeta{Namespace: p.GetNamespace(), Name: p.GetName()},
				DeleteOptions: &meta.DeleteOptions{GracePeriodSeconds: &gracePeriod},
			})
			switch {
			// The eviction API returns 429 Too Many Requests if a pod
			// cannot currently be evicted, for example due to a pod
			// disruption budget.
			case apierrors.IsTooManyRequests(err):
				time.Sleep(5 * time.Second)
			case apierrors.IsNotFound(err):
				e <- nil
				return
			case err != nil:
				e <- errors.Wrapf(err, "cannot evict pod %s/%s", p.GetNamespace(), p.GetName())
				return
			default:
				e <- errors.Wrapf(d.awaitDeletion(p, d.deleteTimeout()), "cannot confirm pod %s/%s was deleted", p.GetNamespace(), p.GetName())
				return
			}
		}
	}
}

func (d *APICordonDrainer) awaitDeletion(p core.Pod, timeout time.Duration) error {
	return wait.PollImmediate(1*time.Second, timeout, func() (bool, error) {
		got, err := d.c.CoreV1().Pods(p.GetNamespace()).Get(p.GetName(), meta.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, errors.Wrapf(err, "cannot get pod %s/%s", p.GetNamespace(), p.GetName())
		}
		if got.GetUID() != p.GetUID() {
			return true, nil
		}
		return false, nil
	})
}
