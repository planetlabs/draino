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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"go.opencensus.io/tag"
	"go.uber.org/zap"
	core "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	runtimejson "k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
)

// Default pod eviction settings.
const (
	DefaultMaxGracePeriod               = 8 * time.Minute
	DefaultEvictionOverhead             = 30 * time.Second
	DefaultPVCRecreateTimeout           = 3 * time.Minute
	DefaultPodDeletePeriodWaitingForPVC = 10 * time.Second

	KindDaemonSet   = "DaemonSet"
	KindStatefulSet = "StatefulSet"

	ConditionDrainedScheduled = "DrainScheduled"
	DefaultSkipDrain          = false

	PVCStorageClassCleanupAnnotationKey   = "draino/delete-pvc-and-pv"
	PVCStorageClassCleanupAnnotationValue = "true"

	CompletedStr = "Completed"
	FailedStr    = "Failed"
	ScheduledStr = "Scheduled"

	NodeLabelKeyReplaceRequest      = "node.datadoghq.com/replace"
	NodeLabelValueReplaceRequested  = "requested"
	NodeLabelValueReplaceProcessing = "processing"
	NodeLabelValueReplaceDone       = "done"

	eventReasonEvictionStarting  = "EvictionStarting"
	eventReasonEvictionSucceeded = "EvictionSucceeded"
	eventReasonEvictionFailed    = "EvictionFailed"

	EvictionAPIURLAnnotationKey = "draino/eviction-api-url"
)

type nodeMutatorFn func(*core.Node)

type EvictionEndpointError struct {
	Message string
}

func (e EvictionEndpointError) Error() string {
	if e.Message == "" {
		return "Eviction endpoint error"
	}
	return "eviction endpoint error: " + e.Message
}

type NodePreprovisioningTimeoutError struct {
}

func (e NodePreprovisioningTimeoutError) Error() string {
	return "timed out waiting for node pre-provisioning"
}

type PodEvictionTimeoutError struct {
}

func (e PodEvictionTimeoutError) Error() string {
	return "timed out waiting for pod disruption to be allowed"
}

type OverlappingDisruptionBudgetsError struct {
}

func (e OverlappingDisruptionBudgetsError) Error() string {
	return "overlapping pod disruption budgets"
}

type PodDeletionTimeoutError struct {
}

func (e PodDeletionTimeoutError) Error() string {
	return "timed out waiting for pod to be deleted (stuck terminating, check finalizers)"
}

type VolumeCleanupError struct {
	Err error
}

func (e VolumeCleanupError) Error() string {
	return "error while cleaning up volumes: " + e.Err.Error()
}

func (e VolumeCleanupError) Unwrap() error {
	return e.Err
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
	MarkDrain(n *core.Node, when, finish time.Time, failed bool, failCount int32) error
	GetPodsToDrain(node string, podStore PodStore) ([]*core.Pod, error)
	GetMaxDrainAttemptsBeforeFail() int32
}

type NodeReplacementStatus string

const (
	NodeReplacementStatusRequested        NodeReplacementStatus = "requested"
	NodeReplacementStatusDone             NodeReplacementStatus = "done"
	NodeReplacementStatusNone             NodeReplacementStatus = "none"
	NodeReplacementStatusBlockedByLimiter NodeReplacementStatus = "blockedByLimiter"
)

// A NodeReplacer helps to request for node replacement
type NodeReplacer interface {
	ReplaceNode(n *core.Node) (NodeReplacementStatus, error)
	PreprovisionNode(n *core.Node) (NodeReplacementStatus, error)
}

// A CordonDrainer both cordons and drains nodes!
type CordonDrainer interface {
	Cordoner
	Drainer
	NodeReplacer
}

type DrainerNodeReplacer interface {
	Drainer
	NodeReplacer
}

// A NoopCordonDrainer does nothing.
type NoopCordonDrainer struct{}

func (d *NoopCordonDrainer) GetPodsToDrain(_ string, _ PodStore) ([]*core.Pod, error) {
	return nil, nil
}

// Cordon does nothing.
func (d *NoopCordonDrainer) Cordon(_ *core.Node, _ ...nodeMutatorFn) error { return nil }

// Uncordon does nothing.
func (d *NoopCordonDrainer) Uncordon(_ *core.Node, _ ...nodeMutatorFn) error { return nil }

// Drain does nothing.
func (d *NoopCordonDrainer) Drain(_ *core.Node) error { return nil }

// MarkDrain does nothing.
func (d *NoopCordonDrainer) MarkDrain(_ *core.Node, _, _ time.Time, _ bool, _ int32) error {
	return nil
}

func (d *NoopCordonDrainer) GetMaxDrainAttemptsBeforeFail() int32 {
	return 0
}

// ReplaceNode return none
func (d *NoopCordonDrainer) ReplaceNode(n *core.Node) (NodeReplacementStatus, error) {
	return NodeReplacementStatusNone, nil
}

// PreprovisionNode return none
func (d *NoopCordonDrainer) PreprovisionNode(n *core.Node) (NodeReplacementStatus, error) {
	return NodeReplacementStatusNone, nil
}

var _ CordonDrainer = &APICordonDrainer{}

// APICordonDrainer drains Kubernetes nodes via the Kubernetes API.
type APICordonDrainer struct {
	c                  kubernetes.Interface
	l                  *zap.Logger
	eventRecorder      record.EventRecorder
	runtimeObjectStore RuntimeObjectStore

	filter                 PodFilterFunc
	cordonLimiter          CordonLimiter
	nodeReplacementLimiter NodeReplacementLimiter

	maxGracePeriod             time.Duration
	evictionHeadroom           time.Duration
	skipDrain                  bool
	maxDrainAttemptsBeforeFail int32

	storageClassesAllowingPVDeletion map[string]struct{}
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

// WithCordonLimiter configures an APICordonDrainer to limit cordon activity
func WithCordonLimiter(limiter CordonLimiter) APICordonDrainerOption {
	return func(d *APICordonDrainer) {
		d.cordonLimiter = limiter
	}
}

// WithNodeReplacementLimiter configures an APICordonDrainer to limit node replacement activity
func WithNodeReplacementLimiter(limiter NodeReplacementLimiter) APICordonDrainerOption {
	return func(d *APICordonDrainer) {
		d.nodeReplacementLimiter = limiter
	}
}

// WithStorageClassesAllowingDeletion configures an APICordonDrainer to allow deletion of PV/PVC for some storage classes only
func WithStorageClassesAllowingDeletion(storageClasses []string) APICordonDrainerOption {
	return func(d *APICordonDrainer) {
		d.storageClassesAllowingPVDeletion = map[string]struct{}{}
		for _, sc := range storageClasses {
			d.storageClassesAllowingPVDeletion[sc] = struct{}{}
		}
	}
}

// WithMaxDrainAttemptsBeforeFail configures the max count of failed drain attempts before a final fail
func WithMaxDrainAttemptsBeforeFail(maxDrainAttemptsBeforeFail int) APICordonDrainerOption {
	return func(d *APICordonDrainer) {
		d.maxDrainAttemptsBeforeFail = int32(maxDrainAttemptsBeforeFail)
	}
}

// NewAPICordonDrainer returns a CordonDrainer that cordons and drains nodes via
// the Kubernetes API.
func NewAPICordonDrainer(c kubernetes.Interface, eventRecorder record.EventRecorder, ao ...APICordonDrainerOption) *APICordonDrainer {
	d := &APICordonDrainer{
		c:                c,
		l:                zap.NewNop(),
		filter:           NewPodFilters(),
		maxGracePeriod:   DefaultMaxGracePeriod,
		evictionHeadroom: DefaultEvictionOverhead,
		skipDrain:        DefaultSkipDrain,
		eventRecorder:    eventRecorder,
	}
	for _, o := range ao {
		o(d)
	}
	return d
}

func (d *APICordonDrainer) SetRuntimeObjectStore(store RuntimeObjectStore) {
	d.runtimeObjectStore = store
}

func (d *APICordonDrainer) deleteTimeout() time.Duration {
	return d.maxGracePeriod + d.evictionHeadroom
}

func (d *APICordonDrainer) GetMaxDrainAttemptsBeforeFail() int32 {
	return d.maxDrainAttemptsBeforeFail
}

// Cordon the supplied node. Marks it unschedulable for new pods.
func (d *APICordonDrainer) Cordon(n *core.Node, mutators ...nodeMutatorFn) error {
	if d.cordonLimiter != nil {
		canCordon, reason := d.cordonLimiter.CanCordon(n)
		if !canCordon {
			return NewLimiterError(reason)
		}
	}
	if n.Spec.Unschedulable {
		return nil
	}

	fresh, err := d.c.CoreV1().Nodes().Get(n.GetName(), meta.GetOptions{})
	if err != nil {
		return fmt.Errorf("cannot get node %s: %w", n.GetName(), err)
	}
	if fresh.Spec.Unschedulable {
		return nil
	}
	fresh.Spec.Unschedulable = true
	for _, m := range mutators {
		m(fresh)
	}
	if _, err := d.c.CoreV1().Nodes().Update(fresh); err != nil {
		return fmt.Errorf("cannot cordon node %s: %w", fresh.GetName(), err)
	}
	return nil
}

// Uncordon the supplied node. Marks it schedulable for new pods.
func (d *APICordonDrainer) Uncordon(n *core.Node, mutators ...nodeMutatorFn) error {
	fresh, err := d.c.CoreV1().Nodes().Get(n.GetName(), meta.GetOptions{})
	if err != nil {
		return fmt.Errorf("cannot get node %s: %w", n.GetName(), err)
	}
	if !fresh.Spec.Unschedulable {
		return nil
	}
	fresh.Spec.Unschedulable = false
	for _, m := range mutators {
		m(fresh)
	}
	if _, err := d.c.CoreV1().Nodes().Update(fresh); err != nil {
		return fmt.Errorf("cannot uncordon node %s: %w", fresh.GetName(), err)
	}
	return nil
}

// MarkDrain set a condition on the node to mark that the drain is scheduled.
func (d *APICordonDrainer) MarkDrain(n *core.Node, when, finish time.Time, failed bool, failCount int32) error {
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
			msgSuffix = fmt.Sprintf(" | %s: %s", FailedStr, finish.Format(time.RFC3339))
			if failCount >= d.maxDrainAttemptsBeforeFail {
				freshNode.Annotations[drainRetryAnnotationKey] = drainRetryAnnotationFailedValue
			}
		} else {
			msgSuffix = fmt.Sprintf(" | %s: %s", CompletedStr, finish.Format(time.RFC3339))
		}
		conditionStatus = core.ConditionFalse
	}

	// Create or update the condition associated to the monitor
	now := meta.Time{Time: time.Now()}
	conditionUpdated := false
	for i, condition := range freshNode.Status.Conditions {
		if string(condition.Type) != ConditionDrainedScheduled {
			continue
		}
		msgPrefix := fmt.Sprintf("[%d] | ", failCount)
		freshNode.Status.Conditions[i].LastHeartbeatTime = now
		freshNode.Status.Conditions[i].Message = msgPrefix + "Drain activity scheduled " + when.Format(time.RFC3339) + msgSuffix
		freshNode.Status.Conditions[i].Status = conditionStatus
		conditionUpdated = true
	}
	if !conditionUpdated { // There was no condition found, let's create one
		freshNode.Status.Conditions = append(freshNode.Status.Conditions,
			core.NodeCondition{
				Type:               ConditionDrainedScheduled,
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

type DrainConditionStatus struct {
	Marked         bool
	Completed      bool
	Failed         bool
	FailedCount    int32
	LastTransition time.Time
}

func IsMarkedForDrain(n *core.Node) (DrainConditionStatus, error) {
	var drainStatus DrainConditionStatus
	for _, condition := range n.Status.Conditions {
		if string(condition.Type) != ConditionDrainedScheduled {
			continue
		}
		drainStatus.Marked = true
		drainStatus.LastTransition = condition.LastTransitionTime.Time
		if condition.Status == core.ConditionFalse {
			if strings.Contains(condition.Message, CompletedStr) {
				drainStatus.Completed = true
				return drainStatus, nil
			}
			if strings.Contains(condition.Message, FailedStr) {
				//[1] | Drain activity scheduled 2020-03-20T15:50:34+01:00 | Failed: 2020-03-20T15:55:50+01:00
				drainStatus.Failed = true
				drainStatus.FailedCount = 1
				msg := strings.Split(condition.Message, " | ")
				msgPrefix := msg[0]
				if msgPrefix[0] == '[' { //Detect new prefix format
					_, err := fmt.Sscanf(msgPrefix, "[%d]", &drainStatus.FailedCount)
					if err != nil {
						return drainStatus, fmt.Errorf("cannot parse failedCount on node%s: %w", n.GetName(), err)
					}
				}
				return drainStatus, nil
			}
		} else if condition.Status == core.ConditionTrue {
			return drainStatus, nil
		}
		return drainStatus, errors.New("failed to read " + ConditionDrainedScheduled + " condition")
	}
	return drainStatus, nil
}

// Drain the supplied node. Evicts the node of all but mirror and DaemonSet pods.
func (d *APICordonDrainer) Drain(n *core.Node) error {
	// Do nothing if draining is not enabled.
	if d.skipDrain {
		LoggerForNode(n, d.l).Debug("Skipping drain because draining is disabled")
		return nil
	}

	pods, err := d.GetPodsToDrain(n.GetName(), nil)
	if err != nil {
		return fmt.Errorf("cannot get pods for node %s: %w", n.GetName(), err)
	}

	abort := make(chan struct{})
	errs := make(chan error, 1)
	for i := range pods {
		pod := pods[i]
		go func() {
			d.eventRecorder.Eventf(pod, core.EventTypeWarning, eventReasonEvictionStarting, "Evicting pod to drain node %s", n.GetName())
			if err := d.evict(pod, abort); err != nil {
				d.eventRecorder.Eventf(pod, core.EventTypeWarning, eventReasonEvictionFailed, "Eviction failed: %v", err)
				errs <- fmt.Errorf("cannot evict pod %s/%s: %w", pod.GetNamespace(), pod.GetName(), err)
				return
			}
			d.eventRecorder.Event(pod, core.EventTypeWarning, eventReasonEvictionSucceeded, "Pod evicted")
			errs <- nil // the for range pods below expects to receive one value per pod from the errs channel
		}()
	}
	// This will _eventually_ abort evictions. Evictions may spend up to
	// d.deleteTimeout() in d.awaitDeletion(), or 5 seconds in backoff before
	// noticing they've been aborted.
	//
	// Note(adrienjt): In addition, they may also spend up to:
	// - 1min/PVC awaiting PVC deletions,
	// - 1min/PV awaiting PV deletions,
	// - and DefaultPVCRecreateTimeout per PVC
	defer close(abort)

	for range pods {
		if err := <-errs; err != nil {
			return fmt.Errorf("cannot evict all pods: %w", err)
			// all remaining evictions are aborted and their errors ignored (aborted or otherwise)
			// TODO(adrienjt): capture missing errors?
			// They are registered as events on pods.
		}
	}
	return nil
}

func (d *APICordonDrainer) GetPodsToDrain(node string, podStore PodStore) ([]*core.Pod, error) {
	var err error
	var pods []*core.Pod
	if podStore != nil {
		if pods, err = podStore.ListPodsForNode(node); err != nil {
			return nil, err
		}
	} else {
		l, err := d.c.CoreV1().Pods(meta.NamespaceAll).List(meta.ListOptions{
			FieldSelector: fields.SelectorFromSet(fields.Set{"spec.nodeName": node}).String(),
		})
		if err != nil {
			return nil, fmt.Errorf("cannot get pods for node %s: %w", node, err)
		}
		for i := range l.Items {
			pods = append(pods, &l.Items[i])
		}
	}

	include := make([]*core.Pod, 0, len(pods))
	for _, p := range pods {
		passes, _, err := d.filter(*p)
		if err != nil {
			return nil, fmt.Errorf("cannot filter pods: %w", err)
		}
		if passes {
			include = append(include, p)
		}
	}
	return include, nil
}

func (d *APICordonDrainer) evict(pod *core.Pod, abort <-chan struct{}) error {
	evictionAPIURL, ok := GetAnnotationFromPodOrController(EvictionAPIURLAnnotationKey, pod, d.runtimeObjectStore)
	if ok {
		return d.evictWithOperatorAPI(evictionAPIURL, pod, abort)
	}
	return d.evictWithKubernetesAPI(pod, abort)
}

func (d *APICordonDrainer) getGracePeriod(pod *core.Pod) int64 {
	gracePeriod := int64(d.maxGracePeriod.Seconds())
	if pod.Spec.TerminationGracePeriodSeconds != nil && *pod.Spec.TerminationGracePeriodSeconds < gracePeriod {
		gracePeriod = *pod.Spec.TerminationGracePeriodSeconds
	}
	return gracePeriod
}
func (d *APICordonDrainer) evictWithKubernetesAPI(pod *core.Pod, abort <-chan struct{}) error {
	gracePeriod := d.getGracePeriod(pod)
	return d.evictionSequence(pod, abort,
		//eviction function
		func() error {
			return d.c.CoreV1().Pods(pod.GetNamespace()).Evict(&policy.Eviction{
				ObjectMeta:    meta.ObjectMeta{Namespace: pod.GetNamespace(), Name: pod.GetName()},
				DeleteOptions: &meta.DeleteOptions{GracePeriodSeconds: &gracePeriod},
			})
		},
		//error handling function
		func(err error) error {
			// The eviction API returns 500 if a pod
			// matches more than one pod disruption budgets.
			// We cannot use apierrors.IsInternalError because Reason is not set, just Code and Message.
			if statErr, ok := err.(apierrors.APIStatus); ok && statErr.Status().Code == 500 {
				return OverlappingDisruptionBudgetsError{} // this one is typed because we match it to a failure cause
			}
			return err // unexpected (we're already catching 429 and 500), may be a client side error
		},
	)

}

// evictWithOperatorAPI This function calls an Operator endpoint to perform the eviction instead of the classic kubernetes eviction endpoint
// The endpoint should support the same payload than the kubernetes eviction endpoint.
// The expected responses are:
// 200/201: eviction accepted and performed/performing
// 429    : need to retry later, equivalent to no credit in PDB
// 404    : the pod is not found, already delete
// 503    : the service is not able to answer now, potentially not reaching the leader, you should retry
// 500    : server error, that could be a transient error, retry couple of times
func (d *APICordonDrainer) evictWithOperatorAPI(url string, pod *core.Pod, abort <-chan struct{}) error {
	d.l.Info("using custom eviction endpoint", zap.String("pod", pod.Namespace+"/"+pod.Name), zap.String("endpoint", url))
	gracePeriod := d.getGracePeriod(pod)
	maxRetryOn500 := 4
	return d.evictionSequence(pod, abort,
		//eviction function
		func() error {
			evictionPayload := &policy.Eviction{
				ObjectMeta:    meta.ObjectMeta{Namespace: pod.GetNamespace(), Name: pod.GetName()},
				DeleteOptions: &meta.DeleteOptions{GracePeriodSeconds: &gracePeriod},
			}

			req, err := http.NewRequest("POST", url, GetEvictionJsonPayload(evictionPayload))
			req.Header.Set("Content-Type", "application/json")

			client := &http.Client{Timeout: 10 * time.Second}
			resp, err := client.Do(req)
			if err != nil {
				d.l.Info("custom eviction endpoint response error", zap.Error(err))
				return fmt.Errorf("cannot evict pod %s/%s, %w", pod.GetNamespace(), pod.GetName(), err)
			}
			defer resp.Body.Close()
			d.l.Info("custom eviction endpoint response", zap.String("pod", pod.Namespace+"/"+pod.Name), zap.String("endpoint", url), zap.Int("responseCode", resp.StatusCode))
			switch {
			case resp.StatusCode == http.StatusOK:
				return nil
			case resp.StatusCode == http.StatusTooManyRequests:
				return apierrors.NewTooManyRequests("retry later", 10)
			case resp.StatusCode == http.StatusNotFound:
				return apierrors.NewNotFound(schema.GroupResource{Resource: "pod"}, pod.Name)
			case resp.StatusCode == http.StatusServiceUnavailable:
				return apierrors.NewTooManyRequests("retry later, service endpoint is not the leader", 15)
			case resp.StatusCode == http.StatusInternalServerError:
				respContent, _ := ioutil.ReadAll(resp.Body)
				if maxRetryOn500 > 0 {
					maxRetryOn500--
					d.l.Info("Custom eviction endpoint returned an error", zap.Int("code", resp.StatusCode), zap.String("body", string(respContent)))
					return apierrors.NewTooManyRequests("retry later following endpoint error", 20)
				}
				d.l.Error("Too many service error from custom eviction endpoint.", zap.Int("code", resp.StatusCode), zap.String("body", string(respContent)))
				return &EvictionEndpointError{Message: fmt.Sprintf("code=%d after several retries", resp.StatusCode)}
			default:
				respContent, _ := ioutil.ReadAll(resp.Body)
				d.l.Error("Unexpected response code from custom eviction endpoint.", zap.Int("code", resp.StatusCode), zap.String("body", string(respContent)))
				return &EvictionEndpointError{Message: fmt.Sprintf("code=%d", resp.StatusCode)}
			}
		},
		//error handling function
		func(err error) error {
			return err
		},
	)
}

func (d *APICordonDrainer) evictionSequence(pod *core.Pod, abort <-chan struct{}, evictionFunc func() error, otherErrorsHandlerFunc func(e error) error) error {
	deadline := time.After(d.deleteTimeout())
	for {
		select {
		case <-abort:
			return errors.New("pod eviction aborted")
		case <-deadline:
			return PodEvictionTimeoutError{} // this one is typed because we match it to a failure cause
		default:
			var err error
			// If you try to evict a terminating pod (e.g., retry to evict a pod that's
			// already been evicted but is still terminating because of a finalizer), and
			// the pod disruption budget is currently exhausted (e.g., because said pod is
			// terminating and its replacement is still pending), the eviction API naively
			// responds with a 429: "Cannot evict pod as it would violate the pod's
			// disruption budget," instead of something less misleading (it actually
			// wouldn't violate the disruption budget, because it wouldn't change the number
			// of available pods). Later, when the replacement pod is running, if you try to
			// re-evict the terminating pod, the eviction API responds with a 201. To avoid
			// the confusion, let's not even try to evict terminating pods, because that
			// doesn't make much sense anyway. However, we still want to wait for their
			// deletion, which is why we filter here and not in GetPodsToDrain.
			if pod.DeletionTimestamp == nil {
				err = evictionFunc()
			}
			switch {
			// The eviction API returns 429 Too Many Requests if a pod
			// cannot currently be evicted, for example due to a pod
			// disruption budget.
			case apierrors.IsTooManyRequests(err):
				waitTime := 10 * time.Second
				if statErr, ok := err.(apierrors.APIStatus); ok && statErr.Status().Details != nil {
					if proposedWaitSeconds := statErr.Status().Details.RetryAfterSeconds; proposedWaitSeconds > 0 {
						waitTime = time.Duration(proposedWaitSeconds) * time.Second
					}
				}
				time.Sleep(waitTime)
			case apierrors.IsNotFound(err):
				// the pod is already gone
				// maybe we still need to perform PVC management
				err = d.deletePVCAndPV(pod)
				if err != nil {
					return VolumeCleanupError{Err: err} // this one is typed because we match it to a failure cause
				}
				return nil
			case err != nil:
				if eh := otherErrorsHandlerFunc(err); eh != nil {
					return err
				}
			default:
				err := d.awaitDeletion(pod, d.deleteTimeout())
				if err != nil {
					return fmt.Errorf("cannot confirm pod was deleted: %w", err)
				}
				err = d.deletePVCAndPV(pod)
				if err != nil {
					return VolumeCleanupError{Err: err} // this one is typed because we match it to a failure cause
				}
				return nil
			}
		}
	}
}

func (d *APICordonDrainer) awaitDeletion(pod *core.Pod, timeout time.Duration) error {
	err := wait.PollImmediate(1*time.Second, timeout, func() (bool, error) {
		got, err := d.c.CoreV1().Pods(pod.GetNamespace()).Get(pod.GetName(), meta.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, fmt.Errorf("cannot get pod %s/%s: %w", pod.GetNamespace(), pod.GetName(), err)
		}
		if got.GetUID() != pod.GetUID() {
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		if errors.Is(err, wait.ErrWaitTimeout) {
			return PodDeletionTimeoutError{} // this one is typed because we match it to a failure cause
		}
		return err // unexpected Get error above
	}
	return nil
}

func (d *APICordonDrainer) deletePVCAndPV(pod *core.Pod) error {
	pvcDeleted, err := d.deletePVCAssociatedWithStorageClass(pod)
	if err != nil {
		return err
	}

	// now if the pod is pending because it is missing the PVC and if it is controlled by a statefulset we should delete it to have statefulset controller rebuilding the PVC
	if len(pvcDeleted) > 0 {
		if err := d.deletePVAssociatedWithDeletedPVC(pod, pvcDeleted); err != nil {
			return err
		}
		for _, pvc := range pvcDeleted {
			if err := d.podDeleteRetryWaitingForPVC(pod, pvc); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *APICordonDrainer) podDeleteRetryWaitingForPVC(pod *core.Pod, pvc *core.PersistentVolumeClaim) error {
	podDeleteCheckPVCFunc := func() (bool, error) {
		// check if the PVC was created
		gotPVC, err := d.c.CoreV1().PersistentVolumeClaims(pvc.GetNamespace()).Get(pvc.GetName(), meta.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}

		if !apierrors.IsNotFound(err) {
			if gotPVC != nil && string(gotPVC.UID) != "" && string(gotPVC.UID) != string(pvc.UID) {
				d.l.Info("associated pvc was recreated", zap.String("pod", pod.GetName()), zap.String("namespace", pod.GetNamespace()), zap.String("pvc", pvc.GetName()), zap.String("pvc-old-uid", string(pvc.GetUID())), zap.String("pvc-new-uid", string(gotPVC.GetUID())))
				return true, nil
			}
		}

		d.l.Info("deleting pod to force pvc recreate", zap.String("pod", pod.GetName()), zap.String("namespace", pod.GetNamespace()))
		err = d.c.CoreV1().Pods(pod.GetNamespace()).Delete(pod.GetName(), &meta.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("cannot delete pod %s/%s to regenerated PVC: %w", pod.GetNamespace(), pod.GetName(), err)
		}
		return false, nil
	}
	return wait.PollImmediate(DefaultPodDeletePeriodWaitingForPVC, DefaultPVCRecreateTimeout, podDeleteCheckPVCFunc)
}

func (d *APICordonDrainer) deletePVAssociatedWithDeletedPVC(pod *core.Pod, pvcDeleted []*core.PersistentVolumeClaim) error {
	for _, claim := range pvcDeleted {
		if claim.Spec.VolumeName == "" {
			continue
		}
		pv, err := d.c.CoreV1().PersistentVolumes().Get(claim.Spec.VolumeName, meta.GetOptions{})
		if apierrors.IsNotFound(err) {
			d.l.Info("GET: PV not found", zap.String("name", claim.Spec.VolumeName), zap.String("claim", claim.Name), zap.String("claimNamespace", claim.Namespace))
			continue // This PV was already deleted
		}

		d.eventRecorder.Event(pv, core.EventTypeNormal, "Eviction", fmt.Sprintf("Deletion requested due to association with evicted pvc %s/%s and pod %s/%s", claim.Namespace, claim.Name, pod.Namespace, pod.Name))
		d.eventRecorder.Event(pod, core.EventTypeNormal, "Eviction", fmt.Sprintf("Deletion of associated PV %s", pv.Name))

		err = d.c.CoreV1().PersistentVolumes().Delete(pv.Name, nil)
		if apierrors.IsNotFound(err) {
			d.l.Info("DELETE: PV not found", zap.String("name", pv.Name))
			continue // This PV was already deleted
		}
		if err != nil {
			d.eventRecorder.Event(pv, core.EventTypeWarning, "EvictionFailure", fmt.Sprintf("Could not delete PV"))
			d.eventRecorder.Event(pod, core.EventTypeWarning, "EvictionFailure", fmt.Sprintf("Could not delete PV %s", pv.Name))
			return fmt.Errorf("cannot delete pv %s: %w", pv.Name, err)
		}
		d.l.Info("deleting pv", zap.String("pv", pv.Name))

		// wait for PVC complete deletion
		if err := d.awaitPVDeletion(pv, time.Minute); err != nil {
			return fmt.Errorf("pv deletion timeout %s: %w", pv.Name, err)
		}
	}
	return nil
}

func (d *APICordonDrainer) awaitPVDeletion(pv *core.PersistentVolume, timeout time.Duration) error {
	return wait.PollImmediate(1*time.Second, timeout, func() (bool, error) {
		got, err := d.c.CoreV1().PersistentVolumes().Get(pv.GetName(), meta.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, fmt.Errorf("cannot get pv %s: %w", pv.GetName(), err)
		}
		if string(got.GetUID()) != string(pv.GetUID()) {
			return true, nil
		}
		return false, nil
	})
}

// deletePVCAssociatedWithStorageClass takes care of deleting the PVCs associated with the annotated classes
// returns the list of deleted PVCs and the first error encountered if any
func (d *APICordonDrainer) deletePVCAssociatedWithStorageClass(pod *core.Pod) ([]*core.PersistentVolumeClaim, error) {
	if d.storageClassesAllowingPVDeletion == nil {
		return nil, nil
	}

	valAnnotation, _ := GetAnnotationFromPodOrController(PVCStorageClassCleanupAnnotationKey, pod, d.runtimeObjectStore)
	if valAnnotation != PVCStorageClassCleanupAnnotationValue {
		return nil, nil
	}

	deletedPVCs := []*core.PersistentVolumeClaim{}

	for _, v := range pod.Spec.Volumes {
		if v.PersistentVolumeClaim == nil {
			continue
		}
		d.l.Info("looking at volume with PVC", zap.String("name", v.Name), zap.String("claim", v.PersistentVolumeClaim.ClaimName))
		pvc, err := d.c.CoreV1().PersistentVolumeClaims(pod.GetNamespace()).Get(v.PersistentVolumeClaim.ClaimName, meta.GetOptions{})
		if apierrors.IsNotFound(err) {
			d.l.Info("GET: PVC not found", zap.String("name", v.Name), zap.String("claim", v.PersistentVolumeClaim.ClaimName))
			continue // This PVC was already deleted
		}
		if err != nil {
			return deletedPVCs, fmt.Errorf("cannot get pvc %s/%s: %w", pod.GetNamespace(), v.PersistentVolumeClaim.ClaimName, err)
		}
		if pvc.Spec.StorageClassName == nil {
			d.l.Info("PVC with no StorageClassName", zap.String("claim", v.PersistentVolumeClaim.ClaimName))
			continue
		}
		if _, ok := d.storageClassesAllowingPVDeletion[*pvc.Spec.StorageClassName]; !ok {
			d.l.Info("Skipping StorageClassName", zap.String("storageClassName", *pvc.Spec.StorageClassName))
			continue
		}

		d.eventRecorder.Event(pod, core.EventTypeNormal, "Eviction", fmt.Sprintf("Deletion of associated PVC %s/%s", pvc.Namespace, pvc.Name))
		d.eventRecorder.Event(pvc, core.EventTypeNormal, "Eviction", fmt.Sprintf("Deletion requested due to association with evicted pod %s/%s", pod.Namespace, pod.Name))

		err = d.c.CoreV1().PersistentVolumeClaims(pod.GetNamespace()).Delete(v.PersistentVolumeClaim.ClaimName, nil)
		if apierrors.IsNotFound(err) {
			d.l.Info("DELETE: PVC not found", zap.String("name", v.Name), zap.String("claim", v.PersistentVolumeClaim.ClaimName))
			continue // This PVC was already deleted
		}
		if err != nil {
			d.eventRecorder.Event(pod, core.EventTypeWarning, "EvictionFailure", fmt.Sprintf("Could not delete PVC %s/%s", pvc.Namespace, pvc.Name))
			return deletedPVCs, fmt.Errorf("cannot delete pvc %s/%s: %w", pod.GetNamespace(), v.PersistentVolumeClaim.ClaimName, err)
		}
		d.l.Info("deleting pvc", zap.String("pvc", v.PersistentVolumeClaim.ClaimName), zap.String("namespace", pod.GetNamespace()), zap.String("pvc-uid", string(pvc.GetUID())))

		// wait for PVC complete deletion
		if err := d.awaitPVCDeletion(pvc, time.Minute); err != nil {
			return deletedPVCs, fmt.Errorf("pvc deletion timeout %s/%s: %w", pod.GetNamespace(), v.PersistentVolumeClaim.ClaimName, err)
		}
		deletedPVCs = append(deletedPVCs, pvc)
	}
	return deletedPVCs, nil
}

func (d *APICordonDrainer) awaitPVCDeletion(pvc *core.PersistentVolumeClaim, timeout time.Duration) error {
	return wait.PollImmediate(1*time.Second, timeout, func() (bool, error) {
		d.l.Info("waiting for pvc complete deletion", zap.String("pvc", pvc.Name), zap.String("namespace", pvc.GetNamespace()), zap.String("pvc-uid", string(pvc.GetUID())))
		got, err := d.c.CoreV1().PersistentVolumeClaims(pvc.GetNamespace()).Get(pvc.GetName(), meta.GetOptions{})
		if apierrors.IsNotFound(err) {
			d.l.Info("pvc not found. It is deleted.", zap.String("pvc", pvc.Name), zap.String("namespace", pvc.GetNamespace()), zap.String("pvc-uid", string(pvc.GetUID())))
			return true, nil
		}
		if err != nil {
			return false, fmt.Errorf("cannot get pvc %s/%s: %w", pvc.GetNamespace(), pvc.GetName(), err)
		}
		if string(got.GetUID()) != string(pvc.GetUID()) {
			d.l.Info("pvc found but with different UID. It is deleted.", zap.String("pvc", pvc.Name), zap.String("namespace", pvc.GetNamespace()), zap.String("pvc-uid", string(pvc.GetUID())), zap.String("pvc-new-uid", string(got.GetUID())))
			return true, nil
		}
		d.l.Info("pvc still present", zap.String("pvc", pvc.Name), zap.String("namespace", pvc.GetNamespace()), zap.String("pvc-uid", string(pvc.GetUID())))
		return false, nil
	})
}

func (d *APICordonDrainer) performNodeReplacement(n *core.Node, reason string, withRateLimiting bool) (NodeReplacementStatus, error) {
	nodeFromStore, err := d.runtimeObjectStore.Nodes().Get(n.Name)
	if err != nil {
		return NodeReplacementStatusNone, err
	}

	replacementValue, ok := nodeFromStore.Labels[NodeLabelKeyReplaceRequest]
	if !ok {
		if withRateLimiting && !d.nodeReplacementLimiter.CanAskForNodeReplacement() {
			nr := &core.ObjectReference{Kind: "Node", Name: n.GetName(), UID: types.UID(n.GetName())}
			d.eventRecorder.Event(nr, core.EventTypeNormal, "NodeReplacementLimited", "Node replacement is blocked by rate limited for the moment.")
			return NodeReplacementStatusBlockedByLimiter, nil
		}

		fresh, err := d.c.CoreV1().Nodes().Get(n.GetName(), meta.GetOptions{})
		if err != nil {
			return NodeReplacementStatusNone, fmt.Errorf("cannot get node %s: %w", n.GetName(), err)
		}
		fresh.Labels[NodeLabelKeyReplaceRequest] = NodeLabelValueReplaceRequested
		if _, err := d.c.CoreV1().Nodes().Update(fresh); err != nil {
			return NodeReplacementStatusNone, fmt.Errorf("cannot request replacement node %s: %w", fresh.GetName(), err)
		}
		tags, _ := tag.New(context.Background(), tag.Upsert(TagNodeName, n.GetName()), tag.Upsert(TagReason, reason)) // nolint:gosec
		StatRecordForNode(tags, n, MeasureNodesReplacementRequest.M(1))
		return NodeReplacementStatusRequested, nil
	}

	switch replacementValue {
	case NodeLabelValueReplaceRequested, NodeLabelValueReplaceProcessing:
		return NodeReplacementStatusRequested, nil
	case NodeLabelValueReplaceDone:
		return NodeReplacementStatusDone, nil
	default:
		return NodeReplacementStatusNone, nil
	}
}

func (d *APICordonDrainer) ReplaceNode(n *core.Node) (NodeReplacementStatus, error) {
	return d.performNodeReplacement(n, newNodeRequestReasonReplacement, true)
}

func (d *APICordonDrainer) PreprovisionNode(n *core.Node) (NodeReplacementStatus, error) {
	return d.performNodeReplacement(n, newNodeRequestReasonPreprovisioning, false)
}

var evictionPayloadEncoder runtime.Encoder

func GetEvictionPayloadEncoder() runtime.Encoder {
	if evictionPayloadEncoder != nil {
		return evictionPayloadEncoder
	}
	scheme := runtime.NewScheme()
	policy.SchemeBuilder.AddToScheme(scheme)
	codecFactory := serializer.NewCodecFactory(scheme)
	jsonSerializer := runtimejson.NewSerializerWithOptions(runtimejson.DefaultMetaFactory, scheme, scheme, runtimejson.SerializerOptions{})
	evictionPayloadEncoder = codecFactory.WithoutConversion().EncoderForVersion(jsonSerializer, policy.SchemeGroupVersion)
	return evictionPayloadEncoder
}

func GetEvictionJsonPayload(obj *policy.Eviction) *bytes.Buffer {
	buffer := bytes.NewBuffer([]byte{})
	GetEvictionPayloadEncoder().Encode(obj, buffer)
	return buffer
}
