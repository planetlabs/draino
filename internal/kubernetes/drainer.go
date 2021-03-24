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
	"strings"
	"time"

	"errors"
	"go.uber.org/zap"
	core "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
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

	NodeLabelKeyReplaceRequest      = "node.datadoghq.com/replace"
	NodeLabelValueReplaceRequested  = "requested"
	NodeLabelValueReplaceProcessing = "processing"
	NodeLabelValueReplaceDone       = "done"
)

type nodeMutatorFn func(*core.Node)

type PodEvictionTimeoutError struct {
}

func (e PodEvictionTimeoutError) Error() string {
	return "timed out waiting for pod disruption to be allowed"
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
	GetPodsToDrain(node string, podStore PodStore) ([]*core.Pod, error)
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
}

// A CordonDrainer both cordons and drains nodes!
type CordonDrainer interface {
	Cordoner
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
func (d *NoopCordonDrainer) MarkDrain(_ *core.Node, _, _ time.Time, _ bool) error {
	return nil
}

// ReplaceNode return none
func (d *NoopCordonDrainer) ReplaceNode(n *core.Node) (NodeReplacementStatus, error) {
	return NodeReplacementStatusNone, nil
}

var _ CordonDrainer = &APICordonDrainer{}

// APICordonDrainer drains Kubernetes nodes via the Kubernetes API.
type APICordonDrainer struct {
	c             kubernetes.Interface
	l             *zap.Logger
	eventRecorder record.EventRecorder

	filter                 PodFilterFunc
	cordonLimiter          CordonLimiter
	nodeReplacementLimiter NodeReplacementLimiter

	maxGracePeriod   time.Duration
	evictionHeadroom time.Duration
	skipDrain        bool

	storageClassesAllowingPVDeletion map[string]struct{}
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
		eventRecorder:    NewEventRecorder(c),
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

// MarkDrain set a condition on the node to mark that that drain is scheduled.
func (d *APICordonDrainer) MarkDrain(n *core.Node, when, finish time.Time, failed bool) error {
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
		freshNode.Status.Conditions[i].LastHeartbeatTime = now
		freshNode.Status.Conditions[i].Message = "Drain activity scheduled " + when.Format(time.RFC3339) + msgSuffix
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
				drainStatus.Failed = true
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
				return fmt.Errorf("cannot evict all pods: %w", err)
			}
		case <-deadline:
			return PodEvictionTimeoutError{}
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

func (d *APICordonDrainer) evict(pod *core.Pod, abort <-chan struct{}, e chan<- error) {
	gracePeriod := int64(d.maxGracePeriod.Seconds())
	if pod.Spec.TerminationGracePeriodSeconds != nil && *pod.Spec.TerminationGracePeriodSeconds < gracePeriod {
		gracePeriod = *pod.Spec.TerminationGracePeriodSeconds
	}
	for {
		select {
		case <-abort:
			e <- errors.New("pod eviction aborted")
			return
		default:
			err := d.c.CoreV1().Pods(pod.GetNamespace()).Evict(&policy.Eviction{
				ObjectMeta:    meta.ObjectMeta{Namespace: pod.GetNamespace(), Name: pod.GetName()},
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
				e <- fmt.Errorf("cannot evict pod %s/%s: %w", pod.GetNamespace(), pod.GetName(), err)
				return
			default:
				err := d.awaitDeletion(pod, d.deleteTimeout())
				if err != nil {
					err = fmt.Errorf("cannot confirm pod %s/%s was deleted: %w", pod.GetNamespace(), pod.GetName(), err)
				}
				e <- err
				return
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
		return fmt.Errorf("at least one PVC associated to that pod was deleted before failure/timeout: %w", err)
	}

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
		if err != nil {
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

	valAnnotation := pod.Annotations[PVCStorageClassCleanupAnnotationKey]
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

func (d *APICordonDrainer) ReplaceNode(n *core.Node) (NodeReplacementStatus, error) {
	replacementValue, ok := n.Labels[NodeLabelKeyReplaceRequest]
	if !ok {
		if !d.nodeReplacementLimiter.CanAskForNodeReplacement() {
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
