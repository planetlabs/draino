package kubernetes

import (
	"time"

	"github.com/pkg/errors"
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

	kindDaemonSet = "DaemonSet"
)

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

// A FilterFunc returns true if the supplied pod passes the filter.
type FilterFunc func(p core.Pod) (bool, error)

// MirrorPodFilter returns true if the supplied pod is not a mirror pod, i.e. a
// pod created by a manifest on the node rather than the API server.
func MirrorPodFilter(p core.Pod) (bool, error) {
	_, mirrorPod := p.GetAnnotations()[core.MirrorPodAnnotationKey]
	return !mirrorPod, nil
}

// NewDaemonSetPodFilter returns a FilterFunc that returns true if the supplied
// pod is not managed by an extant DaemonSet.
func NewDaemonSetPodFilter(client kubernetes.Interface) FilterFunc {
	return func(p core.Pod) (bool, error) {
		c := meta.GetControllerOf(&p)
		if c == nil || c.Kind != kindDaemonSet {
			return true, nil
		}

		// Pods pass the filter if they were created by a DaemonSet that no
		// longer exists.
		if _, err := client.ExtensionsV1beta1().DaemonSets(p.GetNamespace()).Get(c.Name, meta.GetOptions{}); err != nil {
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			return false, errors.Wrapf(err, "cannot get DaemonSet %s/%s", p.GetNamespace(), c.Name)
		}
		return false, nil
	}
}

// NewPodFilters returns a FilterFunc that returns true if all of the supplied
// FilterFuncs return true.
func NewPodFilters(filters ...FilterFunc) FilterFunc {
	return func(p core.Pod) (bool, error) {
		for _, fn := range filters {
			passes, err := fn(p)
			if err != nil {
				return false, errors.Wrap(err, "cannot apply filters")
			}
			if !passes {
				return false, nil
			}
		}
		return true, nil
	}
}

// A Cordoner cordons nodes.
type Cordoner interface {
	// Cordon the supplied node. Marks it unschedulable for new pods.
	Cordon(n *core.Node) error
}

// A Drainer drains nodes.
type Drainer interface {
	// Drain the supplied node. Evicts the node of all but mirror and DaemonSet pods.
	Drain(n *core.Node) error
}

// A CordonDrainer both cordons and drains nodes!
type CordonDrainer interface {
	Cordoner
	Drainer
}

// A NoopCordonDrainer does nothing.
type NoopCordonDrainer struct{}

// Cordon does nothing.
func (d *NoopCordonDrainer) Cordon(n *core.Node) error { return nil }

// Drain does nothing.
func (d *NoopCordonDrainer) Drain(n *core.Node) error { return nil }

// APICordonDrainer drains Kubernetes nodes via the Kubernetes API.
type APICordonDrainer struct {
	c kubernetes.Interface

	filter FilterFunc

	maxGracePeriod   time.Duration
	evictionHeadroom time.Duration
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

// WithPodFilters configures filters that may be used to exclude certain pods
// from eviction when draining.
func WithPodFilter(f FilterFunc) APICordonDrainerOption {
	return func(d *APICordonDrainer) {
		d.filter = f
	}
}

// NewAPICordonDrainer returns a CordonDrainer that cordons and drains nodes via
// the Kubernetes API.
func NewAPICordonDrainer(c kubernetes.Interface, ao ...APICordonDrainerOption) *APICordonDrainer {
	d := &APICordonDrainer{c: c, maxGracePeriod: DefaultMaxGracePeriod, evictionHeadroom: DefaultEvictionOverhead}
	for _, o := range ao {
		o(d)
	}
	return d
}

func (d *APICordonDrainer) deleteTimeout() time.Duration {
	return d.maxGracePeriod + d.evictionHeadroom
}

// Cordon the supplied node. Marks it unschedulable for new pods.
func (d *APICordonDrainer) Cordon(n *core.Node) error {
	fresh, err := d.c.CoreV1().Nodes().Get(n.GetName(), meta.GetOptions{})
	if err != nil {
		return errors.Wrapf(err, "cannot get node %s", n.GetName())
	}
	if fresh.Spec.Unschedulable {
		return nil
	}
	fresh.Spec.Unschedulable = true
	if _, err := d.c.CoreV1().Nodes().Update(fresh); err != nil {
		return errors.Wrapf(err, "cannot cordon node %s", fresh.GetName())
	}
	return nil
}

// Drain the supplied node. Evicts the node of all but mirror and DaemonSet pods.
func (d *APICordonDrainer) Drain(n *core.Node) error {
	pods, err := d.getPods(n.GetName())
	if err != nil {
		return errors.Wrapf(err, "cannot get pods for node %s", n.GetName())
	}

	abort := make(chan struct{})
	errs := make(chan error, 1)
	for _, pod := range pods {
		go d.evict(pod, abort, errs)
	}

	deadline := time.After(d.deleteTimeout())
	for range pods {
		select {
		case err := <-errs:
			if err != nil {
				return errors.Wrap(err, "cannot evict pods")
			}
		case <-deadline:
			// This will _eventually_ abort evictions. Evictions may spend up to
			// d.deleteTimeout() in d.awaitDeletion(), or 5 seconds in backoff
			// before noticing they've been aborted.
			close(abort)
			return errors.Wrap(errTimeout{}, "timed out waiting for eviction to complete")
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
