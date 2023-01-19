package groups

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type GroupFromPod struct {
	kclient   client.Client
	logger    logr.Logger
	keyGetter GroupKeyGetter
	podFilter kubernetes.PodFilterFunc

	podToNodeCache cache.ThreadSafeStore // map[podNS/podName]NodeName, used to report on node in case of pod deletion

	hasSyncedFunc func() bool
}

func NewGroupFromPod(
	kclient client.Client,
	logger logr.Logger,
	keyGetter GroupKeyGetter,
	podFilter kubernetes.PodFilterFunc,
	hasSyncedFunc func() bool,
) *GroupFromPod {
	return &GroupFromPod{
		kclient:        kclient,
		logger:         logger,
		keyGetter:      keyGetter,
		podFilter:      podFilter,
		hasSyncedFunc:  hasSyncedFunc,
		podToNodeCache: cache.NewThreadSafeStore(nil, nil),
	}
}

// Reconcile register the pod to report possible group override
func (r *GroupFromPod) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if !r.hasSyncedFunc() {
		return ctrl.Result{
			Requeue:      true,
			RequeueAfter: 5 * time.Second,
		}, nil
	}
	pod := &v1.Pod{}
	if err := r.kclient.Get(ctx, req.NamespacedName, pod); err != nil {
		if errors.IsNotFound(err) {
			if nodeName, ok := r.podToNodeCache.Get(req.String()); ok {
				errKG := r.notifyKeyGetter(ctx, nodeName.(string))
				if errKG != nil {
					return ctrl.Result{}, errKG
				}
				r.podToNodeCache.Delete(req.String())
			}
			return reconcile.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get Pod Fails: %v", err)
	}
	if pass, _, errFilter := r.podFilter(*pod); !pass {
		if errFilter != nil {
			r.logger.Error(errFilter, "failed to filter pods")
		}
		return ctrl.Result{}, nil
	}

	nodeName := pod.Spec.NodeName
	if nodeName == "" {
		return ctrl.Result{}, nil
	}
	r.podToNodeCache.Add(req.String(), nodeName)
	return ctrl.Result{}, r.notifyKeyGetter(ctx, nodeName)
}

func (r *GroupFromPod) notifyKeyGetter(ctx context.Context, nodeName string) error {
	node := &v1.Node{}
	if err := r.kclient.Get(ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("get Node Fails: %v", err)
	}

	return r.keyGetter.UpdatePodGroupOverrideAnnotation(ctx, node)
}

// SetupWithManager setups the controller with goroutine and predicates
func (r *GroupFromPod) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).WithOptions(controller.Options{MaxConcurrentReconciles: 5}).
		For(&v1.Pod{}).
		WithEventFilter(
			predicate.Funcs{
				CreateFunc:  func(evt event.CreateEvent) bool { return true },
				DeleteFunc:  func(event.DeleteEvent) bool { return true },
				GenericFunc: func(event.GenericEvent) bool { return false },
				UpdateFunc:  func(evt event.UpdateEvent) bool { return true },
			},
		).
		Complete(r)
}
