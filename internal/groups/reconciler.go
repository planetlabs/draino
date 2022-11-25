package groups

import (
	"context"
	"fmt"
	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	nodeWarmUpDelay = 30 * time.Second

	// if the groupKey validation fails, and event will be added on the node with the reason
	eventGroupOverrideMisconfiguration = "GroupOverrideBadConfiguration"
)

type GroupRegistry struct {
	kclient       client.Client
	logger        logr.Logger
	eventRecorder kubernetes.EventRecorder

	nodeWarmUpDelay time.Duration
	keyGetter       GroupKeyGetter
	groupRunner     *GroupsRunner
}

func NewGroupRegistry(ctx context.Context, kclient client.Client, logger logr.Logger, eventRecorder kubernetes.EventRecorder, factory RunnerFactory) *GroupRegistry {
	return &GroupRegistry{
		kclient:         kclient,
		logger:          logger,
		nodeWarmUpDelay: nodeWarmUpDelay,
		keyGetter:       factory.GroupKeyGetter(),
		groupRunner:     NewGroupsRunner(ctx, factory, logger),
		eventRecorder:   eventRecorder,
	}
}

// Reconcile register the node in the reverse index per ProviderIP
func (r *GroupRegistry) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	node := &v1.Node{}
	if err := r.kclient.Get(ctx, req.NamespacedName, node); err != nil {
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get Node Fails: %v", err)
	}

	if valid, reason := r.keyGetter.ValidateGroupKey(node); !valid {
		r.eventRecorder.NodeEventf(ctx, node, v1.EventTypeWarning, eventGroupOverrideMisconfiguration, reason)
		r.logger.Info(eventGroupOverrideMisconfiguration+": default applies, no group override", "node", node.Name)
	}

	r.groupRunner.RunForGroup(r.keyGetter.GetGroupKey(node))

	return ctrl.Result{}, nil
}

// SetupWithManager setups the controller with goroutine and predicates
func (r *GroupRegistry) SetupWithManager(mgr ctrl.Manager) error {

	initSchedulingGroupIndexer(r.kclient, mgr.GetCache(), r.keyGetter)

	return ctrl.NewControllerManagedBy(mgr).WithOptions(controller.Options{MaxConcurrentReconciles: 2}).
		For(&v1.Node{}).
		WithEventFilter(
			predicate.Funcs{
				CreateFunc:  func(event.CreateEvent) bool { return false }, // to early in the process the node might not be complete
				DeleteFunc:  func(event.DeleteEvent) bool { return false }, // we don't care about delete, the runner will stop if the groups is empty
				GenericFunc: func(event.GenericEvent) bool { return false },
				UpdateFunc: func(evt event.UpdateEvent) bool {
					if time.Now().Sub(evt.ObjectNew.GetCreationTimestamp().Time) < r.nodeWarmUpDelay {
						return false // to early in the process the node might not be complete
					}
					return true
				},
			},
		).
		Complete(r)
}
