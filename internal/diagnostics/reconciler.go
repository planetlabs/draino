package diagnostics

import (
	"context"
	"encoding/json"
	"fmt"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/client-go/util/workqueue"
	"time"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	ctrl "sigs.k8s.io/controller-runtime"
)

type Reconciler struct {
	kclient       client.Client
	logger        logr.Logger
	eventRecorder kubernetes.EventRecorder

	diagnosticWriters []Diagnostician

	nodeDiagnosticsLimiter flowcontrol.RateLimiter // client side protection for APIServer

	hasSyncedFunc func() bool
}

func NewDiagnosticsController(ctx context.Context,
	kclient client.Client,
	logger logr.Logger,
	eventRecorder kubernetes.EventRecorder,
	diagnosticWriters []Diagnostician,
	hasSyncedFunc func() bool,
) *Reconciler {
	diagnosticsReconciler := &Reconciler{
		kclient:                kclient,
		logger:                 logger.WithName("diagnosticsReconciler"),
		diagnosticWriters:      diagnosticWriters,
		eventRecorder:          eventRecorder,
		hasSyncedFunc:          hasSyncedFunc,
		nodeDiagnosticsLimiter: flowcontrol.NewTokenBucketRateLimiter(20, 10), // client side protection
	}
	return diagnosticsReconciler
}

// Reconcile register the node in the reverse index per ProviderIP
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if !r.hasSyncedFunc() {
		return ctrl.Result{
			Requeue:      true,
			RequeueAfter: 5 * time.Second,
		}, nil
	}
	node := &v1.Node{}
	if err := r.kclient.Get(ctx, req.NamespacedName, node); err != nil {
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get Node Fails: %v", err)
	}

	if node.Annotations != nil {
		if v, ok := node.Annotations[nodeDiagnosticAnnotationKey]; ok && v == "" {
			if r.nodeDiagnosticsLimiter.TryAccept() {
				results := make(map[string]interface{}, len(r.diagnosticWriters)+1)
				for _, w := range r.diagnosticWriters {
					results[w.GetName()] = w.GetNodeDiagnostic(ctx, node.Name)
				}
				results["Date"] = time.Now().Format(time.RFC3339)
				output, errJson := json.Marshal(results)
				if errJson != nil {
					r.logger.Error(errJson, "Failed to serialize diagnostics", "node", node.Name)
					return ctrl.Result{}, errJson
				}
				if err := kubernetes.PatchNodeAnnotationKeyCR(ctx, r.kclient, node, nodeDiagnosticAnnotationKey, string(output)); err != nil {
					if errors.IsNotFound(err) {
						return ctrl.Result{}, nil // the node was deleted, no more need for update.
					}
					r.logger.Error(err, "Failed to update annotation for diagnostics", "node", node.Name)
					return ctrl.Result{}, err
				}
			}
		}
	}
	return ctrl.Result{}, nil
}

// SetupWithManager setups the controller with goroutine and predicates
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).WithOptions(controller.Options{
		MaxConcurrentReconciles: 2,
		RateLimiter:             workqueue.NewItemExponentialFailureRateLimiter(500*time.Millisecond, 20*time.Second)}).
		For(&v1.Node{}).
		WithEventFilter(
			predicate.Funcs{
				CreateFunc: func(evt event.CreateEvent) bool {
					return true
				},
				DeleteFunc:  func(event.DeleteEvent) bool { return false },
				GenericFunc: func(event.GenericEvent) bool { return false },
				UpdateFunc: func(evt event.UpdateEvent) bool {
					return true
				},
			},
		).
		Complete(r)
}
