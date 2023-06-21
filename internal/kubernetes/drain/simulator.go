package drain

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"

	"github.com/DataDog/compute-go/logs"
	"github.com/go-logr/logr"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/analyser"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	"github.com/planetlabs/draino/internal/kubernetes/utils"
	"github.com/planetlabs/draino/internal/limit"
)

const (
	PositiveCacheResTTL = time.Minute
	NegativeCacheResTTL = 3 * time.Minute

	eventDrainSimulationFailed    = "DrainSimulationFailed"
	eventEvictionSimulationFailed = "EvictionSimulationFailed"
)

type DrainSimulator interface {
	// SimulateDrain will simulate a drain for the given node.
	// This means that it will perform an eviction simulation of all pods running on the node.
	SimulateDrain(context.Context, *corev1.Node) (canEvict bool, reasons []string, err []error)
	// SimulatePodDrain will simulate a drain of the given pod.
	// Before calling the API server it will make sure that some of the obvious problems are not given.
	SimulatePodDrain(context.Context, *corev1.Pod, *corev1.Node) (canEvict bool, reason string, err error)
}

type drainSimulatorImpl struct {
	pdbIndexer         index.PDBIndexer
	podIndexer         index.PodIndexer
	client             client.Client
	eventRecorder      kubernetes.EventRecorder
	rateLimiter        limit.RateLimiter
	logger             logr.Logger
	runtimeObjectStore kubernetes.RuntimeObjectStore
	globalConfig       kubernetes.GlobalConfig
	// skipPodFilter will be used to evaluate if pods running on a node should go through the eviction simulation
	skipPodFilter  kubernetes.PodFilterFunc
	podResultCache utils.TTLCache[simulationResult]
}

type simulationResult struct {
	result bool
	reason string
	err    error
}

var _ DrainSimulator = &drainSimulatorImpl{}

func NewDrainSimulator(
	ctx context.Context,
	client client.Client,
	indexer *index.Indexer,
	skipPodFilter kubernetes.PodFilterFunc,
	eventRecorder kubernetes.EventRecorder,
	rateLimiter limit.RateLimiter,
	logger logr.Logger,
	runtimeObjectStore kubernetes.RuntimeObjectStore,
	globalConfig kubernetes.GlobalConfig,
) DrainSimulator {
	simulator := &drainSimulatorImpl{
		podIndexer:         indexer,
		pdbIndexer:         indexer,
		client:             client,
		skipPodFilter:      skipPodFilter,
		eventRecorder:      eventRecorder,
		rateLimiter:        rateLimiter,
		logger:             logger.WithName("EvictionSimulator"),
		runtimeObjectStore: runtimeObjectStore,
		globalConfig:       globalConfig,
		// TODO think about using alternative solutions like a MRU cache
		podResultCache: utils.NewTTLCache[simulationResult](3*time.Minute, 10*time.Second),
	}

	go simulator.podResultCache.StartCleanupLoop(ctx)

	return simulator
}

func (sim *drainSimulatorImpl) SimulateDrain(ctx context.Context, node *corev1.Node) (bool, []string, []error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "SimulateNodeDrain")
	defer span.Finish()

	pods, err := sim.podIndexer.GetPodsByNode(ctx, node.GetName())
	if err != nil {
		return false, nil, []error{err}
	}

	// stable pod sort for stable reason / error construction
	sort.Slice(pods, func(i, j int) bool {
		podI := pods[i]
		podJ := pods[j]
		return podI.Namespace+"/"+podI.Name < podJ.Namespace+"/"+podJ.Name
	})

	// As we are  caching the positive results for one minute and negative ones for three minutes, we might make a lot of unneeded API calls
	// As an optimization we are iterating over all pods and check if at least one has a negative cache entry, before simulating the drain for all the pods.
	var reasons []string
	var errors []error
	for _, pod := range pods {
		if res, exist := sim.podResultCache.Get(createCacheKey(pod), time.Now()); exist && !res.result {
			reasons = append(reasons, sim.nodeReasonFromPodReason(pod, res.reason))
			if res.err != nil {
				errors = append(errors, res.err)
			}
		}
	}
	if len(reasons) > 0 || len(errors) > 0 {
		sim.eventRecorder.NodeEventf(ctx, node, corev1.EventTypeWarning, eventDrainSimulationFailed, "Drain simulation failed: "+strings.Join(reasons, "; "))
		return false, reasons, errors
	}

	for _, pod := range pods {
		// TODO add suceeded/failed pod drain simulation count metric
		canEvict, reason, err := sim.SimulatePodDrain(ctx, pod, node)
		if !canEvict {
			reasons = append(reasons, sim.nodeReasonFromPodReason(pod, reason))
			if err != nil {
				errors = append(errors, err)
			}
		}
	}

	// TODO add suceeded/failed node drain simulation count metric
	if len(reasons) > 0 {
		sim.eventRecorder.NodeEventf(ctx, node, corev1.EventTypeWarning, eventDrainSimulationFailed, "Drain simulation failed: "+strings.Join(reasons, "; "))
		return false, reasons, errors
	}

	return true, nil, errors
}

func (sim *drainSimulatorImpl) nodeReasonFromPodReason(pod *corev1.Pod, reason string) string {
	return fmt.Sprintf("Cannot drain pod '%s/%s', because: %v", pod.GetNamespace(), pod.GetName(), reason)
}

func (sim *drainSimulatorImpl) SimulatePodDrain(ctx context.Context, pod *corev1.Pod, node *corev1.Node) (bool, string, error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "SimulatePodDrain")
	defer span.Finish()

	if res, exist := sim.podResultCache.Get(createCacheKey(pod), time.Now()); exist {
		return res.result, res.reason, res.err
	}

	passes, reason, err := sim.skipPodFilter(*pod)
	if err != nil {
		return false, reason, err
	}
	if !passes {
		// If the pod does not pass the filter, it means that it will be accepted by default
		sim.writePodCache(pod, true, reason, nil)
		return true, reason, nil
	}

	// if using eviction++ but not opted-in for dry-run, pass simulation early
	// once all teams are opted-in and dry-run is required, this can be removed
	if !sim.operatorAPIDryRunEnabled(pod) {
		return true, "", nil
	}

	// if eviction++ is used, skip pdb checks
	if !sim.usesOperatorAPI(pod) {
		pdbs, err := sim.pdbIndexer.GetPDBsForPods(ctx, []*corev1.Pod{pod})
		if err != nil {
			return false, "", err
		}

		// If there is more than one PDB associated to the given pod, the eviction will fail for sure due to the APIServer behaviour.
		podKey := index.GeneratePodIndexKey(pod.GetName(), pod.GetNamespace())
		if len(pdbs[podKey]) > 1 {
			reason = fmt.Sprintf("Pod has more than one associated PDB: %s", strings.Join(utils.GetPDBNames(pdbs[podKey]), ";"))
			sim.writePodCache(pod, false, reason, nil)
			sim.eventRecorder.PodEventf(ctx, pod, corev1.EventTypeWarning, eventEvictionSimulationFailed, reason)
			return false, reason, nil
		}

		// If there is a matching PDB, check if it would allow disruptions
		if len(pdbs[podKey]) == 1 {
			pdb := pdbs[podKey][0]
			if analyser.IsPDBBlockedByPod(ctx, pod, pdb) {
				reason = fmt.Sprintf("PDB '%s' does not allow any disruptions", pdb.GetName())
				sim.writePodCache(pod, false, reason, nil)
				sim.eventRecorder.PodEventf(ctx, pod, corev1.EventTypeWarning, eventEvictionSimulationFailed, reason)
				return false, reason, nil
			}
		}
	}

	if !sim.rateLimiter.TryAccept() {
		sim.logger.V(logs.ZapDebug).Info("Drain simulation aborted due to rate limiting.")
		return false, "", &k8sclient.ClientSideRateLimit{}
	}

	// do a dry-run eviction call
	evictionDryRunRes, err := sim.simulateAPIEviction(ctx, pod, node)
	if !evictionDryRunRes {
		reason = fmt.Sprintf("Eviction dry run was not successful: %v", err)
		if apierrors.IsForbidden(err) { // This is the admission that is rejecting the drain. The error carry the reason for the rejection
			err = nil
		}
		// Too many requests means either we are rate limited (what's expected in some cases) or that the eviction was rejected by the apiserver.
		// In both cases we don't want to treat it as an error, because it's somewhat expected behaviour.
		if apierrors.IsTooManyRequests(err) {
			err = nil
		}
		sim.writePodCache(pod, false, reason, err)
		sim.eventRecorder.PodEventf(ctx, pod, corev1.EventTypeWarning, eventEvictionSimulationFailed, reason)
		return false, reason, err
	}

	sim.writePodCache(pod, true, "", nil)
	return true, "", nil
}

func (sim *drainSimulatorImpl) simulateAPIEviction(ctx context.Context, pod *corev1.Pod, node *corev1.Node) (bool, error) {
	evictionAPIURL, ok := kubernetes.GetAnnotationFromPodOrController(kubernetes.EvictionAPIURLAnnotationKey, pod, sim.runtimeObjectStore)
	if ok {
		return sim.simulateWithOperatorAPI(ctx, evictionAPIURL, pod, node)
	}
	return sim.simulateWithKubernetesAPI(ctx, pod)
}

// return false if the pod uses eviction++ and is not opted-in for eviction++ dry-run
// these pods should pass simulation right away without doing any checks
// once all teams are opted-in and dry-run is required, this can be removed
func (sim *drainSimulatorImpl) operatorAPIDryRunEnabled(pod *corev1.Pod) bool {
	if sim.usesOperatorAPI(pod) {
		_, ok := kubernetes.GetAnnotationFromPodOrController(kubernetes.EvictionAPIDryRunSupportedAnnotationKey, pod, sim.runtimeObjectStore)
		return !ok
	}
	return true
}

func (sim *drainSimulatorImpl) usesOperatorAPI(pod *corev1.Pod) bool {
	_, ok := kubernetes.GetAnnotationFromPodOrController(kubernetes.EvictionAPIURLAnnotationKey, pod, sim.runtimeObjectStore)
	return ok
}

func (sim *drainSimulatorImpl) simulateWithKubernetesAPI(ctx context.Context, pod *corev1.Pod) (bool, error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "SimulatePodEvictionWithKubernetesAPI")
	defer span.Finish()

	eviction := &policyv1.Eviction{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.GetName(),
			Namespace: pod.GetNamespace(),
		},
		DeleteOptions: &metav1.DeleteOptions{
			DryRun: []string{"All"},
		},
	}
	err := sim.client.SubResource("eviction").Create(ctx, pod, eviction)
	if err != nil {
		sim.logger.V(logs.ZapDebug).Info("Error returned by simulation eviction", "pod", pod.Namespace+"/"+pod.Name, "err", err, "IsTooManyReq", apierrors.IsTooManyRequests(err), "IsForbidden", apierrors.IsForbidden(err), "Reason", apierrors.ReasonForError(err))
		return false, fmt.Errorf("Cannot evict pod '%s/%s': %w", pod.Namespace, pod.Name, err)
	}

	return true, nil
}

func (sim *drainSimulatorImpl) simulateWithOperatorAPI(ctx context.Context, evictionAPIUrl string, pod *corev1.Pod, node *corev1.Node) (bool, error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "SimulatePodEvictionWithOperatorAPI")
	defer span.Finish()

	conditions := kubernetes.GetConditionsTypes(kubernetes.GetNodeOffendingConditions(node, sim.globalConfig.SuppliedConditions))
	_, err := kubernetes.CallOperatorAPI(ctx, sim.logger, evictionAPIUrl, pod, conditions, true, 1)
	if err != nil {
		sim.logger.V(logs.ZapDebug).Info("Error returned by simulation eviction", "pod", pod.Namespace+"/"+pod.Name, "err", err, "IsTooManyReq", apierrors.IsTooManyRequests(err), "IsForbidden", apierrors.IsForbidden(err), "Reason", apierrors.ReasonForError(err))
		return false, fmt.Errorf("Cannot evict pod '%s/%s': %w", pod.Namespace, pod.Name, err)
	}

	return true, nil
}

func (sim *drainSimulatorImpl) writePodCache(pod *corev1.Pod, result bool, reason string, err error) {
	ttl := NegativeCacheResTTL
	if result {
		ttl = PositiveCacheResTTL
	}
	sim.podResultCache.AddCustomTTL(createCacheKey(pod), simulationResult{result: result, reason: reason, err: err}, ttl)
}

func createCacheKey(pod *corev1.Pod) string {
	return string(pod.UID)
}
