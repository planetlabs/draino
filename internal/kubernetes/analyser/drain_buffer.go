package analyser

import (
	"context"
	"fmt"
	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sort"
	"strings"
	"time"
)

const (
	DrainBufferMisconfigured = "DrainBufferMisconfigured"

	DefaultEstimatedRecoveryRecordTTL = 15 * time.Minute
	DefaultCacheCleanPeriod           = 3 * time.Minute
	DefaultDrainBufferLength          = 3 * time.Minute
)

// DrainBufferChecker, can a node be drain and respect drain buffer configuration
type DrainBufferChecker interface {
	// DrainBufferAcceptsDrain check if the node can be drained, would we respect all the drain buffer set on the node or on the pods
	// the time value passed should represent "Now" on a live system. We are passing it to allow simulation (future) and ease unittesting
	DrainBufferAcceptsDrain(context.Context, *v1.Node, time.Time) bool
}

// drainBufferChecker, implementation for DrainBufferChecker
// It retrieves the configuration for drain-buffer on node and pods. It compares this with the pdb transition changes.
type drainBufferChecker struct {
	logger            logr.Logger
	kclient           client.Client
	eventRecorder     kubernetes.EventRecorder
	store             kubernetes.RuntimeObjectStore
	indexer           index.Indexer
	drainBufferConfig DrainBufferCheckerConfiguration

	// cacheRecoveryTime for a combination {Node+Pods} this cache store the estimated recoveryTime
	// Note the if a pod is delete from the node the key can't be used anymore, so that cacheclean will remove this key when the TTL expire
	cacheRecoveryTime cache.ThreadSafeStore
}

var _ DrainBufferChecker = &drainBufferChecker{}

// drainBufferInfo, the drain buffer configuration can be set on the node, on the pod or the controller of the pod.
// this structure allow us to carry the value and the source of the configuration
type drainBufferInfo struct {
	length     time.Duration
	sourceType string
	sourceName string
}

// recoveryEstimationDate is used as value in the cache cacheRecoveryTime
type recoveryEstimationDate struct {
	estimatedRecovery time.Time
	ttl               time.Time
}

// DrainBufferCheckerConfiguration Configuration used in the constructor of DrainBufferChecker
type DrainBufferCheckerConfiguration struct {
	// DefaultDrainBuffer, default value to be used if no configuration is given on pods or nodes
	DefaultDrainBuffer *time.Duration
	// EstimatedRecoveryRecordTTL, TTL value to be used to remove the record from the cache
	EstimatedRecoveryRecordTTL *time.Duration
	// CacheCleanupPeriod, how often we should attempt the cache cleanup
	CacheCleanupPeriod *time.Duration
}

func defaultDuration(d **time.Duration, duration time.Duration) {
	if *d == nil {
		defaultD := duration
		*d = &defaultD
	}
}

func (c *DrainBufferCheckerConfiguration) applyDefault() {
	defaultDuration(&c.DefaultDrainBuffer, DefaultDrainBufferLength)
	defaultDuration(&c.EstimatedRecoveryRecordTTL, DefaultEstimatedRecoveryRecordTTL)
	defaultDuration(&c.CacheCleanupPeriod, DefaultCacheCleanPeriod)
}

// NewDrainBufferChecker constructor for the DrainBufferChecker
func NewDrainBufferChecker(ctx context.Context, logger logr.Logger, kclient client.Client,
	eventRecorder kubernetes.EventRecorder, store kubernetes.RuntimeObjectStore, indexer index.Indexer,
	config DrainBufferCheckerConfiguration) DrainBufferChecker {
	config.applyDefault()

	d := &drainBufferChecker{
		logger:            logger,
		kclient:           kclient,
		eventRecorder:     eventRecorder,
		store:             store,
		indexer:           indexer,
		drainBufferConfig: config,
		cacheRecoveryTime: cache.NewThreadSafeStore(nil, nil),
	}

	go d.runCacheCleanup(ctx)

	return d
}

// runCacheCleanup, delete all the record with an expired TTL
func (d *drainBufferChecker) runCacheCleanup(ctx context.Context) {
	logger := d.logger.WithName("drain-buffer-cache")
	logger.Info("starting cache cleanup")
	defer logger.Info("stopping cache cleanup")
	wait.Until(func() {
		now := time.Now()
		var toDelete []string
		keys := d.cacheRecoveryTime.ListKeys()
		// TODO create a metrics for:
		// count, nb deleted, time taken to perform the cleanup
		for _, k := range keys {
			if obj, ok := d.cacheRecoveryTime.Get(k); ok {
				record := obj.(recoveryEstimationDate)
				if record.estimatedRecovery.Before(now) {
					toDelete = append(toDelete, k)
					continue
				}
				if record.ttl.Before(now) {
					toDelete = append(toDelete, k)
				}
			}
		}
		d.logger.Info("cache stats", "count", len(keys), "toBeDeleted", len(toDelete))
		for _, k := range toDelete {
			d.cacheRecoveryTime.Delete(k)
		}
	},
		*d.drainBufferConfig.CacheCleanupPeriod,
		ctx.Done())
}

// getDrainBuffersConfigurations retrieve the drain-buffers on the node and on the pods of the nodes.
// if no configuration is found the default value is being used. The result is indexed by podKey
func (d *drainBufferChecker) getDrainBuffersConfigurations(ctx context.Context, node *v1.Node) map[string]drainBufferInfo {
	drainBufferNode := drainBufferInfo{
		sourceType: "user/node",
		sourceName: node.Name,
	}
	var found bool
	if drainBufferNode.length, found = d.getNodeDrainBufferConfiguration(ctx, node); !found {
		drainBufferNode.length = *d.drainBufferConfig.DefaultDrainBuffer
		drainBufferNode.sourceType = "default/node"
	}

	buffers := map[string]drainBufferInfo{}

	pods, err := d.indexer.GetPodsByNode(ctx, node.Name)
	if err != nil {
		d.logger.Error(err, "Failed to get pods for node", "node", node.Name)
		return buffers
	}

	for _, p := range pods {
		drainBufferPod := drainBufferNode // inherit from the node
		podDrainBufferConfig, found := d.getPodDrainBufferConfiguration(ctx, p)
		if found && podDrainBufferConfig > drainBufferPod.length {
			drainBufferPod.length = podDrainBufferConfig
			drainBufferPod.sourceType = "user/pod"
			drainBufferPod.sourceName = p.Namespace + "/" + p.Name

		} else if ctrl, ok := kubernetes.GetControllerForPod(p, d.store); ok && ctrl != nil {
			ctrlDrainBufferConfig, found, _ := d.getDrainBufferConfigurationFromAnnotation(ctrl) // Not doing any event on the controller in case of error: we are missing a good method for generic object in the eventRecorder
			if found && ctrlDrainBufferConfig > drainBufferPod.length {
				drainBufferPod.length = ctrlDrainBufferConfig
				drainBufferPod.sourceType = "user/controller"
				drainBufferPod.sourceName = p.Namespace + "/" + ctrl.GetName()
			}
		}
		buffers[index.GeneratePodIndexKey(p.Name, p.Namespace)] = drainBufferPod
	}
	return buffers
}

func (d *drainBufferChecker) getNodeDrainBufferConfiguration(ctx context.Context, node *v1.Node) (time.Duration, bool) {
	nodeDrainBuffer, found, err := d.getDrainBufferConfigurationFromAnnotation(node)
	if err != nil {
		d.eventRecorder.NodeEventf(ctx, node, v1.EventTypeWarning, DrainBufferMisconfigured, "the value for "+kubernetes.CustomDrainBufferAnnotation+" cannot be parsed as a duration: %#v", err)
	}
	return nodeDrainBuffer, found
}

func (d *drainBufferChecker) getPodDrainBufferConfiguration(ctx context.Context, pod *v1.Pod) (time.Duration, bool) {
	podDrainBuffer, found, err := d.getDrainBufferConfigurationFromAnnotation(pod)
	if err != nil {
		d.eventRecorder.PodEventf(ctx, pod, v1.EventTypeWarning, DrainBufferMisconfigured, "the value for "+kubernetes.CustomDrainBufferAnnotation+" cannot be parsed as a duration: %#v", err)
	}
	return podDrainBuffer, found
}

func (d *drainBufferChecker) getDrainBufferConfigurationFromAnnotation(obj metav1.Object) (time.Duration, bool, error) {
	annotations := obj.GetAnnotations()
	if customDrainBuffer, ok := annotations[kubernetes.CustomDrainBufferAnnotation]; ok {
		durationValue, err := time.ParseDuration(customDrainBuffer)
		if err != nil {
			return 0, false, err
		}
		return durationValue, true, nil
	}
	return 0, false, nil
}

// DrainBufferAcceptsDrain checks if the node can be drained
func (d *drainBufferChecker) DrainBufferAcceptsDrain(ctx context.Context, node *v1.Node, now time.Time) bool {
	// retrieve the drain buffer configuration (from node and pods)
	drainBuffers := d.getDrainBuffersConfigurations(ctx, node)

	// retrieve all the associated pdb
	pods, err := d.indexer.GetPodsByNode(ctx, node.Name)
	if err != nil {
		d.logger.Error(err, "Failed to get pods for node", "node", node.Name)
		return false
	}

	cacheKey, err := buildNodePodsCacheKey(node, pods, drainBuffers)
	if err != nil {
		d.logger.Error(err, "Failed to build cache key", "node", node.Name)
		return false
	}

	if d.cacheRecoveryTime != nil {
		// check if we have cached result from previous analysis
		objInCache, found := d.cacheRecoveryTime.Get(cacheKey)
		if found {
			recovery := objInCache.(recoveryEstimationDate)
			if recovery.estimatedRecovery.After(now) {
				// TODO add metrics to check how many time the cache was used
				return false
			}
		}
	}

	pdbsForPods, err := d.indexer.GetPDBsForPods(ctx, pods)
	if err != nil {
		d.logger.Error(err, "Failed to retrieve pdbs for the pods of the node", "node", node.Name)
		// TODO metrics for error
		return false
	}

	var latestRecover time.Time
	canDrain := true
	for podKey, pdbs := range pdbsForPods {
		drainBuffer := drainBuffers[podKey]
		disruptionAllowed, stableSince, pdb := getLatestDisruption(d.logger, pdbs)
		pdbName := ""
		if pdb != nil {
			pdbName = pdb.Name
		}
		loggerInLoop := d.logger.WithValues("node", node.Name, "pod", podKey, "pdb", pdbName)

		if !disruptionAllowed {
			loggerInLoop.Info("pdb blocking")
			return false
		}

		if pdb == nil || stableSince.IsZero() {
			loggerInLoop.Info("pdb blocking, no condition defined or no pdb")
			return false
		}

		recoverAt := stableSince.Add(drainBuffer.length)
		if recoverAt.After(now) && recoverAt.After(latestRecover) {
			canDrain = false
			loggerInLoop.Info("drain buffer blocking", "drain-buffer", drainBuffer)
			latestRecover = recoverAt
			if d.cacheRecoveryTime != nil {
				// add an entry into the cache to avoid recomputing this state before the drain buffer is respected.
				d.cacheRecoveryTime.Add(cacheKey, recoveryEstimationDate{
					estimatedRecovery: recoverAt,
					ttl:               now.Add(*d.drainBufferConfig.EstimatedRecoveryRecordTTL),
				})
			}
		}
	}

	return canDrain
}
func buildNodePodsCacheKey(node *v1.Node, pods []*v1.Pod, drainBuffers map[string]drainBufferInfo) (string, error) {
	podsUIDs := make([]string, len(pods))
	for i, p := range pods {
		podkey := index.GeneratePodIndexKey(p.Name, p.Namespace)
		dbuffer, found := drainBuffers[podkey]
		if !found {
			return "", fmt.Errorf("missing drain-buffer configuration for pod %s", index.GeneratePodIndexKey(p.Name, p.Namespace))
		}
		// We are using drain buffer because if the configuration change we want to use/introduce a different key.
		// The cached result may change due to the drainBuffer configuration change
		podsUIDs[i] = string(p.UID) + dbuffer.length.String()
	}
	sort.Strings(podsUIDs) // stable order
	return string(node.UID) + "#" + strings.Join(podsUIDs, "#"), nil
}

// getLatestDisruption found for how long the PDB has been stable if the disruption is allowed
func getLatestDisruption(logger logr.Logger, pdbs []*policyv1.PodDisruptionBudget) (disruptionAllowed bool, stableSince time.Time, disruptedPDB *policyv1.PodDisruptionBudget) {
	disruptionAllowed = true
	for _, pdb := range pdbs {
		conditionFound := false
		for _, c := range pdb.Status.Conditions {
			if c.Type == policyv1.DisruptionAllowedCondition {
				conditionFound = true
				if c.Status == metav1.ConditionFalse {
					disruptedPDB = pdb
					disruptionAllowed = false
					return
				}
				when := c.LastTransitionTime.Time
				if c.LastTransitionTime.Time.IsZero() {
					logger.Info("PDB condition without transition timestamp", "namespace", pdb.Namespace, "name", pdb.Name)
					// logging here because this should not exist, but it is not a blocking problem, we will consider that pdb is stable since its creation
					when = pdb.CreationTimestamp.Time
				}
				if stableSince.Before(when) {
					disruptedPDB = pdb
					stableSince = when
				}
			}
		}
		if !conditionFound {
			if stableSince.Before(pdb.CreationTimestamp.Time) {
				disruptedPDB = pdb
				stableSince = pdb.CreationTimestamp.Time
			}
		}
	}
	return
}
