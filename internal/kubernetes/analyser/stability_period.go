package analyser

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	StabilityPeriodAnnotationKey = "node-lifecycle.datadoghq.com/stability-period"
	StabilityPeriodMisconfigured = "StabilityPeriodMisconfigured"

	DefaultEstimatedRecoveryRecordTTL = 15 * time.Minute
	DefaultCacheCleanPeriod           = 3 * time.Minute
	DefaultStabilityPeriodLength      = 3 * time.Minute
)

// StabilityPeriodChecker, can a node be drain and respect stability period configuration
type StabilityPeriodChecker interface {
	// StabilityPeriodAcceptsDrain check if the node can be drained, would we respect all the stability periods set on the node or on the pods
	// the time value passed should represent "Now" on a live system. We are passing it to allow simulation (future) and ease unittesting
	StabilityPeriodAcceptsDrain(context.Context, *v1.Node, time.Time) bool
}

// stabilityPeriodChecker, implementation for StabilityPeriodChecker
// It retrieves the configuration for stability period on node and pods. It compares this with the pdb transition changes.
type stabilityPeriodChecker struct {
	logger                logr.Logger
	kclient               client.Client
	eventRecorder         kubernetes.EventRecorder
	store                 kubernetes.RuntimeObjectStore
	indexer               *index.Indexer
	stabilityPeriodConfig StabilityPeriodCheckerConfiguration
	podFilterFunc         kubernetes.PodFilterFunc

	// cacheRecoveryTime for a combination {Node+Pods} this cache store the estimated recoveryTime
	// Note the if a pod is delete from the node the key can't be used anymore, so that cacheclean will remove this key when the TTL expire
	cacheRecoveryTime cache.ThreadSafeStore
}

var _ StabilityPeriodChecker = &stabilityPeriodChecker{}

// stabilityPeriodInfo, the stability period configuration can be set on the node, on the pod or the controller of the pod.
// this structure allow us to carry the value and the source of the configuration
type stabilityPeriodInfo struct {
	length     time.Duration
	sourceType string
	sourceName string
}

// recoveryEstimationDate is used as value in the cache cacheRecoveryTime
type recoveryEstimationDate struct {
	estimatedRecovery time.Time
	ttl               time.Time
}

// StabilityPeriodCheckerConfiguration Configuration used in the constructor of StabilityPeriodChecker
type StabilityPeriodCheckerConfiguration struct {
	// DefaultStabilityPeriod, default value to be used if no configuration is given on pods or nodes
	DefaultStabilityPeriod *time.Duration
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

func (c *StabilityPeriodCheckerConfiguration) applyDefault() {
	defaultDuration(&c.DefaultStabilityPeriod, DefaultStabilityPeriodLength)
	defaultDuration(&c.EstimatedRecoveryRecordTTL, DefaultEstimatedRecoveryRecordTTL)
	defaultDuration(&c.CacheCleanupPeriod, DefaultCacheCleanPeriod)
}

// NewStabilityPeriodChecker constructor for the StabilityPeriodChecker
func NewStabilityPeriodChecker(ctx context.Context, logger logr.Logger, kclient client.Client,
	eventRecorder kubernetes.EventRecorder, store kubernetes.RuntimeObjectStore, indexer *index.Indexer,
	config StabilityPeriodCheckerConfiguration, podFilterFunc kubernetes.PodFilterFunc) StabilityPeriodChecker {
	config.applyDefault()

	s := &stabilityPeriodChecker{
		logger:                logger,
		kclient:               kclient,
		eventRecorder:         eventRecorder,
		store:                 store,
		indexer:               indexer,
		stabilityPeriodConfig: config,
		cacheRecoveryTime:     cache.NewThreadSafeStore(nil, nil),
		podFilterFunc:         podFilterFunc,
	}

	go s.runCacheCleanup(ctx)

	return s
}

// runCacheCleanup, delete all the record with an expired TTL
func (d *stabilityPeriodChecker) runCacheCleanup(ctx context.Context) {
	logger := d.logger.WithName("stability-period-cache")
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
		*d.stabilityPeriodConfig.CacheCleanupPeriod,
		ctx.Done())
}

// getStabilityPeriodsConfigurations retrieve the stability-periods on the node and on the pods of the nodes.
// if no configuration is found the default value is being used. The result is indexed by podKey
func (d *stabilityPeriodChecker) getStabilityPeriodsConfigurations(ctx context.Context, node *v1.Node) map[string]stabilityPeriodInfo {
	stabilityPeriodNode := stabilityPeriodInfo{
		sourceType: "user/node",
		sourceName: node.Name,
	}
	var found bool
	if stabilityPeriodNode.length, found = d.getNodeStabilityPeriodConfiguration(ctx, node); !found {
		stabilityPeriodNode.length = *d.stabilityPeriodConfig.DefaultStabilityPeriod
		stabilityPeriodNode.sourceType = "default/node"
	}

	periods := map[string]stabilityPeriodInfo{}

	pods, err := d.indexer.GetPodsByNode(ctx, node.Name)
	if err != nil {
		d.logger.Error(err, "Failed to get pods for node", "node", node.Name)
		return periods
	}

	for _, p := range pods {
		stabilityPeriodPod := stabilityPeriodNode // inherit from the node
		podStabilityPeriodConfig, found := d.getPodStabilityPeriodConfiguration(ctx, p)
		if found && podStabilityPeriodConfig.length > stabilityPeriodPod.length {
			stabilityPeriodPod = podStabilityPeriodConfig
		}
		periods[index.GeneratePodIndexKey(p.Name, p.Namespace)] = stabilityPeriodPod
	}
	return periods
}

func (d *stabilityPeriodChecker) getNodeStabilityPeriodConfiguration(ctx context.Context, node *v1.Node) (time.Duration, bool) {
	nodeStabilityPeriod, found, err := getStabilityPeriodConfigurationFromAnnotation(node)
	if err != nil && d.eventRecorder != nil {
		d.eventRecorder.NodeEventf(ctx, node, v1.EventTypeWarning, StabilityPeriodMisconfigured, "the value for "+StabilityPeriodAnnotationKey+" cannot be parsed as a duration: %#v", err)
	}
	return nodeStabilityPeriod, found
}

func (d *stabilityPeriodChecker) getPodStabilityPeriodConfiguration(ctx context.Context, pod *v1.Pod) (sp stabilityPeriodInfo, found bool) {
	var err error
	sp.length, found, err = getStabilityPeriodConfigurationFromAnnotation(pod)
	if !found {
		if obj, foundCtrl := kubernetes.GetControllerForPod(pod, d.store); foundCtrl {
			sp.length, found, err = getStabilityPeriodConfigurationFromAnnotation(obj)
			if err != nil && d.eventRecorder != nil {
				// TODO fix the event recorder to be able to put an event on any type of object
			}
			if found {
				sp.sourceType = "user/controller"
				sp.sourceName = obj.GetNamespace() + "/" + obj.GetName()
			}
			return sp, found
		}
	}

	if err != nil && d.eventRecorder != nil {
		d.eventRecorder.PodEventf(ctx, pod, v1.EventTypeWarning, StabilityPeriodMisconfigured, "the value for "+StabilityPeriodAnnotationKey+" cannot be parsed as a duration: %#v", err)
	}

	if found {
		sp.sourceType = "user/pod"
		sp.sourceName = pod.Namespace + "/" + pod.Name
	}

	return sp, found
}

func getStabilityPeriodConfigurationFromAnnotation(obj metav1.Object) (time.Duration, bool, error) {
	annotations := obj.GetAnnotations()
	if customStabilityPeriod, ok := annotations[StabilityPeriodAnnotationKey]; ok {
		durationValue, err := time.ParseDuration(customStabilityPeriod)
		if err != nil {
			return 0, false, err
		}
		return durationValue, true, nil
	}
	return 0, false, nil
}

// StabilityPeriodAcceptsDrain checks if the node can be drained
func (d *stabilityPeriodChecker) StabilityPeriodAcceptsDrain(ctx context.Context, node *v1.Node, now time.Time) bool {
	// retrieve the stability period configuration (from node and pods)
	stabilityPeriods := d.getStabilityPeriodsConfigurations(ctx, node)

	// retrieve all the associated pdb
	pods, err := d.indexer.GetPodsByNode(ctx, node.Name)
	if err != nil {
		d.logger.Error(err, "Failed to get pods for node", "node", node.Name)
		return false
	}

	pods, err = d.filterPods(pods)
	if err != nil {
		d.logger.Error(err, "Failed to filter pods", "node", node.Name)
		return false
	}

	cacheKey, err := buildNodePodsCacheKey(node, pods, stabilityPeriods)
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
		period := stabilityPeriods[podKey]
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

		recoverAt := stableSince.Add(period.length)
		if recoverAt.After(now) && recoverAt.After(latestRecover) {
			canDrain = false
			loggerInLoop.Info("stability period blocking", "stability-period", period)
			latestRecover = recoverAt
			if d.cacheRecoveryTime != nil {
				// add an entry into the cache to avoid recomputing this state before the stability period is respected.
				d.cacheRecoveryTime.Add(cacheKey, recoveryEstimationDate{
					estimatedRecovery: recoverAt,
					ttl:               now.Add(*d.stabilityPeriodConfig.EstimatedRecoveryRecordTTL),
				})
			}
		}
	}

	return canDrain
}

func (d *stabilityPeriodChecker) filterPods(pods []*v1.Pod) ([]*v1.Pod, error) {
	result := make([]*v1.Pod, 0, len(pods))
	for _, pod := range pods {
		keep, _, err := d.podFilterFunc(*pod)
		if err != nil {
			return nil, err
		}
		if keep {
			result = append(result, pod)
		}
	}
	return result, nil
}

func buildNodePodsCacheKey(node *v1.Node, pods []*v1.Pod, periods map[string]stabilityPeriodInfo) (string, error) {
	podsUIDs := make([]string, len(pods))
	for i, p := range pods {
		podkey := index.GeneratePodIndexKey(p.Name, p.Namespace)
		sPeriod, found := periods[podkey]
		if !found {
			return "", fmt.Errorf("missing stability-period configuration for pod %s", index.GeneratePodIndexKey(p.Name, p.Namespace))
		}
		// We are using stability period because if the configuration change we want to use/introduce a different key.
		// The cached result may change due to the stability period configuration change
		podsUIDs[i] = string(p.UID) + sPeriod.length.String()
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
