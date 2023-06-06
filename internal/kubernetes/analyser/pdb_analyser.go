package analyser

import (
	"context"
	"errors"
	"time"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/utils/clock"
)

var _ PDBAnalyser = &pdbAnalyserImpl{}

// pdbAnalyserImpl is an implementation of the analyser interface
type pdbAnalyserImpl struct {
	podIndexer              index.PodIndexer
	pdbIndexer              index.PDBIndexer
	context                 context.Context
	logger                  logr.Logger
	clock                   clock.Clock
	podWarmupDelayExtension time.Duration
}

// NewPDBAnalyser creates an instance of the PDB analyzer
func NewPDBAnalyser(ctx context.Context, logger logr.Logger, indexer *index.Indexer, clock clock.Clock, podWarmupDelayExtension time.Duration) PDBAnalyser {
	return &pdbAnalyserImpl{context: ctx, podIndexer: indexer, pdbIndexer: indexer, logger: logger.WithName("PDBAnalyser"), clock: clock, podWarmupDelayExtension: podWarmupDelayExtension}
}

// CompareNode return true if the node n1 should be drained in priority compared to node n2
func (a *pdbAnalyserImpl) CompareNode(n1, n2 *corev1.Node) bool {

	// Sorting elements might be impossible or not stable. Since we have to look at pods and associated PDB
	// we may have some situation where attempting a drain will fail anyway.
	// let say that pod marked with * take the budget for the associated app
	// Node1 (*podApp1,podApp2)
	// Node2 (podApp1,*podApp2)
	// in that case it is impossible to get out of the situation. Starting by n1 or n2 would result in a failed drain

	// Design decision: lookup by application and find the node that is affected by the lower number of PDB (not being 0)
	// This node is the one that have maximum chances to unlock situation by potentially making one (or few) application stable again

	// As the code is being written the function BlockingPodsOnNode is O(1) since it only captures data prebuilt into indexes

	podAndPDBBlocking1, err1 := a.BlockingPodsOnNode(a.context, n1.Name)
	if err1 != nil {
		a.logger.Error(err1, "Failed to do BlockingPodsOnNode", "node", n1.Name)
	}
	podAndPDBBlocking2, err2 := a.BlockingPodsOnNode(a.context, n2.Name)
	if err2 != nil {
		a.logger.Error(err2, "Failed to do BlockingPodsOnNode", "node", n2.Name)
	}

	// Purge specific pod that might show problem due to transient situation like rollout (warmup), termination, ...
	podAndPDBBlocking1 = a.removeTransientBlockingStates(podAndPDBBlocking1)
	podAndPDBBlocking2 = a.removeTransientBlockingStates(podAndPDBBlocking2)

	idxByPDB := func(s []BlockingPod) map[string][]*BlockingPod {
		pdbIdx := map[string][]*BlockingPod{}
		for i := range s {
			pdbName := s[i].PDB.Namespace + "/" + s[i].PDB.Name
			pdbIdx[pdbName] = append(pdbIdx[pdbName], &s[i])
		}
		return pdbIdx
	}
	pdbSet1 := idxByPDB(podAndPDBBlocking1)
	pdbSet2 := idxByPDB(podAndPDBBlocking2)

	if len(pdbSet1) == 0 { // n1 can't be priority compare to 2
		return false
	}
	if len(pdbSet2) == 0 { // n1 has at least one pdb failing and not n2 so n1 is in priority
		return true
	}

	// both have affected pdb. Let's take the one with less impact that would have maximum chances to drain
	if len(pdbSet1) < len(pdbSet2) {
		return true
	}
	if len(pdbSet2) < len(pdbSet1) {
		return false
	}

	// if they are both affected by the same number of PDB, lets take the one that have the lower number of pod
	// impacted as this would maximize the chance of drain success and gaining in stability
	return len(podAndPDBBlocking1) < len(podAndPDBBlocking2)
}

func (a *pdbAnalyserImpl) BlockingPodsOnNode(ctx context.Context, nodeName string) ([]BlockingPod, error) {
	pods, err := a.podIndexer.GetPodsByNode(ctx, nodeName)
	if err != nil {
		return nil, err
	}

	blockingPods := make([]BlockingPod, 0)
	for _, pod := range pods {
		if podutil.IsPodReady(pod) {
			// we are only interested in not ready pods
			continue
		}
		pdbs, err := a.pdbIndexer.GetPDBsBlockedByPod(ctx, pod.GetName(), pod.GetNamespace())
		if err != nil {
			return nil, errors.New("cannot get blocked pdbs by pod name")
		}

		for _, pdb := range pdbs {
			blockingPods = append(blockingPods, BlockingPod{
				NodeName: nodeName,
				Pod:      pod.DeepCopy(),
				PDB:      pdb.DeepCopy(),
			})
		}
	}

	return blockingPods, nil
}

func (a *pdbAnalyserImpl) removeTransientBlockingStates(b []BlockingPod) []BlockingPod {
	result := make([]BlockingPod, 0, len(b))
	for _, p := range b {
		if a.isWarmingUpPods(p) {
			continue
		}
		result = append(result, p)
	}
	return result
}

func getMaxRestartCount(p *corev1.Pod) (max int32) {
	checkCS := func(cs []corev1.ContainerStatus) {
		for _, c := range cs {
			if c.RestartCount > max {
				max = c.RestartCount
			}
		}
	}
	checkCS(p.Status.EphemeralContainerStatuses)
	checkCS(p.Status.InitContainerStatuses)
	checkCS(p.Status.ContainerStatuses)
	return max
}

func hasImageFailureOrCLBContainer(p *corev1.Pod) bool {
	checkCS := func(cs []corev1.ContainerStatus) bool {
		for _, c := range cs {
			if c.State.Waiting != nil {
				switch c.State.Waiting.Reason {
				//https://github.com/kubernetes/kubernetes/blob/64af1adaceba4db8d0efdb91453bce7073973771/pkg/kubelet/images/types.go#L27
				case "ErrImagePullBackOff", "ImageInspectError", "ErrImagePull", "ErrImageNeverPull", "InvalidImageName", "RegistryUnavailable":
					return true
				// for example the image is Ok, but the binary cannot be found in the image, like typo in the command name
				case "CrashLoopBackOff":
					return true
				}
			}
		}
		return false
	}
	return checkCS(p.Status.ContainerStatuses) || checkCS(p.Status.InitContainerStatuses) || checkCS(p.Status.EphemeralContainerStatuses)
}

// isWarmingUpPods check if this pod is in a warmup phase (it is being started)
func (a *pdbAnalyserImpl) isWarmingUpPods(b BlockingPod) bool {
	pod := b.Pod

	if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
		return false // this is a permanent and final state
	}

	if hasImageFailureOrCLBContainer(pod) {
		return false
	}

	if pod.Status.Phase == corev1.PodPending {
		// PodPending means the pod has been accepted by the system, but one or more of the containers
		// has not been started. This includes time before being bound to a node, as well as time spent
		// pulling images onto the host.

		// Do we have many restarts for a container
		// This would reveal cases where init containers are constantly failing
		if getMaxRestartCount(pod) >= 3 {
			// this is likely to reoccur, the pod has a problem
			return false
		}
	}

	if pod.Status.Phase != corev1.PodRunning {
		// The pod is probably in PodPending phase with no restart registered
		// So everything looks ok, let's continue
		return true
	}

	if !allMainContainersHaveStartedAtLeastOnce(pod) {
		// Not all the containers have started and none is in CLB so let's wait
		return true
	}

	// index containers by name
	containersWarmUpDelay := make(map[string]time.Duration, len(pod.Spec.Containers))
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		containersWarmUpDelay[c.Name] = getContainerWarmUpDelay(c)
	}

	// Let's check that all containers have been given enough time to start
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Terminated == nil && cs.State.Running != nil {
			if cs.State.Running.StartedAt.Time.Add(containersWarmUpDelay[cs.Name]).Add(a.podWarmupDelayExtension).After(a.clock.Now()) {
				return true
			}
		}
	}
	return false
}

func allMainContainersHaveStartedAtLeastOnce(pod *corev1.Pod) bool {
	for _, c := range pod.Status.ContainerStatuses {
		if c.LastTerminationState.Terminated != nil {
			continue
		}
		if c.State.Running != nil || c.State.Terminated != nil {
			continue
		}
		return false
	}
	return true
}

func getContainerWarmUpDelay(c *corev1.Container) time.Duration {
	return getMaxProbeDelay([]*corev1.Probe{c.StartupProbe, c.LivenessProbe})
}
func getProbeDelay(p *corev1.Probe) time.Duration {
	return time.Duration(p.InitialDelaySeconds+p.PeriodSeconds*p.SuccessThreshold) * time.Second
}
func getMaxProbeDelay(probes []*corev1.Probe) (max time.Duration) {
	for _, p := range probes {
		if p != nil {
			d := getProbeDelay(p)
			if d > max {
				max = d
			}
		}
	}
	return
}

func IsPDBBlockedByPod(ctx context.Context, pod *corev1.Pod, pdb *policyv1.PodDisruptionBudget) bool {
	// If the pod is not ready it's already taking budget from the PDB
	// If the remaining budget is still positive or zero, it's fine
	var podTakingBudget int32 = 0
	if !podutil.IsPodReady(pod) {
		podTakingBudget = 1
	}

	// CurrentHealthy - DesiredHealthy will give the currently available budget.
	// If the given pod is not ready, we know that it's taking some of the budget already, so we are increasing the number in that case.
	// In case of lockness, where MaxUnavailable is set to zero, DesiredHealthy will always be equal to the amount of pods covered.
	// In later versions the logic is going to change and we have to adapt the algorithm: https://github.com/kubernetes/kubernetes/commit/a429797f2e84adf5582d3d30d23c9fcfce2b66d8
	remainingBudget := ((pdb.Status.CurrentHealthy + podTakingBudget) - pdb.Status.DesiredHealthy)

	return remainingBudget <= 0
}
