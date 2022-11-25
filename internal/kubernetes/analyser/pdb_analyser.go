package analyser

import (
	"context"
	"errors"

	"github.com/planetlabs/draino/internal/kubernetes/index"
	"github.com/planetlabs/draino/internal/kubernetes/utils"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
)

var _ Interface = &PDBAnalyser{}

// PDBAnalyser is an implementation of the analyser interface
type PDBAnalyser struct {
	podIndexer index.PodIndexer
	pdbIndexer index.PDBIndexer
}

// NewPDBAnalyser creates an instance of the PDB analyzer
func NewPDBAnalyser(indexer *index.Indexer) Interface {
	return &PDBAnalyser{podIndexer: indexer, pdbIndexer: indexer}
}

func (a *PDBAnalyser) BlockingPodsOnNode(ctx context.Context, nodeName string) ([]BlockingPod, error) {
	pods, err := a.podIndexer.GetPodsByNode(ctx, nodeName)
	if err != nil {
		return nil, err
	}

	blockingPods := make([]BlockingPod, 0)
	for _, pod := range pods {
		if utils.IsPodReady(pod) {
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

func IsPDBBlocked(ctx context.Context, pod *corev1.Pod, pdb *policyv1.PodDisruptionBudget) bool {
	// If the pod is not ready it's already taking budget from the PDB
	// If the remaining budget is still positive or zero, it's fine
	var podTakingBudget int32 = 0
	if !utils.IsPodReady(pod) {
		podTakingBudget = 1
	}

	// CurrentHealthy - DesiredHealthy will give the currently available budget.
	// If the given pod is not ready, we know that it's taking some of the budget already, so we are increasing the number in that case.
	// In case of lockness, where MaxUnavailable is set to zero, DesiredHealthy will always be equal to the amount of pods covered.
	// In later versions the logic is going to change and we have to adapt the algorithm: https://github.com/kubernetes/kubernetes/commit/a429797f2e84adf5582d3d30d23c9fcfce2b66d8
	remainingBudget := ((pdb.Status.CurrentHealthy + podTakingBudget) - pdb.Status.DesiredHealthy)

	return remainingBudget <= 0
}
