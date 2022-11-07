package analyser

import (
	"context"
	"errors"

	"github.com/planetlabs/draino/internal/kubernetes/index"
	"github.com/planetlabs/draino/internal/kubernetes/utils"
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
