package index

import (
	"context"
	"errors"
	"fmt"

	"github.com/planetlabs/draino/internal/kubernetes/utils"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	cachek "k8s.io/client-go/tools/cache"
	cachecr "sigs.k8s.io/controller-runtime/pkg/cache"
	clientcr "sigs.k8s.io/controller-runtime/pkg/client"
)

// PDBBlockByPodIdx is the index key used to index blocking pods for each PDB
const PDBBlockByPodIdx = "pod:blocking:pdb"

// PDBIndexer abstracts all the methods related to PDB based indices
type PDBIndexer interface {
	// GetPDBsBlockedByPod will return a list of PDBs that are blocked by the given pod.
	// This means that the disruption budget are used by the Pod.
	GetPDBsBlockedByPod(ctx context.Context, podName, ns string) ([]*policyv1.PodDisruptionBudget, error)
}

func (i *Indexer) GetPDBsBlockedByPod(ctx context.Context, podName, ns string) ([]*policyv1.PodDisruptionBudget, error) {
	key := generatePodIndexKey(podName, ns)
	return GetFromIndex[policyv1.PodDisruptionBudget](ctx, i, PDBBlockByPodIdx, key)
}

func initPDBIndexer(client clientcr.Client, cache cachecr.Cache) error {
	informer, err := cache.GetInformer(context.Background(), &policyv1.PodDisruptionBudget{})
	if err != nil {
		return err
	}
	return informer.AddIndexers(map[string]cachek.IndexFunc{
		PDBBlockByPodIdx: func(obj interface{}) ([]string, error) { return indexPDBBlockingPod(client, obj) },
	})
}

func indexPDBBlockingPod(client clientcr.Client, o interface{}) ([]string, error) {
	pdb, ok := o.(*policyv1.PodDisruptionBudget)
	if !ok {
		return nil, errors.New("cannot parse pdb object in indexer")
	}
	return getBlockingPodsForPDB(client, pdb)
}

func getBlockingPodsForPDB(client clientcr.Client, pdb *policyv1.PodDisruptionBudget) ([]string, error) {
	var pods corev1.PodList
	if err := client.List(context.Background(), &pods, &clientcr.ListOptions{Namespace: pdb.GetNamespace()}); err != nil {
		return []string{}, err
	}

	selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
	if err != nil {
		return []string{}, err
	}

	blockingPods := make([]string, 0)
	for _, pod := range pods.Items {
		if utils.IsPodReady(&pod) {
			continue
		}

		ls := labels.Set(pod.GetLabels())
		if !selector.Matches(ls) {
			continue
		}

		blockingPods = append(blockingPods, generatePodIndexKey(pod.GetName(), pod.GetNamespace()))
	}

	return blockingPods, nil
}

func generatePodIndexKey(podName, ns string) string {
	// This is needed because PDBs are namespace scoped, so we might have name collisions
	return fmt.Sprintf("%s/%s", podName, ns)
}
