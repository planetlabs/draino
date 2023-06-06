package index

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	cachek "k8s.io/client-go/tools/cache"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
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
	// GetPDBsForPods will return a map indexed by podnames
	// with associated PDBs as value
	GetPDBsForPods(ctx context.Context, pods []*corev1.Pod) (map[string][]*policyv1.PodDisruptionBudget, error)
}

func (i *Indexer) GetPDBsBlockedByPod(ctx context.Context, podName, ns string) ([]*policyv1.PodDisruptionBudget, error) {
	key := GeneratePodIndexKey(podName, ns)
	return GetFromIndex[policyv1.PodDisruptionBudget](ctx, i, PDBBlockByPodIdx, key)
}

func (i *Indexer) GetPDBsForPods(ctx context.Context, pods []*corev1.Pod) (map[string][]*policyv1.PodDisruptionBudget, error) {

	// Design decision:
	// this implementation does the label selection at each call so the response time will be
	// a bit bigger than if we were using an index. But the index would be super costly in terms of CPU
	// since each time the PDB (or status) is updated (so each time a pod change status, or labels, or new pods, or deleted pod)
	// we would redo the labelSelection to build the index.
	// The gain is response time is not important, so we prefer to avoid the cost of the index.
	// This method should be called only when we decide for a node drain (or simulation), that should in the end cost
	// less CPU (by far) than the index.

	result := map[string][]*policyv1.PodDisruptionBudget{}

	// let's group pods per namespace
	// to perform analysis per namespace
	perNamespace := map[string][]*corev1.Pod{}
	for _, p := range pods {
		perNs := perNamespace[p.Namespace]
		perNamespace[p.Namespace] = append(perNs, p)
	}

	// work for each namespace
	for ns, podsInNs := range perNamespace {
		var pdbList policyv1.PodDisruptionBudgetList
		if err := i.client.List(ctx, &pdbList, &clientcr.ListOptions{Namespace: ns}); err != nil {
			i.logger.Error(err, "failed to list pdb", "namespace", ns)
			continue
		}

		for j := range pdbList.Items {
			pdb := &pdbList.Items[j]
			selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
			if err != nil {
				i.logger.Error(fmt.Errorf("failed to build selector for pdb: %v", err), "namespace", pdb.Namespace, "name", pdb.Name)
				continue
			}
			for _, pod := range podsInNs {
				ls := labels.Set(pod.GetLabels())
				if !selector.Matches(ls) {
					continue
				}
				podKey := GeneratePodIndexKey(pod.Name, pod.Namespace)
				otherPDBs := result[podKey]
				result[podKey] = append(otherPDBs, pdb)
			}
		}
	}
	return result, nil
}

type podListFunc = func(ctx context.Context, namespace string) (*corev1.PodList, error)

func initPDBIndexer(cache cachecr.Cache, podListFn podListFunc) error {
	informer, err := cache.GetInformer(context.Background(), &policyv1.PodDisruptionBudget{})
	if err != nil {
		return err
	}

	return informer.AddIndexers(map[string]cachek.IndexFunc{
		PDBBlockByPodIdx: func(obj interface{}) ([]string, error) { return indexPDBBlockingPod(podListFn, obj) },
	})
}

func indexPDBBlockingPod(podListFn podListFunc, o interface{}) ([]string, error) {
	pdb, ok := o.(*policyv1.PodDisruptionBudget)
	if !ok {
		return nil, errors.New("cannot parse pdb object in indexer")
	}
	return getAssociatedPodsForPDB(podListFn, pdb, true)
}

func getAssociatedPodsForPDB(podListFn podListFunc, pdb *policyv1.PodDisruptionBudget, onlyNotReadyPods bool) ([]string, error) {
	pods, err := podListFn(context.Background(), pdb.Namespace)
	if err != nil {
		return nil, err
	}

	selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
	if err != nil {
		return []string{}, err
	}

	associatedPods := make([]string, 0)
	for _, pod := range pods.Items {
		if onlyNotReadyPods && podutil.IsPodReady(&pod) {
			continue
		}
		ls := labels.Set(pod.GetLabels())
		if !selector.Matches(ls) {
			continue
		}

		associatedPods = append(associatedPods, GeneratePodIndexKey(pod.GetName(), pod.GetNamespace()))
	}
	return associatedPods, nil

}

func GeneratePodIndexKey(podName, ns string) string {
	// This is needed because PDBs are namespace scoped, so we might have name collisions
	return fmt.Sprintf("%s/%s", ns, podName)
}
