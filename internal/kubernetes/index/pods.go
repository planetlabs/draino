package index

import (
	"context"
	"errors"

	corev1 "k8s.io/api/core/v1"
	cachek "k8s.io/client-go/tools/cache"
	cachecr "sigs.k8s.io/controller-runtime/pkg/cache"
)

const (
	// PodsByNodeNameIdx is the index key used to index all pods running on a node
	PodsByNodeNameIdx = "pods:by:node"
	PodsByPhase       = "pods:by:phase"
)

// PodIndexer abstracts all the methods related to Pod based indices
type PodIndexer interface {
	// GetPodsByNode will return all the pods assigned to the given node
	GetPodsByNode(ctx context.Context, nodeName string) ([]*corev1.Pod, error)
	// GetPodsByPhase will return all pods that are in the given phase
	GetPodsByPhase(ctx context.Context, phase corev1.PodPhase) ([]*corev1.Pod, error)
	// GetPodCount will return the total amount of pods in the system
	GetPodCount(ctx context.Context) (int, error)
}

func (i *Indexer) GetPodsByNode(ctx context.Context, nodeName string) ([]*corev1.Pod, error) {
	return GetFromIndex[corev1.Pod](ctx, i, PodsByNodeNameIdx, nodeName)
}

func (i *Indexer) GetPodsByPhase(ctx context.Context, phase corev1.PodPhase) ([]*corev1.Pod, error) {
	return GetFromIndex[corev1.Pod](ctx, i, PodsByPhase, string(phase))
}

func (i *Indexer) GetPodCount(ctx context.Context) (int, error) {
	inf, err := i.GetSharedIndexInformer(ctx, &corev1.Pod{})
	if err != nil {
		return 0, err
	}

	return len(inf.GetStore().List()), nil
}

func initPodIndexer(cache cachecr.Cache) error {
	informer, err := cache.GetInformer(context.Background(), &corev1.Pod{})
	if err != nil {
		return err
	}
	return informer.AddIndexers(map[string]cachek.IndexFunc{
		PodsByNodeNameIdx: indexPodsByNodeName,
		PodsByPhase:       indexPodsByPhase,
	})
}

func indexPodsByPhase(o interface{}) ([]string, error) {
	pod, ok := o.(*corev1.Pod)
	if !ok {
		return []string{""}, nil
	}
	return []string{string(pod.Status.Phase)}, nil
}

func indexPodsByNodeName(o interface{}) ([]string, error) {
	pod, ok := o.(*corev1.Pod)
	if !ok {
		return nil, errors.New("cannot parse pod object for indexing")
	}
	return []string{pod.Spec.NodeName}, nil
}
