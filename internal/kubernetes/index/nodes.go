package index

import (
	"context"

	"github.com/planetlabs/draino/internal/kubernetes/utils"
	corev1 "k8s.io/api/core/v1"
)

type NodeIndexer interface {
	GetAllNodes() ([]*corev1.Node, error)
}

func (i *Indexer) GetAllNodes() ([]*corev1.Node, error) {
	inf, err := i.GetSharedIndexInformer(context.Background(), &corev1.Node{})
	if err != nil {
		return nil, err
	}
	nodes := inf.GetStore().List()
	return utils.ParseObjects[*corev1.Node](nodes)
}
