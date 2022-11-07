package index

import (
	"context"

	"github.com/planetlabs/draino/internal/kubernetes/utils"
	cachek "k8s.io/client-go/tools/cache"
	clientcr "sigs.k8s.io/controller-runtime/pkg/client"
)

// GetSharedIndexInformer is used to abstract the GetSharedIndexInformer function.
type GetSharedIndexInformer interface {
	GetSharedIndexInformer(ctx context.Context, obj clientcr.Object) (cachek.SharedIndexInformer, error)
}

// GetFromIndex is returning all elements that match the given key in the given index name
func GetFromIndex[T any, PT interface {
	clientcr.Object
	*T
}](ctx context.Context, i GetSharedIndexInformer, idx, key string) ([]PT, error) {
	// These two lines will create an instance of the expected result Object.
	// https://go.googlesource.com/proposal/+/refs/heads/master/design/43651-type-parameters.md#pointer-method-example
	var t T
	obj := PT(&t)

	indexInformer, err := i.GetSharedIndexInformer(ctx, obj)
	if err != nil {
		return nil, err
	}
	indexer := indexInformer.GetIndexer()
	objs, err := indexer.ByIndex(idx, key)
	if err != nil {
		return nil, err
	}
	return utils.ParseObjects[PT](objs)
}
