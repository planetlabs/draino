package index

import (
	"context"
	"fmt"

	cachek "k8s.io/client-go/tools/cache"
	cachecr "sigs.k8s.io/controller-runtime/pkg/cache"
	clientcr "sigs.k8s.io/controller-runtime/pkg/client"
)

// Make sure that the Informer is implementing all the required interfaces
var (
	_ PDBIndexer = &Indexer{}
	_ PodIndexer = &Indexer{}

	_ GetSharedIndexInformer = &Indexer{}
)

// Indexer is an implementation of multiple interfaces exported by the index module
type Indexer struct {
	client clientcr.Client
	cache  cachecr.Cache
}

// New creates and initializes a new Indexer object
func New(client clientcr.Client, cache cachecr.Cache) (*Indexer, error) {
	informer := &Indexer{client, cache}

	if err := informer.Init(); err != nil {
		return nil, err
	}

	return informer, nil
}

// Init will initialize all the indices that are used / available.
func (i *Indexer) Init() error {
	if err := initPDBIndexer(i.client, i.cache); err != nil {
		return err
	}
	if err := initPodIndexer(i.cache); err != nil {
		return err
	}
	return nil
}

// GetSharedIndexInformer returns an index / informer for the given object type.
// It will return an error if there is no or an invalid informer.
func (i *Indexer) GetSharedIndexInformer(ctx context.Context, obj clientcr.Object) (cachek.SharedIndexInformer, error) {
	informer, err := i.cache.GetInformer(ctx, obj)
	if err != nil {
		return nil, err
	}

	indexInformer, ok := informer.(cachek.SharedIndexInformer)
	if !ok {
		return nil, fmt.Errorf("unable to create shared index informer")
	}

	return indexInformer, nil
}
