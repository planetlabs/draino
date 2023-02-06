package index

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes/utils"

	corev1 "k8s.io/api/core/v1"
	cachek "k8s.io/client-go/tools/cache"
	"k8s.io/utils/clock"
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
	logger logr.Logger
	clock  clock.Clock

	podListCache utils.TTLCache[*corev1.PodList]
}

// New creates and initializes a new Indexer object
func New(ctx context.Context, client clientcr.Client, cache cachecr.Cache, logger logr.Logger) (*Indexer, error) {
	informer := &Indexer{
		client:       client,
		cache:        cache,
		logger:       logger,
		clock:        clock.RealClock{},
		podListCache: utils.NewTTLCache[*corev1.PodList](10*time.Second, 10*time.Second),
	}

	go informer.podListCache.StartCleanupLoop(ctx)

	if err := informer.Init(); err != nil {
		return nil, err
	}

	return informer, nil
}

func (i *Indexer) listPodsCached(ctx context.Context, namespace string) (*corev1.PodList, error) {
	if lst, exist := i.podListCache.Get(namespace, i.clock.Now()); exist {
		return lst, nil
	}
	var podList corev1.PodList
	if err := i.client.List(ctx, &podList, &clientcr.ListOptions{Namespace: namespace}); err != nil {
		return nil, err
	}

	i.podListCache.Add(namespace, &podList)

	return &podList, nil
}

// Init will initialize all the indices that are used / available.
func (i *Indexer) Init() error {
	if err := initPDBIndexer(i.cache, i.listPodsCached); err != nil {
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
