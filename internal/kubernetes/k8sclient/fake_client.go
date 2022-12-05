package k8sclient

import (
	"context"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	cachecr "sigs.k8s.io/controller-runtime/pkg/cache"
	fakecache "sigs.k8s.io/controller-runtime/pkg/cache/informertest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type WithIndex = func(client client.Client, cache cachecr.Cache) error

func BuildInformer[
	T any,
	L any,
	PT interface {
		runtime.Object
		*T
	},
	PL interface {
		client.ObjectList
		*L
	},
](client client.WithWatch) cache.SharedIndexInformer {
	var t T
	obj := PT(&t)

	return cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options v1.ListOptions) (runtime.Object, error) {
				var lst L
				list := PL(&lst)
				err := client.List(context.Background(), list)
				return list, err
			},
			WatchFunc: func(options v1.ListOptions) (watch.Interface, error) {
				var lst L
				list := PL(&lst)
				return client.Watch(context.Background(), list)
			},
			DisableChunking: true,
		},
		obj,
		time.Second,
		cache.Indexers{},
	)
}

func createCache(client client.WithWatch) *fakecache.FakeInformers {
	return &fakecache.FakeInformers{
		InformersByGVK: map[schema.GroupVersionKind]cache.SharedIndexInformer{
			policyv1.SchemeGroupVersion.WithKind("PodDisruptionBudget"): BuildInformer[policyv1.PodDisruptionBudget, policyv1.PodDisruptionBudgetList](client),
			corev1.SchemeGroupVersion.WithKind("Pod"):                   BuildInformer[corev1.Pod, corev1.PodList](client),
			corev1.SchemeGroupVersion.WithKind("Node"):                  BuildInformer[corev1.Node, corev1.NodeList](client),
		},
		Scheme: client.Scheme(),
	}
}

type FakeConf struct {
	Objects []runtime.Object
	Indexes []WithIndex
}

type FakeClientWrapper struct {
	mgrClient client.Client
	cache     *fakecache.FakeInformers
}

func NewFakeClient(conf FakeConf) (*FakeClientWrapper, error) {
	mgrClient := fake.NewFakeClient(conf.Objects...)
	cache := createCache(mgrClient)

	for _, idx := range conf.Indexes {
		if err := idx(mgrClient, cache); err != nil {
			return nil, err
		}
	}

	return &FakeClientWrapper{
		mgrClient: mgrClient,
		cache:     cache,
	}, nil
}

func (c *FakeClientWrapper) GetManagerClient() client.Client {
	return c.mgrClient
}

func (c *FakeClientWrapper) GetCache() cachecr.Cache {
	return c.cache
}

func (c *FakeClientWrapper) Start(ch chan struct{}) {
	var wg sync.WaitGroup
	for _, inf := range c.cache.InformersByGVK {
		wg.Add(1)
		go func(inf cache.SharedIndexInformer) {
			// This is a blocking operation
			go inf.Run(ch)
			defer wg.Done()

			for {
				if inf.HasSynced() {
					break
				}
			}
		}(inf)
	}
	wg.Wait()
}
