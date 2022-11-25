package index

import (
	"github.com/go-logr/logr"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/informers"
	fakeclient "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	cachecr "sigs.k8s.io/controller-runtime/pkg/cache"
	fakecache "sigs.k8s.io/controller-runtime/pkg/cache/informertest"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func createCache(inf informers.SharedInformerFactory, scheme *runtime.Scheme) cachecr.Cache {
	return &fakecache.FakeInformers{
		InformersByGVK: map[schema.GroupVersionKind]cache.SharedIndexInformer{
			policyv1.SchemeGroupVersion.WithKind("PodDisruptionBudget"): inf.Policy().V1().PodDisruptionBudgets().Informer(),
			corev1.SchemeGroupVersion.WithKind("Pod"):                   inf.Core().V1().Pods().Informer(),
		},
		Scheme: scheme,
	}

}

func NewFakeIndexer(ch chan struct{}, objects []runtime.Object, logger logr.Logger) (*Indexer, error) {
	fakeClient := fake.NewFakeClient(objects...)
	fakeKubeClient := fakeclient.NewSimpleClientset(objects...)

	inf := informers.NewSharedInformerFactory(fakeKubeClient, 10*time.Second)
	cache := createCache(inf, fakeClient.Scheme())

	informer, err := New(fakeClient, cache, logger)
	if err != nil {
		return nil, err
	}

	inf.Start(ch)
	inf.WaitForCacheSync(ch)

	return informer, nil
}

func NewFakePDBIndexer(ch chan struct{}, objects []runtime.Object, logger logr.Logger) (PDBIndexer, error) {
	return NewFakeIndexer(ch, objects, logger)
}

func NewFakePodIndexer(ch chan struct{}, objects []runtime.Object, logger logr.Logger) (PodIndexer, error) {
	return NewFakeIndexer(ch, objects, logger)
}

type createPodOptions struct {
	Name     string
	Ns       string
	NodeName string
	IsReady  bool
	LS       labels.Set
}

func createPod(opts createPodOptions) *corev1.Pod {
	var label labels.Set = map[string]string{}
	if opts.LS != nil {
		label = opts.LS
	}
	ready := corev1.ConditionFalse
	if opts.IsReady {
		ready = corev1.ConditionTrue
	}
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      opts.Name,
			Namespace: opts.Ns,
			Labels:    label,
		},
		Spec: corev1.PodSpec{
			NodeName: opts.NodeName,
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.ContainersReady,
					Status: ready,
				},
			},
		},
	}
}
