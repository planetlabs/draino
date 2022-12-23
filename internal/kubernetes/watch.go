/*
Copyright 2018 Planet Labs Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing permissions
and limitations under the License.
*/

package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

type SyncedStore interface {
	HasSynced() bool
}

type RuntimeObjectStore interface {
	SyncedStore
	Nodes() NodeStore
	Pods() PodStore
	StatefulSets() StatefulSetStore
	PersistentVolumes() PersistentVolumeStore
	PersistentVolumeClaims() PersistentVolumeClaimStore
}

type RuntimeObjectStoreImpl struct {
	NodesStore                 *NodeWatch
	PodsStore                  *PodWatch
	StatefulSetsStore          *StatefulSetWatch
	PersistentVolumeStore      *PersistentVolumeWatch
	PersistentVolumeClaimStore *PersistentVolumeClaimWatch
	hasSynced                  bool
}

func (r *RuntimeObjectStoreImpl) Nodes() NodeStore {
	return r.NodesStore
}

func (r *RuntimeObjectStoreImpl) Pods() PodStore {
	return r.PodsStore
}

func (r *RuntimeObjectStoreImpl) StatefulSets() StatefulSetStore {
	return r.StatefulSetsStore
}

func (r *RuntimeObjectStoreImpl) PersistentVolumes() PersistentVolumeStore {
	return r.PersistentVolumeStore
}

func (r *RuntimeObjectStoreImpl) PersistentVolumeClaims() PersistentVolumeClaimStore {
	return r.PersistentVolumeClaimStore
}

func (r *RuntimeObjectStoreImpl) HasSynced() bool {
	if r.hasSynced {
		return true
	}
	r.hasSynced = r.NodesStore.HasSynced() &&
		r.StatefulSetsStore.HasSynced() &&
		r.PodsStore.HasSynced() &&
		r.PersistentVolumeStore.HasSynced() &&
		r.PersistentVolumeClaimStore.HasSynced()
	return r.hasSynced
}

// An NodeStore is a cache of node resources.
type NodeStore interface {
	SyncedStore
	// Get an node by name. Returns an error if the node does not exist.
	Get(name string) (*core.Node, error)
	ListNodes() []*core.Node
}

// An NodeWatch is a cache of node resources that notifies registered
// handlers when its contents change.
type NodeWatch struct {
	cache.SharedInformer
}

var _ NodeStore = &NodeWatch{}

// NewNodeWatch creates a watch on node resources. Nodes are cached and the
// provided ResourceEventHandlers are called when the cache changes.
func NewNodeWatch(ctx context.Context, c kubernetes.Interface, rs ...cache.ResourceEventHandler) *NodeWatch {
	lw := &cache.ListWatch{
		ListFunc:  func(o meta.ListOptions) (runtime.Object, error) { return c.CoreV1().Nodes().List(ctx, o) },
		WatchFunc: func(o meta.ListOptions) (watch.Interface, error) { return c.CoreV1().Nodes().Watch(ctx, o) },
	}
	i := cache.NewSharedInformer(lw, &core.Node{}, 30*time.Minute)
	for _, r := range rs {
		i.AddEventHandler(r)
	}
	return &NodeWatch{i}
}

// Get an node by name. Returns an error if the node does not exist.
func (w *NodeWatch) Get(name string) (*core.Node, error) {
	o, exists, err := w.GetStore().GetByKey(name)
	if err != nil {
		return nil, fmt.Errorf("cannot get node %s: %w", name, err)
	}
	if !exists {
		return nil, apierrors.NewNotFound(v1.Resource("node"), name)
	}
	return o.(*core.Node), nil
}

// ListNodes List the nodes of the store
func (w *NodeWatch) ListNodes() []*core.Node {
	list := w.GetStore().List()
	nodes := make([]*core.Node, len(list))
	for i := range list {
		nodes[i] = list[i].(*core.Node)
	}
	return nodes
}

type PodStore interface {
	SyncedStore
	// List all the pods of a given node
	ListPodsForNode(nodeName string) ([]*core.Pod, error)
	ListPodsByStatus(podStatus string) ([]*core.Pod, error)
	GetPodCount() (int, error)
	ListPodsForClaim(namespace, claimName string) ([]*core.Pod, error)
}

// A PodWatch is a cache of pod resources that notifies registered
// handlers when its contents change.
type PodWatch struct {
	cache.SharedIndexInformer
}

var _ PodStore = &PodWatch{}

const podNodeNameIndexField = ".spec.nodeName"
const podStatusIndexField = ".status.phase"
const podClaimIndexField = ".spec.phase"

// NewPodWatch creates a watch on pod resources. Pods are cached and the
// provided ResourceEventHandlers are called when the cache changes.
func NewPodWatch(ctx context.Context, c kubernetes.Interface) *PodWatch {
	lw := &cache.ListWatch{
		ListFunc:  func(o meta.ListOptions) (runtime.Object, error) { return c.CoreV1().Pods("").List(ctx, o) },
		WatchFunc: func(o meta.ListOptions) (watch.Interface, error) { return c.CoreV1().Pods("").Watch(ctx, o) },
	}

	i := cache.NewSharedIndexInformer(lw, &core.Pod{}, 30*time.Minute, cache.Indexers{
		podNodeNameIndexField: func(obj interface{}) ([]string, error) {
			p, ok := obj.(*core.Pod)
			if !ok {
				return []string{""}, nil
			}
			return []string{p.Spec.NodeName}, nil
		},
		podStatusIndexField: func(obj interface{}) ([]string, error) {
			p, ok := obj.(*core.Pod)
			if !ok {
				return []string{""}, nil
			}
			return []string{string(p.Status.Phase)}, nil
		},
		podClaimIndexField: func(obj interface{}) ([]string, error) {
			p, ok := obj.(*core.Pod)
			if !ok {
				return []string{""}, nil
			}
			claims := []string{}
			for _, v := range p.Spec.Volumes {
				if v.PersistentVolumeClaim != nil {
					claims = append(claims, p.Namespace+"/"+v.PersistentVolumeClaim.ClaimName)
				}
			}
			return claims, nil
		},
	})
	return &PodWatch{i}
}

func (w *PodWatch) ListPodsForNode(nodeName string) ([]*core.Pod, error) {
	if !w.HasSynced() {
		return nil, errors.New("pod informer not yet synced")
	}
	objs, err := w.GetIndexer().ByIndex(podNodeNameIndexField, nodeName)
	if err != nil {
		return nil, err
	}
	pods := make([]*core.Pod, len(objs))
	for i := range objs {
		p, ok := objs[i].(*core.Pod)
		if !ok {
			return nil, errors.New("unexpected object type in Pod store")
		}
		pods[i] = p
	}
	sort.Sort(PodsSortedByName(pods))
	return pods, nil
}

func (w *PodWatch) ListPodsByStatus(podStatus string) ([]*core.Pod, error) {
	if !w.HasSynced() {
		return nil, errors.New("pod informer not yet synced")
	}
	objs, err := w.GetIndexer().ByIndex(podStatusIndexField, podStatus)
	if err != nil {
		return nil, err
	}
	pods := make([]*core.Pod, len(objs))
	for i := range objs {
		p, ok := objs[i].(*core.Pod)
		if !ok {
			return nil, errors.New("unexpected object type in Pod store")
		}
		pods[i] = p
	}
	sort.Sort(PodsSortedByName(pods))
	return pods, nil
}

func (w *PodWatch) GetPodCount() (int, error) {
	if !w.HasSynced() {
		return 0, errors.New("pod informer not yet synced")
	}
	count := len(w.GetStore().List())
	return count, nil
}

func (w *PodWatch) ListPodsForClaim(namespace, claimName string) ([]*core.Pod, error) {
	if !w.HasSynced() {
		return nil, errors.New("pod informer not yet synced")
	}
	objs, err := w.GetIndexer().ByIndex(podClaimIndexField, namespace+"/"+claimName)
	if err != nil {
		return nil, err
	}
	pods := make([]*core.Pod, len(objs))
	for i := range objs {
		p, ok := objs[i].(*core.Pod)
		if !ok {
			return nil, errors.New("unexpected object type in Pod store")
		}
		pods[i] = p
	}
	sort.Sort(PodsSortedByName(pods))
	return pods, nil
}

type PodsSortedByName []*core.Pod

func (a PodsSortedByName) Len() int           { return len(a) }
func (a PodsSortedByName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a PodsSortedByName) Less(i, j int) bool { return strings.Compare(a[i].Name, a[j].Name) > 0 }

type StatefulSetStore interface {
	SyncedStore
	// Get sts by name
	Get(namespace, name string) (*v1.StatefulSet, error)
}

// A StatefulSetWatch is a cache of sts resources that notifies registered
// handlers when its contents change.
type StatefulSetWatch struct {
	cache.SharedInformer
}

var _ StatefulSetStore = &StatefulSetWatch{}

// NewStatefulsetWatch creates a watch on sts resources.
func NewStatefulsetWatch(ctx context.Context, c kubernetes.Interface) *StatefulSetWatch {
	lw := &cache.ListWatch{
		ListFunc:  func(o meta.ListOptions) (runtime.Object, error) { return c.AppsV1().StatefulSets("").List(ctx, o) },
		WatchFunc: func(o meta.ListOptions) (watch.Interface, error) { return c.AppsV1().StatefulSets("").Watch(ctx, o) },
	}

	i := cache.NewSharedInformer(lw, &v1.StatefulSet{}, 30*time.Minute)
	return &StatefulSetWatch{i}
}

func (s StatefulSetWatch) Get(namespace, name string) (*v1.StatefulSet, error) {
	obj, exists, err := s.GetStore().GetByKey(namespace + "/" + name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, apierrors.NewNotFound(v1.Resource("statefulset"), name)
	}
	if obj != nil {
		sts, ok := obj.(*v1.StatefulSet)
		if ok {
			return sts, nil
		}
		if !ok {
			return nil, errors.New("Failed to cast object from store to Statefulset.")
		}
	}
	return nil, apierrors.NewNotFound(v1.Resource("statefulset"), name)
}

type PersistentVolumeStore interface {
	SyncedStore
	// Get the PV associated with a node
	GetPVForNode(node *core.Node) []*core.PersistentVolume
}

// A PersistentVolumeWatch is a cache of pv resources that notifies registered
// handlers when its contents change.
type PersistentVolumeWatch struct {
	cache.SharedIndexInformer
}

var _ PersistentVolumeStore = &PersistentVolumeWatch{}

const (
	pvNodeNameIndexField = "hostname"
	hostNameLabelKey     = "kubernetes.io/hostname"
)

// NewPersistentVolumeWatch creates a watch on persistentVolume resources.
func NewPersistentVolumeWatch(ctx context.Context, c kubernetes.Interface) *PersistentVolumeWatch {
	lw := &cache.ListWatch{
		ListFunc:  func(o meta.ListOptions) (runtime.Object, error) { return c.CoreV1().PersistentVolumes().List(ctx, o) },
		WatchFunc: func(o meta.ListOptions) (watch.Interface, error) { return c.CoreV1().PersistentVolumes().Watch(ctx, o) },
	}

	i := cache.NewSharedIndexInformer(lw, &core.PersistentVolume{}, 30*time.Minute,
		cache.Indexers{pvNodeNameIndexField: func(obj interface{}) ([]string, error) {
			pv, ok := obj.(*core.PersistentVolume)
			if !ok {
				return nil, fmt.Errorf("Object is not a PersitentVolume")
			}
			return []string{pv.Labels[hostNameLabelKey]}, nil
		}},
	)
	return &PersistentVolumeWatch{i}
}

func (p *PersistentVolumeWatch) GetPVForNode(node *core.Node) []*core.PersistentVolume {
	hostname := node.Labels[hostNameLabelKey]
	objects, err := p.SharedIndexInformer.GetIndexer().ByIndex(pvNodeNameIndexField, hostname)
	if err != nil {
		return nil
	}
	result := []*core.PersistentVolume{}
	for _, pvObj := range objects {
		if pv, ok := pvObj.(*core.PersistentVolume); ok {
			result = append(result, pv)
		}
	}
	return result
}

type PersistentVolumeClaimStore interface {
	SyncedStore
	// Get the PVC associated with a node
	Get(namespace, name string) (*core.PersistentVolumeClaim, error)
}

// A PersistentVolumeClaimWatch is a cache of pvc resources that notifies registered
// handlers when its contents change.
type PersistentVolumeClaimWatch struct {
	cache.SharedInformer
}

var _ PersistentVolumeClaimStore = &PersistentVolumeClaimWatch{}

// NewPersistentVolumeClaimWatch creates a watch on persistentVolumeClaim resources.
func NewPersistentVolumeClaimWatch(ctx context.Context, c kubernetes.Interface) *PersistentVolumeClaimWatch {
	lw := &cache.ListWatch{
		ListFunc: func(o meta.ListOptions) (runtime.Object, error) {
			return c.CoreV1().PersistentVolumeClaims("").List(ctx, o)
		},
		WatchFunc: func(o meta.ListOptions) (watch.Interface, error) {
			return c.CoreV1().PersistentVolumeClaims("").Watch(ctx, o)
		},
	}

	i := cache.NewSharedInformer(lw, &core.PersistentVolumeClaim{}, 30*time.Minute)
	return &PersistentVolumeClaimWatch{i}
}

func (p *PersistentVolumeClaimWatch) Get(namespace, name string) (*core.PersistentVolumeClaim, error) {
	obj, exists, err := p.GetStore().GetByKey(namespace + "/" + name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, apierrors.NewNotFound(v1.Resource("persistentVolumeClaim"), name)
	}

	if obj != nil {
		pvc, ok := obj.(*core.PersistentVolumeClaim)
		if ok {
			return pvc, nil
		}
		if !ok {
			return nil, errors.New("Failed to cast object from store to PersistentVolumeClaim.")
		}
	}
	return nil, apierrors.NewNotFound(v1.Resource("persistentVolumeClaim"), name)
}

// RunStoreForTest can be used in test to get a running and synched store
func RunStoreForTest(ctx context.Context, kclient kubernetes.Interface) (store RuntimeObjectStore, closingFunc func()) {
	stopCh := make(chan struct{})
	stsWatch := NewStatefulsetWatch(ctx, kclient)
	podWatch := NewPodWatch(ctx, kclient)
	nodeWatch := NewNodeWatch(ctx, kclient)
	pvWatch := NewPersistentVolumeWatch(ctx, kclient)
	pvcWatch := NewPersistentVolumeClaimWatch(ctx, kclient)

	go stsWatch.Run(stopCh)
	go podWatch.Run(stopCh)
	go nodeWatch.Run(stopCh)
	go pvWatch.Run(stopCh)
	go pvcWatch.Run(stopCh)

	store = &RuntimeObjectStoreImpl{
		StatefulSetsStore:          stsWatch,
		PodsStore:                  podWatch,
		NodesStore:                 nodeWatch,
		PersistentVolumeStore:      pvWatch,
		PersistentVolumeClaimStore: pvcWatch,
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !store.HasSynced() {
			time.Sleep(10 * time.Millisecond)
		}
	}()
	wg.Wait()
	return store, func() { close(stopCh) }
}
