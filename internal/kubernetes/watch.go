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
}

type RuntimeObjectStoreImpl struct {
	NodesStore        NodeStore
	PodsStore         PodStore
	StatefulSetsStore StatefulSetStore
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

func (r *RuntimeObjectStoreImpl) HasSynced() bool {
	return r.NodesStore.HasSynced() && r.StatefulSetsStore.HasSynced() && r.PodsStore.HasSynced()
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
func NewNodeWatch(c kubernetes.Interface, rs ...cache.ResourceEventHandler) *NodeWatch {
	lw := &cache.ListWatch{
		ListFunc:  func(o meta.ListOptions) (runtime.Object, error) { return c.CoreV1().Nodes().List(o) },
		WatchFunc: func(o meta.ListOptions) (watch.Interface, error) { return c.CoreV1().Nodes().Watch(o) },
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
		return nil, fmt.Errorf("node %s does not exist", name)
	}
	return o.(*core.Node), nil
}

// ListNodes List the nodes of the store
func (w *NodeWatch) ListNodes() []*core.Node {
	list := w.GetStore().List()
	nodes := make([]*core.Node, len(list))
	for i, o := range list {
		nodes[i] = o.(*core.Node)
	}
	return nodes
}

type PodStore interface {
	SyncedStore
	// List all the pods of a given node
	ListPodsForNode(nodeName string) ([]*core.Pod, error)
	ListPodsByStatus(podStatus string) ([]*core.Pod, error)
	GetPodCount() (int, error)
}

// A PodWatch is a cache of node resources that notifies registered
// handlers when its contents change.
type PodWatch struct {
	cache.SharedIndexInformer
}

var _ PodStore = &PodWatch{}

const podNodeNameIndexField = ".spec.nodeName"
const podStatusIndexField = ".status.phase"

// NewPodWatch creates a watch on pod resources. Pods are cached and the
// provided ResourceEventHandlers are called when the cache changes.
func NewPodWatch(c kubernetes.Interface) *PodWatch {
	lw := &cache.ListWatch{
		ListFunc:  func(o meta.ListOptions) (runtime.Object, error) { return c.CoreV1().Pods("").List(o) },
		WatchFunc: func(o meta.ListOptions) (watch.Interface, error) { return c.CoreV1().Pods("").Watch(o) },
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
	for i, obj := range objs {
		p, ok := obj.(*core.Pod)
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
	for i, obj := range objs {
		p, ok := obj.(*core.Pod)
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

type PodsSortedByName []*core.Pod

func (a PodsSortedByName) Len() int           { return len(a) }
func (a PodsSortedByName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a PodsSortedByName) Less(i, j int) bool { return strings.Compare(a[i].Name, a[j].Name) > 0 }

type StatefulSetStore interface {
	SyncedStore
	// List all the pods of a given node
	Get(namespace, name string) (*v1.StatefulSet, error)
}

// A PodWatch is a cache of node resources that notifies registered
// handlers when its contents change.
type StatefulSetWatch struct {
	cache.SharedInformer
}

var _ StatefulSetStore = &StatefulSetWatch{}

// NewStatefulsetWatch creates a watch on sts resources.
func NewStatefulsetWatch(c kubernetes.Interface) *StatefulSetWatch {
	lw := &cache.ListWatch{
		ListFunc:  func(o meta.ListOptions) (runtime.Object, error) { return c.AppsV1().StatefulSets("").List(o) },
		WatchFunc: func(o meta.ListOptions) (watch.Interface, error) { return c.AppsV1().StatefulSets("").Watch(o) },
	}

	i := cache.NewSharedInformer(lw, &v1.StatefulSet{}, 30*time.Minute)
	return &StatefulSetWatch{i}
}

func (s StatefulSetWatch) Get(namespace, name string) (*v1.StatefulSet, error) {
	obj, _, err := s.GetStore().GetByKey(namespace + "/" + name)
	if err != nil {
		return nil, err
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

// RunStoreForTest can be used in test to get a running and synched store
func RunStoreForTest(kclient kubernetes.Interface) (store RuntimeObjectStore, closingFunc func()) {
	stopCh := make(chan struct{})
	stsWatch := NewStatefulsetWatch(kclient)
	podWatch := NewPodWatch(kclient)
	nodeWatch := NewNodeWatch(kclient)

	go stsWatch.Run(stopCh)
	go podWatch.Run(stopCh)
	go nodeWatch.Run(stopCh)

	store = &RuntimeObjectStoreImpl{
		StatefulSetsStore: stsWatch,
		PodsStore:         podWatch,
		NodesStore:        nodeWatch,
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
