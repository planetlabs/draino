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
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/go-test/deep"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
)

const (
	name = "name"
)

type getByKeyFunc func(key string) (interface{}, bool, error)
type listFunc func() []interface{}
type predictableInformer struct {
	cache.SharedInformer
	fn     getByKeyFunc
	fnList listFunc
}

func (i *predictableInformer) GetStore() cache.Store {
	return &cache.FakeCustomStore{GetByKeyFunc: i.fn, ListFunc: i.fnList}
}

func TestNodeWatcher(t *testing.T) {
	cases := []struct {
		name    string
		fn      getByKeyFunc
		want    *core.Node
		wantErr bool
	}{
		{
			name: "NodeExists",
			fn: func(k string) (interface{}, bool, error) {
				return &core.Node{}, true, nil
			},
			want: &core.Node{},
		},
		{
			name: "NodeDoesNotExist",
			fn: func(k string) (interface{}, bool, error) {
				return nil, false, nil
			},
			wantErr: true,
		},
		{
			name: "ErrorGettingNode",
			fn: func(k string) (interface{}, bool, error) {
				return nil, false, errors.New("boom")
			},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			i := &predictableInformer{fn: tc.fn}
			w := &NodeWatch{i}
			got, err := w.Get(name)
			if err != nil {
				if tc.wantErr {
					return
				}
				t.Errorf("w.Get(%v): %v", name, err)
			}

			if diff := deep.Equal(tc.want, got); diff != nil {
				t.Errorf("w.Get(%v): want != got %v", name, diff)
			}
		})
	}
}

func TestNodeWatch_ListNodes(t *testing.T) {
	tests := []struct {
		name string
		fc   listFunc
		want int
	}{
		{
			name: "empty",
			fc:   func() []interface{} { return []interface{}{} },
			want: 0,
		},
		{
			name: "one",
			fc: func() []interface{} {
				return []interface{}{&core.Node{}}
			},
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := &predictableInformer{fnList: tt.fc}
			w := &NodeWatch{SharedInformer: i}
			if got := len(w.ListNodes()); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("len(ListNodes()) = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPodWatch_ListPodsForNode(t *testing.T) {
	tests := []struct {
		name     string
		nodeName string
		objects  []runtime.Object
		want     []*core.Pod
	}{
		{
			name:     "empty",
			nodeName: "node1",
			objects: []runtime.Object{
				&core.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node1",
					},
				},
			},
			want: []*core.Pod{},
		},
		{
			name:     "pod1 on node1",
			nodeName: "node1",
			objects: []runtime.Object{
				&core.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node1",
					},
				},
				&core.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pod1",
					},
				},
				&core.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "podSched",
					},
					Spec: core.PodSpec{
						NodeName: "node1",
					},
				},
			},
			want: []*core.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "podSched",
					},
					Spec: core.PodSpec{
						NodeName: "node1",
					},
				},
			},
		},
		{
			name:     "pod1 on node1",
			nodeName: "node1",
			objects: []runtime.Object{
				&core.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node1",
					},
				},
				&core.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pod1",
					},
				},
				&core.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "podSched",
					},
					Spec: core.PodSpec{
						NodeName: "node1",
					},
				},
				&core.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "podSched2",
					},
					Spec: core.PodSpec{
						NodeName: "node1",
					},
				},
			},
			want: []*core.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "podSched",
					},
					Spec: core.PodSpec{
						NodeName: "node1",
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "podSched2",
					},
					Spec: core.PodSpec{
						NodeName: "node1",
					},
				},
			},
		},
		{
			name:     "no pod on node2",
			nodeName: "node2",
			objects: []runtime.Object{
				&core.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node2",
					},
				},
				&core.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pod1",
					},
				},
				&core.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pod2",
					},
					Spec: core.PodSpec{
						NodeName: "node1",
					},
				},
			},
			want: []*core.Pod{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewPodWatch(context.Background(), fake.NewSimpleClientset(tt.objects...))
			stop := make(chan struct{})
			defer close(stop)
			go w.SharedIndexInformer.Run(stop)
			// Wait for the informer to sync
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for !w.HasSynced() {
					time.Sleep(100 * time.Millisecond)
				}
			}()
			wg.Wait()

			// Test the function
			got, _ := w.ListPodsForNode(tt.nodeName)
			sort.Sort(PodsSortedByName(tt.want))
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ListPodsForNode() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_GetPVForNode(t *testing.T) {
	hostname := "ip-10-128-208-156"
	node0 := &core.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "ip-10-128-208-156.ec2.internal",
			Labels: map[string]string{hostNameLabelKey: hostname},
		},
	}
	nodeOther := &core.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "ip-10-123-231-001.ec2.internal",
			Labels: map[string]string{hostNameLabelKey: "ip-10-123-231-001"},
		},
	}
	pv0 := &core.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "pv0",
			Labels: map[string]string{hostNameLabelKey: "ip-10-128-208-156"},
		},
		Spec:   core.PersistentVolumeSpec{},
		Status: core.PersistentVolumeStatus{},
	}

	tests := []struct {
		name    string
		objects []runtime.Object
		node    *core.Node
		want    []*core.PersistentVolume
	}{
		{
			name:    "nothing",
			node:    nodeOther,
			objects: []runtime.Object{nodeOther},
			want:    []*core.PersistentVolume{},
		},
		{
			name:    "no match",
			node:    nodeOther,
			objects: []runtime.Object{pv0, nodeOther},
			want:    []*core.PersistentVolume{},
		},
		{
			name:    "match",
			node:    node0,
			objects: []runtime.Object{pv0, node0},
			want:    []*core.PersistentVolume{pv0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kclient := fake.NewSimpleClientset(tt.objects...)
			store, closeCh := RunStoreForTest(context.Background(), kclient)
			defer closeCh()
			if got := store.PersistentVolumes().GetPVForNode(tt.node); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetPVForNode() = %v, want %v", got, tt.want)
			}
		})
	}
}
