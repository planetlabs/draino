package kubernetes

import (
	"fmt"
	"testing"
	"time"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/fake"
)

func TestScopeObserverImpl_IsAnnotationUpdateNeeded(t *testing.T) {
	tests := []struct {
		name           string
		configName     string
		nodeFilterFunc func(obj interface{}) bool
		podFilterFunc  PodFilterFunc
		objects        []runtime.Object
		node           *v1.Node
		want           bool
	}{
		{
			name:           "no annotation - not in-scope --> no update",
			configName:     "draino1",
			nodeFilterFunc: func(obj interface{}) bool { return false }, // not in scope
			podFilterFunc:  NewPodFilters(),
			objects:        []runtime.Object{},
			node:           &v1.Node{},
			want:           false,
		},
		{
			name:           "no annotation - not in-scope (pod) --> no update",
			configName:     "draino1",
			nodeFilterFunc: func(obj interface{}) bool { return true }, // in scope for node
			podFilterFunc:  func(p v1.Pod) (pass bool, reason string, err error) { return false, "test", nil },
			objects:        []runtime.Object{&v1.Pod{}},
			node:           &v1.Node{},
			want:           false,
		},
		{
			name:           "no annotation - not in-scope (pod with error) --> no update",
			configName:     "draino1",
			nodeFilterFunc: func(obj interface{}) bool { return true }, // in scope for node
			podFilterFunc: func(p v1.Pod) (pass bool, reason string, err error) {
				return true, "test", fmt.Errorf("error path test")
			},
			objects: []runtime.Object{&v1.Pod{}},
			node:    &v1.Node{},
			want:    false,
		},
		{
			name:           "no annotation - in-scope --> update needed",
			configName:     "draino1",
			nodeFilterFunc: func(obj interface{}) bool { return true }, // in scope
			podFilterFunc:  NewPodFilters(),
			objects:        []runtime.Object{},
			node:           &v1.Node{},
			want:           true,
		},
		{
			name:           "no annotation - in-scope (node and pod) --> update needed",
			configName:     "draino1",
			nodeFilterFunc: func(obj interface{}) bool { return true },                                        // in scope node
			podFilterFunc:  func(p v1.Pod) (pass bool, reason string, err error) { return true, "test", nil }, // in scope pod
			objects:        []runtime.Object{&v1.Pod{}},
			node:           &v1.Node{},
			want:           true,
		},
		{
			name:           "annotation present - in-scope --> no update needed",
			configName:     "draino1",
			nodeFilterFunc: func(obj interface{}) bool { return true }, // in scope
			podFilterFunc:  NewPodFilters(),
			objects:        []runtime.Object{},
			node: &v1.Node{
				ObjectMeta: meta.ObjectMeta{Annotations: map[string]string{ConfigurationAnnotationKey: "draino1"}},
			},
			want: false,
		},
		{
			name:           "other annotation present - in-scope --> update needed",
			configName:     "draino1",
			nodeFilterFunc: func(obj interface{}) bool { return true }, // in scope
			podFilterFunc:  NewPodFilters(),
			objects:        []runtime.Object{},
			node: &v1.Node{
				ObjectMeta: meta.ObjectMeta{Annotations: map[string]string{ConfigurationAnnotationKey: "draino-other"}},
			},
			want: true,
		},
		{
			name:           "annotation present - not in-scope --> update needed",
			configName:     "draino1",
			nodeFilterFunc: func(obj interface{}) bool { return false }, // not in scope
			podFilterFunc:  NewPodFilters(),
			objects:        []runtime.Object{},
			node: &v1.Node{
				ObjectMeta: meta.ObjectMeta{Annotations: map[string]string{ConfigurationAnnotationKey: "draino1"}},
			},
			want: true,
		},
		{
			name:           "annotation present with other - not in-scope --> update needed",
			configName:     "draino1",
			nodeFilterFunc: func(obj interface{}) bool { return false }, // not in scope
			podFilterFunc:  NewPodFilters(),
			objects:        []runtime.Object{},
			node: &v1.Node{
				ObjectMeta: meta.ObjectMeta{Annotations: map[string]string{ConfigurationAnnotationKey: "draino1,other-draino"}},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kclient := fake.NewSimpleClientset(tt.objects...)
			nodesWatch := NewNodeWatch(kclient)
			podsWatch := NewPodWatch(kclient)
			statefulsetsWatch := NewStatefulsetWatch(kclient)
			runtimeObjectStore := &RuntimeObjectStoreImpl{
				NodesStore:        nodesWatch,
				PodsStore:         podsWatch,
				StatefulSetsStore: statefulsetsWatch,
			}
			stopCh := make(chan struct{})
			defer close(stopCh)
			go nodesWatch.Run(stopCh)
			go podsWatch.Run(stopCh)
			go statefulsetsWatch.Run(stopCh)
			s := &DrainoConfigurationObserverImpl{
				kclient:            kclient,
				runtimeObjectStore: runtimeObjectStore,
				configName:         tt.configName,
				nodeFilterFunc:     tt.nodeFilterFunc,
				podFilterFunc:      tt.podFilterFunc,
				logger:             zap.NewNop(),
			}

			wait.PollImmediate(200*time.Millisecond, 5*time.Second, func() (done bool, err error) {
				return s.runtimeObjectStore.HasSynced(), nil
			})

			if got := s.IsAnnotationUpdateNeeded(tt.node); got != tt.want {
				t.Errorf("IsAnnotationUpdateNeeded() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestScopeObserverImpl_updateNodeAnnotationsAndLabels(t *testing.T) {
	tests := []struct {
		name           string
		nodeName       string
		nodeStore      NodeStore
		podStore       PodStore
		configName     string
		conditions     []SuppliedCondition
		nodeFilterFunc func(obj interface{}) bool
		podFilterFunc  PodFilterFunc
		objects        []runtime.Object
		validationFunc func(node *v1.Node) bool
		wantErr        bool
	}{
		{
			name:           "no annotation - in-scope --> update",
			configName:     "draino1",
			nodeFilterFunc: func(obj interface{}) bool { return true },
			podFilterFunc:  NewPodFilters(),
			objects: []runtime.Object{
				&v1.Node{
					ObjectMeta: meta.ObjectMeta{
						Name: "node1",
					},
				},
			},
			nodeName: "node1",
			validationFunc: func(node *v1.Node) bool {
				return node.Annotations[ConfigurationAnnotationKey] == "draino1"
			},
			wantErr: false,
		},
		{
			name:           "other annotation - in-scope --> update",
			configName:     "draino1",
			nodeFilterFunc: func(obj interface{}) bool { return true },
			podFilterFunc:  NewPodFilters(),
			objects: []runtime.Object{
				&v1.Node{
					ObjectMeta: meta.ObjectMeta{
						Name:        "node1",
						Annotations: map[string]string{ConfigurationAnnotationKey: "other"},
					},
				},
			},
			nodeName: "node1",
			validationFunc: func(node *v1.Node) bool {
				v := node.Annotations[ConfigurationAnnotationKey]
				return v == "draino1,other" || v == "other,draino1"
			},
			wantErr: false,
		},
		{
			name:           "contain annotation - not-in-scope --> update",
			configName:     "draino1",
			nodeFilterFunc: func(obj interface{}) bool { return false }, // not in scope
			podFilterFunc:  NewPodFilters(),
			objects: []runtime.Object{
				&v1.Node{
					ObjectMeta: meta.ObjectMeta{
						Name:        "node1",
						Annotations: map[string]string{ConfigurationAnnotationKey: "other,draino1"},
					},
				},
			},
			nodeName: "node1",
			validationFunc: func(node *v1.Node) bool {
				return node.Annotations[ConfigurationAnnotationKey] == "other"
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kclient := fake.NewSimpleClientset(tt.objects...)
			nodesWatch := NewNodeWatch(kclient)
			podsWatch := NewPodWatch(kclient)
			statefulsetsWatch := NewStatefulsetWatch(kclient)
			runtimeObjectStore := &RuntimeObjectStoreImpl{
				NodesStore:        nodesWatch,
				PodsStore:         podsWatch,
				StatefulSetsStore: statefulsetsWatch,
			}
			stopCh := make(chan struct{})
			defer close(stopCh)
			go nodesWatch.Run(stopCh)
			go podsWatch.Run(stopCh)
			go statefulsetsWatch.Run(stopCh)
			s := &DrainoConfigurationObserverImpl{
				kclient:            kclient,
				runtimeObjectStore: runtimeObjectStore,
				configName:         tt.configName,
				conditions:         tt.conditions,
				nodeFilterFunc:     tt.nodeFilterFunc,
				podFilterFunc:      tt.podFilterFunc,
				logger:             zap.NewNop(),
			}
			wait.PollImmediate(200*time.Millisecond, 5*time.Second, func() (done bool, err error) {
				return s.runtimeObjectStore.HasSynced(), nil
			})
			if err := s.updateNodeAnnotations(tt.nodeName); (err != nil) != tt.wantErr {
				t.Errorf("updateNodeAnnotations() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err := wait.PollImmediate(50*time.Millisecond, 5*time.Second,
				func() (bool, error) {
					n, err := kclient.CoreV1().Nodes().Get(tt.nodeName, meta.GetOptions{})
					if err != nil {
						return false, nil
					}
					if !tt.validationFunc(n) {
						return false, nil
					}
					return true, nil
				}); err != nil {
				t.Errorf("Validation failed")
				return
			}
		})
	}
}
