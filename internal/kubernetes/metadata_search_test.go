package kubernetes

import (
	"context"
	"fmt"
	"github.com/go-logr/zapr"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	v1 "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fakeclient "k8s.io/client-go/kubernetes/fake"
	"sort"
	"testing"
)

func TestSearcAnnotationFromNodeAndThenPodOrController(t *testing.T) {
	testKey := "testKey"
	testValue := "testValue"
	nodeNoAnnotation := &core.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node1",
		},
	}
	nodeNoKey := &core.Node{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{},
			Name:        "node1",
		},
	}
	nodeWithKey := &core.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "node1",
			Annotations: map[string]string{testKey: testValue},
		},
	}
	podNoAnnotation := &core.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "no-annotation",
			Namespace: "ns",
			OwnerReferences: []metav1.OwnerReference{metav1.OwnerReference{
				Controller: &isController,
				Kind:       kindReplicaSet,
				Name:       deploymentName + "-xyz",
				APIVersion: "apps/v1",
			}},
		},
		Spec: core.PodSpec{
			NodeName: "node1",
		},
	}
	podNoKey := &core.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "no-key",
			Namespace:   "ns",
			Annotations: map[string]string{},
			OwnerReferences: []metav1.OwnerReference{metav1.OwnerReference{
				Controller: &isController,
				Kind:       kindReplicaSet,
				Name:       deploymentName + "-xyz",
				APIVersion: "apps/v1",
			}},
		},
		Spec: core.PodSpec{
			NodeName: "node1",
		},
	}
	podWithKey := &core.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "with-key",
			Namespace:   "ns",
			Annotations: map[string]string{testKey: testValue},
			OwnerReferences: []metav1.OwnerReference{metav1.OwnerReference{
				Controller: &isController,
				Kind:       kindReplicaSet,
				Name:       deploymentName + "-xyz",
				APIVersion: "apps/v1",
			}},
		},
		Spec: core.PodSpec{
			NodeName: "node1",
		},
	}
	DeploymentNoKey := &v1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploymentName,
			Namespace: "ns",
		},
	}
	DeploymentWithKey := &v1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        deploymentName,
			Namespace:   "ns",
			Annotations: map[string]string{testKey: testValue},
		},
	}

	pods := map[string]*core.Pod{
		podWithKey.Name:      podWithKey,
		podNoKey.Name:        podNoKey,
		podNoAnnotation.Name: podNoAnnotation,
	}

	tests := []struct {
		name    string
		node    *core.Node
		objects []runtime.Object
		want    map[string][]MetadataSearchResultItem[string]
		wantErr bool
	}{
		{
			name: "node no annotation",
			node: nodeNoAnnotation,
			want: map[string][]MetadataSearchResultItem[string]{},
		},
		{
			name: "node no key",
			node: nodeNoKey,
			want: map[string][]MetadataSearchResultItem[string]{},
		},
		{
			name: "node with key",
			node: nodeWithKey,
			want: map[string][]MetadataSearchResultItem[string]{testValue: {{
				Value:  testValue,
				Node:   nodeWithKey,
				NodeId: nodeWithKey.Name,
			}}},
		},
		{
			name: "node,pod no annotation, controller no key",
			node: nodeNoAnnotation,
			objects: []runtime.Object{
				podNoAnnotation, DeploymentNoKey,
			},
			want: map[string][]MetadataSearchResultItem[string]{},
		},
		{
			name: "node,pod,controller no key",
			node: nodeNoKey,
			want: map[string][]MetadataSearchResultItem[string]{},
			objects: []runtime.Object{
				podNoKey, DeploymentNoKey,
			},
		},
		{
			name: "node,pod,no key and controller with key",
			node: nodeNoKey,
			want: map[string][]MetadataSearchResultItem[string]{testValue: {{
				Value:        testValue,
				Pod:          podNoKey,
				OnController: true,
			}}},
			objects: []runtime.Object{
				podNoKey, DeploymentWithKey, nodeNoKey,
			},
		},
		{
			name: "node no key, pod,controller with key",
			node: nodeNoKey,
			want: map[string][]MetadataSearchResultItem[string]{testValue: {{
				Value:        testValue,
				Pod:          podWithKey,
				OnController: false,
			}}},
			objects: []runtime.Object{
				podWithKey, DeploymentWithKey,
			},
		},
	}
	testLogger := zapr.NewLogger(zap.NewNop())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			wrapper, err := k8sclient.NewFakeClient(k8sclient.FakeConf{Objects: tt.objects})
			assert.NoError(t, err)

			fakeKubeClient := fakeclient.NewSimpleClientset(tt.objects...)
			store, closeFunc := RunStoreForTest(context.Background(), fakeKubeClient)
			defer closeFunc()

			fakeIndexer, err := index.New(wrapper.GetManagerClient(), wrapper.GetCache(), testLogger)
			assert.NoError(t, err)

			ch := make(chan struct{})
			defer close(ch)
			wrapper.Start(ch)

			if err != nil {
				t.Fatalf("can't create fakeIndexer: %#v", err)
			}

			got, err := SearchAnnotationFromNodeAndThenPodOrController(context.Background(), fakeIndexer, store, func(s string) (string, error) { return s, nil }, testKey, tt.node, true, true)
			if tt.wantErr != (err != nil) {
				fmt.Printf("%sGetAnnotationFromNodeAndThenPodOrController() ERR: %#v", tt.name, err)
				t.Failed()
			}
			if err != nil {
				return
			}

			// be sure that we are using the same pointer for comparison
			for _, v := range got.Result {
				for i := range v {
					if v[i].Pod != nil {
						v[i].setPod(pods[v[i].Pod.Name])
					}
				}
			}
			for _, v := range tt.want {
				for i := range v {
					if v[i].Pod != nil {
						v[i].setPod(pods[v[i].Pod.Name])
					}
				}
			}

			assert.Equalf(t, tt.want, got.Result, "GetAnnotationFromNodeAndThenPodOrController()")
		})
	}
}

func TestMetadataSearch_ValuesWithoutDupe(t *testing.T) {
	type testCase[T any] struct {
		name    string
		a       MetadataSearch[T]
		wantOut []T
	}
	tests := []testCase[string]{
		{
			name: "dupe node pod",
			a: MetadataSearch[string]{
				Result: map[string][]MetadataSearchResultItem[string]{"a": {{Value: "a", Node: &core.Node{}}, {Value: "a", Pod: &core.Pod{}}}, "b": {{Value: "b"}}},
			},
			wantOut: []string{"a", "b"},
		},
		{
			name: "pod only",
			a: MetadataSearch[string]{
				Result: map[string][]MetadataSearchResultItem[string]{"a": {{Value: "a"}}, "b": {{Value: "b"}}, "c": {{Value: "c"}}},
			},
			wantOut: []string{"a", "b", "c"},
		},
		{
			name:    "nil",
			a:       MetadataSearch[string]{},
			wantOut: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sort.Strings(tt.wantOut)
			out := tt.a.ValuesWithoutDupe()
			sort.Strings(out)

			assert.Equalf(t, tt.wantOut, out, "ValuesWithoutDupe()")
		})
	}
}
