package sorters

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	"github.com/planetlabs/draino/internal/kubernetes/utils"
	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCompareNoAnnotationDrainPriority(t *testing.T) {
	tests := []struct {
		name string
		n1   *corev1.Node
		n2   *corev1.Node
		want bool
	}{
		{
			name: "no labels x2", want: false,
			n1: &corev1.Node{ObjectMeta: meta.ObjectMeta{Name: "n1"}},
			n2: &corev1.Node{ObjectMeta: meta.ObjectMeta{Name: "n2"}},
		},
		{
			name: "no labels on 1", want: false,
			n1: &corev1.Node{ObjectMeta: meta.ObjectMeta{Name: "n1"}},
			n2: &corev1.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:        "n2",
					Annotations: map[string]string{AnnotationDrainPriorityPKey: ""},
				}},
		},
		{
			name: "no labels on 2", want: false,
			n1: &corev1.Node{ObjectMeta: meta.ObjectMeta{
				Name:        "n1",
				Annotations: map[string]string{AnnotationDrainPriorityPKey: ""},
			}},
			n2: &corev1.Node{ObjectMeta: meta.ObjectMeta{Name: "n2"}},
		},
		{
			name: "labels on both with empty value", want: false,
			n1: &corev1.Node{ObjectMeta: meta.ObjectMeta{
				Name:        "n1",
				Annotations: map[string]string{AnnotationDrainPriorityPKey: ""},
			}},
			n2: &corev1.Node{ObjectMeta: meta.ObjectMeta{
				Name:        "n2",
				Annotations: map[string]string{AnnotationDrainPriorityPKey: ""},
			}},
		},
		{
			name: "labels on both with empty value and numeric on 1", want: true,
			n1: &corev1.Node{ObjectMeta: meta.ObjectMeta{
				Name:        "n1",
				Annotations: map[string]string{AnnotationDrainPriorityPKey: "10"},
			}},
			n2: &corev1.Node{ObjectMeta: meta.ObjectMeta{
				Name:        "n2",
				Annotations: map[string]string{AnnotationDrainPriorityPKey: ""},
			}},
		},
		{
			name: "default value is 1, so setting a real value takes priority", want: true,
			n1: &corev1.Node{ObjectMeta: meta.ObjectMeta{
				Name:        "n1",
				Annotations: map[string]string{AnnotationDrainPriorityPKey: "1"},
			}},
			n2: &corev1.Node{ObjectMeta: meta.ObjectMeta{
				Name:        "n2",
				Annotations: map[string]string{AnnotationDrainPriorityPKey: ""},
			}},
		},
		{
			name: "default value is 0, so setting a real value takes priority", want: false,
			n1: &corev1.Node{ObjectMeta: meta.ObjectMeta{
				Name:        "n1",
				Annotations: map[string]string{AnnotationDrainPriorityPKey: ""},
			}},
			n2: &corev1.Node{ObjectMeta: meta.ObjectMeta{
				Name:        "n2",
				Annotations: map[string]string{AnnotationDrainPriorityPKey: "1"},
			}},
		},
		{
			name: "labels on both with empty value and numeric on 2", want: false,
			n1: &corev1.Node{ObjectMeta: meta.ObjectMeta{
				Name:        "n1",
				Annotations: map[string]string{AnnotationDrainPriorityPKey: ""},
			}},
			n2: &corev1.Node{ObjectMeta: meta.ObjectMeta{
				Name:        "n2",
				Annotations: map[string]string{AnnotationDrainPriorityPKey: "1"},
			}},
		},
		{
			name: "labels on both with values 1 bigger", want: true,
			n1: &corev1.Node{ObjectMeta: meta.ObjectMeta{
				Name:        "n1",
				Annotations: map[string]string{AnnotationDrainPriorityPKey: "10"},
			}},
			n2: &corev1.Node{ObjectMeta: meta.ObjectMeta{
				Name:        "n2",
				Annotations: map[string]string{AnnotationDrainPriorityPKey: "9"},
			}},
		},
		{
			name: "labels on both with values 1 smaller", want: false,
			n1: &corev1.Node{ObjectMeta: meta.ObjectMeta{
				Name:        "n1",
				Annotations: map[string]string{AnnotationDrainPriorityPKey: "8"},
			}},
			n2: &corev1.Node{ObjectMeta: meta.ObjectMeta{
				Name:        "n2",
				Annotations: map[string]string{AnnotationDrainPriorityPKey: "9"},
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objects := []runtime.Object{tt.n1, tt.n2}
			wrapper, err := k8sclient.NewFakeClient(k8sclient.FakeConf{Objects: objects})
			assert.NoError(t, err)
			fakeClient := fake.NewSimpleClientset(objects...)

			idx, err := index.New(context.Background(), wrapper.GetManagerClient(), wrapper.GetCache(), logr.Discard())
			assert.NoError(t, err)

			ch := make(chan struct{})
			defer close(ch)
			wrapper.Start(ch)

			store, closingFunc := kubernetes.RunStoreForTest(context.Background(), fakeClient)
			defer closingFunc()

			prioritizer := NewAnnotationPrioritizer(store, idx, kubernetes.NoopEventRecorder{}, logr.Discard())
			if got := prioritizer(tt.n1, tt.n2); got != tt.want {
				t.Errorf("CompareNodeAnnotationDrainPriority() = %v, want %v", got, tt.want)
			}
		})
	}
}

type prioResult = int

const (
	TestN1Prio prioResult = 1
	TestEqual  prioResult = 0
	TestN2Prio prioResult = -1
)

func TestCompareNoAnnotationDrainPriority_PodOrCtrlLabels(t *testing.T) {
	tests := []struct {
		name    string
		n1      *corev1.Node
		n2      *corev1.Node
		objects []runtime.Object
		// 1 - n1 has priority
		// 0 - both have same priority
		// -1 - n2 has priority
		want prioResult
	}{
		{
			name: "should not find any prio", want: TestEqual,
			n1: &corev1.Node{ObjectMeta: meta.ObjectMeta{Name: "n1"}},
			n2: &corev1.Node{ObjectMeta: meta.ObjectMeta{Name: "n2"}},
			objects: mergeArrs(
				createTestObjects(testConf{name: "p1", node: "n1"}),
				createTestObjects(testConf{name: "p2", node: "n2"}),
			),
		},
		{
			name: "should find priority on pod (left)", want: TestN1Prio,
			n1: &corev1.Node{ObjectMeta: meta.ObjectMeta{Name: "n1"}},
			n2: &corev1.Node{ObjectMeta: meta.ObjectMeta{Name: "n2"}},
			objects: mergeArrs(
				createTestObjects(testConf{name: "p1", node: "n1", podAnnotation: utils.StrPtr("1")}),
				createTestObjects(testConf{name: "p2", node: "n2"}),
			),
		},
		{
			name: "should find priority on pod (right)", want: TestN2Prio,
			n1: &corev1.Node{ObjectMeta: meta.ObjectMeta{Name: "n1"}},
			n2: &corev1.Node{ObjectMeta: meta.ObjectMeta{Name: "n2"}},
			objects: mergeArrs(
				createTestObjects(testConf{name: "p1", node: "n1"}),
				createTestObjects(testConf{name: "p2", node: "n2", podAnnotation: utils.StrPtr("1")}),
			),
		},
		{
			name: "should find same priority", want: TestEqual,
			n1: &corev1.Node{ObjectMeta: meta.ObjectMeta{Name: "n1"}},
			n2: &corev1.Node{ObjectMeta: meta.ObjectMeta{Name: "n2"}},
			objects: mergeArrs(
				createTestObjects(testConf{name: "p1", node: "n1", podAnnotation: utils.StrPtr("1")}),
				createTestObjects(testConf{name: "p2", node: "n2", podAnnotation: utils.StrPtr("1")}),
			),
		},
		{
			name: "should use highest prio in chain", want: TestN2Prio,
			n1: &corev1.Node{ObjectMeta: meta.ObjectMeta{Name: "n1"}},
			n2: &corev1.Node{ObjectMeta: meta.ObjectMeta{Name: "n2"}},
			objects: mergeArrs(
				createTestObjects(testConf{name: "p1", node: "n1", podAnnotation: utils.StrPtr("2")}),
				createTestObjects(testConf{name: "p2", node: "n2", podAnnotation: utils.StrPtr("1"), ctrlAnnotation: utils.StrPtr("10")}),
			),
		},
		{
			name: "should find same priority in chain", want: TestEqual,
			n1: &corev1.Node{ObjectMeta: meta.ObjectMeta{Name: "n1"}},
			n2: &corev1.Node{ObjectMeta: meta.ObjectMeta{Name: "n2"}},
			objects: mergeArrs(
				createTestObjects(testConf{name: "p1", node: "n1", podAnnotation: utils.StrPtr("10"), ctrlAnnotation: utils.StrPtr("2")}),
				createTestObjects(testConf{name: "p2", node: "n2", podAnnotation: utils.StrPtr("1"), ctrlAnnotation: utils.StrPtr("10")}),
			),
		},
		{
			name: "should find highest prio on n2", want: TestN2Prio,
			n1: &corev1.Node{ObjectMeta: meta.ObjectMeta{
				Name:        "n1",
				Annotations: map[string]string{AnnotationDrainPriorityPKey: "7"},
			}},
			n2: &corev1.Node{ObjectMeta: meta.ObjectMeta{
				Name:        "n2",
				Annotations: map[string]string{AnnotationDrainPriorityPKey: "20"},
			}},
			objects: mergeArrs(
				createTestObjects(testConf{name: "p1", node: "n1", podAnnotation: utils.StrPtr("10"), ctrlAnnotation: utils.StrPtr("2")}),
				createTestObjects(testConf{name: "p2", node: "n2", podAnnotation: utils.StrPtr("1"), ctrlAnnotation: utils.StrPtr("10")}),
			),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objects := append(tt.objects, tt.n1, tt.n2)
			wrapper, err := k8sclient.NewFakeClient(k8sclient.FakeConf{Objects: objects})
			assert.NoError(t, err)
			fakeClient := fake.NewSimpleClientset(objects...)

			idx, err := index.New(context.Background(), wrapper.GetManagerClient(), wrapper.GetCache(), logr.Discard())
			assert.NoError(t, err)

			ch := make(chan struct{})
			defer close(ch)
			wrapper.Start(ch)

			store, closingFunc := kubernetes.RunStoreForTest(context.Background(), fakeClient)
			defer closingFunc()

			prioritizer := NewAnnotationPrioritizer(store, idx, kubernetes.NoopEventRecorder{}, logr.Discard())

			prio := TestEqual
			switch {
			case prioritizer(tt.n1, tt.n2):
				prio = TestN1Prio
			case prioritizer(tt.n2, tt.n1):
				prio = TestN2Prio
			}

			if prio != tt.want {
				t.Errorf("CompareNodeAnnotationDrainPriority() = %v, want %v", prio, tt.want)
			}
		})
	}
}

type testConf struct {
	name           string
	node           string
	podAnnotation  *string
	ctrlAnnotation *string
}

func createTestObjects(conf testConf) []runtime.Object {
	ctrlAnnotations := map[string]string{}
	if conf.ctrlAnnotation != nil {
		ctrlAnnotations[AnnotationDrainPriorityPKey] = *conf.ctrlAnnotation
	}
	ctrlName := conf.name + "-ctrl"
	rsName := ctrlName + "-rs"
	ctrl := &appsv1.Deployment{
		ObjectMeta: meta.ObjectMeta{
			Name:        ctrlName,
			Namespace:   "default",
			Annotations: ctrlAnnotations,
		},
	}

	podAnnotations := map[string]string{}
	if conf.podAnnotation != nil {
		podAnnotations[AnnotationDrainPriorityPKey] = *conf.podAnnotation
	}
	pod := &corev1.Pod{
		ObjectMeta: meta.ObjectMeta{
			Name:        conf.name,
			Namespace:   "default",
			Annotations: podAnnotations,
			OwnerReferences: []meta.OwnerReference{
				{
					Name: rsName,
					Kind: "ReplicaSet",
				},
			},
		},
		Spec: corev1.PodSpec{
			NodeName: conf.node,
		},
	}
	return []runtime.Object{ctrl, pod}
}

func mergeArrs[T any](arrs ...[]T) []T {
	resLen := 0
	for _, arr := range arrs {
		resLen += len(arr)
	}

	res := make([]T, 0, resLen)
	for _, arr := range arrs {
		res = append(res, arr...)
	}

	return res
}
