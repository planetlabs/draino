package filters

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/planetlabs/draino/internal/kubernetes"
)

func TestNewPodFilter(t *testing.T) {
	podFilterFuncBasedOnNodeName := func(p corev1.Pod) (pass bool, reason string, err error) {
		if strings.HasSuffix(p.Spec.NodeName, "ok") {
			return true, "", nil
		}
		return false, "testReason", nil
	}

	n1ok := &corev1.Node{
		ObjectMeta: v1.ObjectMeta{
			Name: "n1-ok",
		},
	}
	n2Notok := &corev1.Node{
		ObjectMeta: v1.ObjectMeta{
			Name: "n2-no",
		},
	}
	p1 := &corev1.Pod{
		ObjectMeta: v1.ObjectMeta{
			Name: "p1",
		},
		Spec: corev1.PodSpec{
			NodeName: "n1-ok",
		},
	}
	p2 := &corev1.Pod{
		ObjectMeta: v1.ObjectMeta{
			Name: "p2",
		},
		Spec: corev1.PodSpec{
			NodeName: "n2-no",
		},
	}
	tests := []struct {
		name          string
		podFilterFunc kubernetes.PodFilterFunc
		objects       []runtime.Object
		nodes         []*corev1.Node
		want          []*corev1.Node
	}{
		{
			name:          "test pod filter",
			podFilterFunc: podFilterFuncBasedOnNodeName,
			objects:       []runtime.Object{n1ok, n2Notok, p1, p2},
			nodes:         []*corev1.Node{n1ok, n2Notok},
			want:          []*corev1.Node{n1ok},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, closingFunc := kubernetes.RunStoreForTest(context.Background(), fake.NewSimpleClientset(tt.objects...))
			defer closingFunc()
			f := NewPodFilter(zapr.NewLogger(zap.NewNop()), tt.podFilterFunc, store)
			if got := f.Filter(context.Background(), tt.nodes); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("checkPodFilters() = %v, want %v", got, tt.want)
			}
		})
	}
}
