package groups

import (
	"context"
	"github.com/go-logr/zapr"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fakeclient "k8s.io/client-go/kubernetes/fake"
	"testing"
)

func TestGroupKeyFromMetadata_GetGroupKey(t *testing.T) {
	tests := []struct {
		name                       string
		labelsKeys                 []string
		annotationKeys             []string
		groupOverrideAnnotationKey string
		node                       *v1.Node
		want                       GroupKey
	}{
		{
			name:       "empty 2 keys labels",
			labelsKeys: []string{"L1", "L2"},
			node: &v1.Node{
				ObjectMeta: meta.ObjectMeta{
					Labels:      nil,
					Annotations: nil,
				},
			},
			want: GroupKey("#"),
		},
		{
			name:           "empty 2 keys labels 1 annotation",
			labelsKeys:     []string{"L1", "L2"},
			annotationKeys: []string{"A1"},
			node: &v1.Node{
				ObjectMeta: meta.ObjectMeta{
					Labels:      nil,
					Annotations: nil,
				},
			},
			want: GroupKey("##"),
		},
		{
			name:           "values 2 keys labels 2 annotation",
			labelsKeys:     []string{"L1", "L2"},
			annotationKeys: []string{"A1", "A2"},
			node: &v1.Node{
				ObjectMeta: meta.ObjectMeta{
					Labels:      map[string]string{"L0": "l0", "L1": "l1", "L2": "l2", "L3": "l3"},
					Annotations: map[string]string{"A0": "a0", "A1": "a1", "A2": "a2", "A3": "a3"},
				},
			},
			want: GroupKey("l1#l2#a1#a2"),
		},
		{
			name:                       "override",
			labelsKeys:                 []string{"L1", "L2"},
			annotationKeys:             []string{"A1", "A2"},
			groupOverrideAnnotationKey: "ZZZ",
			node: &v1.Node{
				ObjectMeta: meta.ObjectMeta{
					Labels:      map[string]string{"L0": "l0", "L1": "l1", "L2": "l2", "L3": "l3"},
					Annotations: map[string]string{"AAA": "aaa", "A0": "a0", "A1": "a1", "A2": "a2", "A3": "a3", "ZZZ": "zzz,xxx"},
				},
			},
			want: GroupKey("zzz#xxx"),
		},
		{
			name:                       "pod override",
			labelsKeys:                 []string{"L1", "L2"},
			groupOverrideAnnotationKey: "ZZZ",
			node: &v1.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:        "the-node",
					Labels:      nil,
					Annotations: map[string]string{"AAA": "aaa", "A0": "a0", "A1": "a1", "A2": "a2", "A3": "a3", "ZZZ" + podOverrideAnnotationSuffix: "zzz,xxx"},
				},
			},
			want: GroupKey("zzz#xxx"),
		},
	}
	testLogger := zapr.NewLogger(zap.NewNop())
	for _, tt := range tests {
		func() { // to better handler defer statements
			wrapper, err := k8sclient.NewFakeClient(k8sclient.FakeConf{})
			assert.NoError(t, err)

			fakeKubeClient := fakeclient.NewSimpleClientset()
			store, closeFunc := kubernetes.RunStoreForTest(context.Background(), fakeKubeClient)
			defer closeFunc()

			fakeIndexer, err := index.New(wrapper.GetManagerClient(), wrapper.GetCache(), testLogger)
			assert.NoError(t, err)

			ch := make(chan struct{})
			defer close(ch)
			wrapper.Start(ch)

			if err != nil {
				t.Fatalf("can't create fakeIndexer: %#v", err)
			}

			t.Run(tt.name, func(t *testing.T) {
				g := NewGroupKeyFromNodeMetadata(nil, testLogger, kubernetes.NoopEventRecorder{}, fakeIndexer, store, tt.labelsKeys, tt.annotationKeys, tt.groupOverrideAnnotationKey)
				if got := g.GetGroupKey(tt.node); got != tt.want {
					t.Errorf("GetGroupKey() = %v, want %v", got, tt.want)
				}
			})
		}()
	}
}

func TestGroupKeyFromMetadata_ValidateGroupKey(t *testing.T) {
	const (
		keyOverride = "keyOverride"
	)

	tests := []struct {
		name       string
		node       *v1.Node
		wantValid  bool
		wantReason string
	}{
		{
			name: "no key",
			node: &v1.Node{
				ObjectMeta: meta.ObjectMeta{
					Annotations: map[string]string{"other": "other"},
				},
			},
			wantValid:  true,
			wantReason: "",
		},
		{
			name: "valid value",
			node: &v1.Node{
				ObjectMeta: meta.ObjectMeta{
					Annotations: map[string]string{keyOverride: "okvalue"},
				},
			},
			wantValid:  true,
			wantReason: "",
		},
		{
			name: "empty value",
			node: &v1.Node{
				ObjectMeta: meta.ObjectMeta{
					Annotations: map[string]string{keyOverride: ""},
				},
			},
			wantValid:  false,
			wantReason: "Empty value for keyOverride annotation, group override feature will be ignored",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupKeyFromMetadata{
				labelsKeys:                 nil,
				annotationKeys:             nil,
				groupOverrideAnnotationKey: keyOverride,
			}
			gotValid, gotReason := g.ValidateGroupKey(tt.node)
			assert.Equalf(t, tt.wantValid, gotValid, "ValidateGroupKey Valid field")
			assert.Equalf(t, tt.wantReason, gotReason, "Reason field")
		})
	}
}

func TestGroupKeyFromMetadata_GetGroupKeyFromPods(t *testing.T) {
	tests := []struct {
		name                       string
		objects                    []runtime.Object
		groupOverrideAnnotationKey string
		node                       *v1.Node
		want                       GroupKey
		override                   bool
	}{
		{
			name:                       "pod override",
			groupOverrideAnnotationKey: "ZZZ",
			node: &v1.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:        "the-node",
					Labels:      nil,
					Annotations: nil,
				},
			},
			objects: []runtime.Object{
				&v1.Pod{
					ObjectMeta: meta.ObjectMeta{
						Name:        "pod1",
						Annotations: map[string]string{"A3": "a3", "ZZZ": "zzz,xxx"},
					},
					Spec: v1.PodSpec{
						NodeName: "the-node",
					},
				},
			},
			want:     GroupKey("zzz#xxx"),
			override: true,
		},
		{
			name:                       "pod override double, reject",
			groupOverrideAnnotationKey: "ZZZ",
			node: &v1.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:        "the-node",
					Labels:      nil,
					Annotations: nil,
				},
			},
			objects: []runtime.Object{
				&v1.Pod{
					ObjectMeta: meta.ObjectMeta{
						Name:        "pod1",
						Annotations: map[string]string{"A3": "a3", "ZZZ": "zzz,xxx"},
					},
					Spec: v1.PodSpec{
						NodeName: "the-node",
					},
				},
				&v1.Pod{
					ObjectMeta: meta.ObjectMeta{
						Name:        "pod2",
						Annotations: map[string]string{"A3": "a3", "ZZZ": "zzz,yyy"},
					},
					Spec: v1.PodSpec{
						NodeName: "the-node",
					},
				},
			},
			want:     GroupKey(""),
			override: false,
		},
		{
			name:                       "no override",
			groupOverrideAnnotationKey: "ZZZ",
			node: &v1.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:        "the-node",
					Labels:      nil,
					Annotations: nil,
				},
			},
			objects: []runtime.Object{
				&v1.Pod{
					ObjectMeta: meta.ObjectMeta{
						Name:        "pod1",
						Annotations: map[string]string{"A3": "a3", "OTHER": "zzz,xxx"},
					},
					Spec: v1.PodSpec{
						NodeName: "the-node",
					},
				},
			},
			want:     GroupKey(""),
			override: false,
		},
	}
	testLogger := zapr.NewLogger(zap.NewNop())
	for _, tt := range tests {

		wrapper, err := k8sclient.NewFakeClient(k8sclient.FakeConf{Objects: tt.objects})
		assert.NoError(t, err)

		fakeKubeClient := fakeclient.NewSimpleClientset(tt.objects...)
		store, closeFunc := kubernetes.RunStoreForTest(context.Background(), fakeKubeClient)
		defer closeFunc()

		fakeIndexer, err := index.New(wrapper.GetManagerClient(), wrapper.GetCache(), testLogger)
		assert.NoError(t, err)

		ch := make(chan struct{})
		defer close(ch)
		wrapper.Start(ch)

		if err != nil {
			t.Fatalf("can't create fakeIndexer: %#v", err)
		}

		t.Run(tt.name, func(t *testing.T) {
			g := NewGroupKeyFromNodeMetadata(nil, testLogger, kubernetes.NoopEventRecorder{}, fakeIndexer, store, nil, nil, tt.groupOverrideAnnotationKey).(*GroupKeyFromMetadata)
			gotValue, override := g.getGroupOverrideFromPods(tt.node)
			assert.Equalf(t, tt.want, gotValue, "groupKey value")
			assert.Equalf(t, tt.override, override, "Override")
		})

	}
}
