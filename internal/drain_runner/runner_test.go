package drain_runner

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/planetlabs/draino/internal/groups"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	"github.com/planetlabs/draino/internal/protector"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	cachecr "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type failDrainer struct {
	kubernetes.NoopCordonDrainer
}

func (d *failDrainer) Drain(ctx context.Context, n *v1.Node) error { return errors.New("myerr") }

type testPreprocessor struct {
	isDone bool
}

func (_ *testPreprocessor) GetName() string {
	return "testPreprocessor"
}

func (p *testPreprocessor) IsDone(node *corev1.Node) (bool, error) {
	return p.isDone, nil
}

func TestDrainRunner(t *testing.T) {
	tests := []struct {
		Name          string
		Key           groups.GroupKey
		Node          *corev1.Node
		Preprocessors []DrainPreProzessor
		Drainer       kubernetes.Drainer

		ShoulHaveTaint  bool
		ExpectedTaint   k8sclient.DrainTaintValue
		ExpectedRetries int
	}{
		{
			Name:            "Should drain the node",
			Key:             "my-key",
			Node:            createNode("my-key", k8sclient.TaintDrainCandidate),
			Drainer:         &kubernetes.NoopCordonDrainer{},
			ShoulHaveTaint:  true,
			ExpectedTaint:   k8sclient.TaintDrained,
			ExpectedRetries: 0,
		},
		{
			Name:            "Should fail during drain and remove the candidate status from the node",
			Key:             "my-key",
			Node:            createNode("my-key", k8sclient.TaintDrainCandidate),
			Drainer:         &failDrainer{},
			ShoulHaveTaint:  false,
			ExpectedRetries: 1,
		},
		{
			Name:            "Should ignore node without taint",
			Key:             "my-key",
			Node:            createNode("foo", ""),
			Drainer:         &failDrainer{},
			ShoulHaveTaint:  false,
			ExpectedRetries: 0,
		},
		{
			Name:            "Should not act on node with different key",
			Key:             "my-key",
			Node:            createNode("foo", k8sclient.TaintDrainCandidate),
			Drainer:         &failDrainer{},
			ShoulHaveTaint:  true,
			ExpectedTaint:   k8sclient.TaintDrainCandidate,
			ExpectedRetries: 0,
		},
		{
			Name:            "Should wait for preprocessor to finish",
			Key:             "my-key",
			Node:            createNode("my-key", k8sclient.TaintDrainCandidate),
			Drainer:         &kubernetes.NoopCordonDrainer{},
			Preprocessors:   []DrainPreProzessor{&testPreprocessor{isDone: false}},
			ShoulHaveTaint:  true,
			ExpectedTaint:   k8sclient.TaintDrainCandidate,
			ExpectedRetries: 0,
		},
		{
			Name:            "Can finish if preprocessors are done",
			Key:             "my-key",
			Node:            createNode("my-key", k8sclient.TaintDrainCandidate),
			Drainer:         &kubernetes.NoopCordonDrainer{},
			Preprocessors:   []DrainPreProzessor{&testPreprocessor{isDone: true}},
			ShoulHaveTaint:  true,
			ExpectedTaint:   k8sclient.TaintDrained,
			ExpectedRetries: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {

			wrapper, err := k8sclient.NewFakeClient(k8sclient.FakeConf{
				Objects: []runtime.Object{tt.Node},
				Indexes: []k8sclient.WithIndex{
					func(_ client.Client, cache cachecr.Cache) error {
						return groups.InitSchedulingGroupIndexer(cache, groups.NewGroupKeyFromNodeMetadata([]string{"key"}, nil, ""))
					},
				},
			})
			assert.NoError(t, err)

			client := fake.NewSimpleClientset(tt.Node)
			store, cl := kubernetes.RunStoreForTest(context.Background(), client)
			defer cl()

			ch := make(chan struct{})
			defer close(ch)
			runner, err := NewFakeRunner(&FakeOptions{
				Chan:          ch,
				ClientWrapper: wrapper,
				Preprocessors: tt.Preprocessors,
				Drainer:       tt.Drainer,
				PVProtector:   protector.NewPVCProtector(store, zap.NewNop(), false),
			})

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			runner.handleGroup(ctx, &groups.RunnerInfo{Context: ctx, Key: tt.Key})
			assert.NoError(t, ctx.Err(), "context reached deadline")

			var node corev1.Node
			err = wrapper.GetManagerClient().Get(context.Background(), types.NamespacedName{Name: tt.Node.Name}, &node)
			assert.NoError(t, err)

			taint, exist := k8sclient.GetNLATaint(&node)
			if tt.ShoulHaveTaint {
				assert.True(t, exist)
				assert.Equal(t, tt.ExpectedTaint, taint.Value)
			} else {
				assert.False(t, exist)
			}

			drainAttempts := runner.retryWall.GetDrainRetryAttemptsCount(&node)
			assert.Equal(t, tt.ExpectedRetries, drainAttempts)

		})
	}
}

func createNode(key string, taintVal k8sclient.DrainTaintValue) *corev1.Node {
	taints := []corev1.Taint{}
	if taintVal != "" {
		taints = append(taints, *k8sclient.CreateNLATaint(taintVal, time.Now()))
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "foo-node",
			Labels: map[string]string{"key": key},
		},
		Spec: corev1.NodeSpec{
			Taints: taints,
		},
	}
}