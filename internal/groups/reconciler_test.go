package groups

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/zapr"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	"go.uber.org/zap"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func NewTestRunnerFactory() *TestRunnerFactory {
	return &TestRunnerFactory{
		runCount: map[GroupKey]int{},
		stop:     make(chan struct{}),
	}
}

type TestRunnerFactory struct {
	runCount map[GroupKey]int
	stop     chan struct{}
	sync.RWMutex
}

func (t *TestRunnerFactory) Stop() {
	close(t.stop)
}

func (t *TestRunnerFactory) Run(r *RunnerInfo) error {
	t.Lock()
	t.runCount[r.Key] = t.runCount[r.Key] + 1
	t.Unlock()
	<-t.stop
	return nil
}

func (t *TestRunnerFactory) BuildRunner() Runner {
	return t
}

var _ RunnerFactory = &TestRunnerFactory{}

func TestNewGroupRegistry(t *testing.T) {

	RegisterMetrics(prometheus.NewRegistry())
	testLogger := zapr.NewLogger(zap.NewNop())
	tests := []struct {
		name                  string
		drainFactory          RunnerFactory
		drainCandidateFactory RunnerFactory
		keyGetterFactory      func(client.Client) GroupKeyGetter
		nodes                 []runtime.Object
		runCount              map[GroupKey]int
	}{
		{
			name:                  "test1",
			drainFactory:          NewTestRunnerFactory(),
			drainCandidateFactory: NewTestRunnerFactory(),
			keyGetterFactory: func(client client.Client) GroupKeyGetter {
				return NewGroupKeyFromNodeMetadata(client, testLogger, kubernetes.NoopEventRecorder{}, nil, nil, []string{"key"}, nil, "")
			},
			runCount: map[GroupKey]int{
				"g1": 1,
				"g2": 1,
			},
			nodes: []runtime.Object{
				&corev1.Node{
					ObjectMeta: meta.ObjectMeta{
						Name:              "node-g1-1",
						CreationTimestamp: meta.Time{Time: time.Now().Add(-time.Hour)},
						Labels:            map[string]string{"key": "g1"},
					},
				},
				&corev1.Node{
					ObjectMeta: meta.ObjectMeta{
						Name:              "node-g1-2",
						CreationTimestamp: meta.Time{Time: time.Now().Add(-time.Hour)},
						Labels:            map[string]string{"key": "g1"},
					},
				},
				&corev1.Node{
					ObjectMeta: meta.ObjectMeta{
						Name:              "node-g2-0",
						CreationTimestamp: meta.Time{Time: time.Now().Add(-time.Hour)},
						Labels:            map[string]string{"key": "g2"},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodeFilter := func(o interface{}) bool {
				return true
			}
			wrapper, err := k8sclient.NewFakeClient(k8sclient.FakeConf{Objects: tt.nodes})
			assert.NoError(t, err, "cannot initialize client wrapper")

			ch := make(chan struct{})
			defer close(ch)
			wrapper.Start(ch)

			keyGetter := tt.keyGetterFactory(wrapper.GetManagerClient())
			gr := NewGroupRegistry(context.Background(), wrapper.GetManagerClient(), testLogger, nil, keyGetter, tt.drainFactory, tt.drainCandidateFactory, nodeFilter, func() bool { return true }, 0)

			// inject all the objects
			for _, o := range tt.nodes {
				n := o.(*corev1.Node)
				gr.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: n.Name}})
			}

			// wait for the runs
			time.Sleep(time.Second)

			testFactory := tt.drainCandidateFactory.(*TestRunnerFactory)
			assert.Equal(t, tt.runCount, testFactory.runCount)
			assert.Equal(t, len(tt.runCount), gr.groupDrainCandidateRunner.countRunners())
			testFactory.Stop()

			// wait for the cleanup
			time.Sleep(100 * time.Millisecond)
			assert.Equal(t, 0, gr.groupDrainCandidateRunner.countRunners())
		})
	}
}
