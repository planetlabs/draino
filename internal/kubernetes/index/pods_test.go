package index

import (
	"context"
	"testing"

	"github.com/go-logr/zapr"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	"go.uber.org/zap"

	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/slices"
	"k8s.io/apimachinery/pkg/runtime"
)

func Test_PodIndexer(t *testing.T) {
	tests := []struct {
		Name             string
		TestNodeName     string
		ExpectedPodNames []string
		Objects          []runtime.Object
	}{
		{
			Name:             "should find one pod on node",
			TestNodeName:     "my-node",
			ExpectedPodNames: []string{"my-test-pod"},
			Objects: []runtime.Object{
				createPod(createPodOptions{Name: "my-test-pod", Ns: "default", NodeName: "my-node"}),
				createPod(createPodOptions{Name: "my-foo-pod-2", Ns: "default", NodeName: "my-foo-node"}),
				createPod(createPodOptions{Name: "my-foo-pod", Ns: "default", NodeName: "my-foo-node"}),
			},
		},
		{
			Name:             "should find all pods for one node",
			TestNodeName:     "my-node",
			ExpectedPodNames: []string{"my-test-pod", "my-test-pod-2"},
			Objects: []runtime.Object{
				createPod(createPodOptions{Name: "my-test-pod", Ns: "default", NodeName: "my-node"}),
				createPod(createPodOptions{Name: "my-test-pod-2", Ns: "default", NodeName: "my-node"}),
				createPod(createPodOptions{Name: "my-foo-pod", Ns: "default", NodeName: "my-foo-node"}),
			},
		},
		{
			Name:             "should return empty array if nothing was found",
			TestNodeName:     "empty-node",
			ExpectedPodNames: []string{},
			Objects: []runtime.Object{
				createPod(createPodOptions{Name: "my-test-pod", Ns: "default", NodeName: "my-node"}),
				createPod(createPodOptions{Name: "my-test-pod-2", Ns: "default", NodeName: "my-node"}),
				createPod(createPodOptions{Name: "my-foo-pod", Ns: "default", NodeName: "my-foo-node"}),
			},
		},
		{
			Name:             "should not fail if no pods in cluster",
			TestNodeName:     "empty-node",
			ExpectedPodNames: []string{},
			Objects:          []runtime.Object{},
		},
	}

	testLogger := zapr.NewLogger(zap.NewNop())
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			ch := make(chan struct{})
			defer close(ch)
			wrapper, err := k8sclient.NewFakeClient(k8sclient.FakeConf{Objects: tt.Objects})

			informer, err := New(wrapper.GetManagerClient(), wrapper.GetCache(), testLogger)
			assert.NoError(t, err)

			wrapper.Start(ch)

			pods, err := informer.GetPodsByNode(context.TODO(), tt.TestNodeName)
			assert.NoError(t, err)

			assert.Equal(t, len(tt.ExpectedPodNames), len(pods), "received amount of pods to not match expected amount")
			for _, pod := range pods {
				assert.True(t, slices.Contains(tt.ExpectedPodNames, pod.GetName()), "found pod is not expected", pod.GetName())
			}
		})
	}
}
