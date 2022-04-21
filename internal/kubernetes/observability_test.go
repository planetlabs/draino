package kubernetes

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/fake"
)

func TestScopeObserverImpl_GetLabelUpdate(t *testing.T) {
	getNode := func(labelValue string) *v1.Node {
		return &v1.Node{
			ObjectMeta: meta.ObjectMeta{Labels: map[string]string{ConfigurationLabelKey: labelValue}},
		}
	}
	tests := []struct {
		name              string
		configName        string
		nodeFilterFunc    func(obj interface{}) bool
		podFilterFunc     PodFilterFunc
		objects           []runtime.Object
		node              *v1.Node
		expectedValue     string
		expectedOutOfDate bool
		expectedError     string
	}{
		{
			name:              "no label - not in-scope --> update to out of scope",
			configName:        "draino1",
			nodeFilterFunc:    func(obj interface{}) bool { return false }, // not in scope
			podFilterFunc:     NewPodFilters(),
			objects:           []runtime.Object{},
			node:              getNode(""),
			expectedValue:     OutOfScopeLabelValue,
			expectedOutOfDate: true,
		},
		{
			name:              "out of scope label - not in-scope --> no update",
			configName:        "draino1",
			nodeFilterFunc:    func(obj interface{}) bool { return false }, // not in scope
			podFilterFunc:     NewPodFilters(),
			objects:           []runtime.Object{},
			node:              getNode(OutOfScopeLabelValue),
			expectedValue:     OutOfScopeLabelValue,
			expectedOutOfDate: false,
		},
		{
			name:              "out of scope label - not in-scope (pod) --> no update",
			configName:        "draino1",
			nodeFilterFunc:    func(obj interface{}) bool { return true }, // in scope for node
			podFilterFunc:     func(p v1.Pod) (pass bool, reason string, err error) { return false, "test", nil },
			objects:           []runtime.Object{&v1.Pod{}},
			node:              getNode(OutOfScopeLabelValue),
			expectedValue:     OutOfScopeLabelValue,
			expectedOutOfDate: false,
		},
		{
			name:           "out of scope label - not in-scope (pod with error) --> error",
			configName:     "draino1",
			nodeFilterFunc: func(obj interface{}) bool { return true }, // in scope for node
			podFilterFunc: func(p v1.Pod) (pass bool, reason string, err error) {
				return true, "test", fmt.Errorf("error path test")
			},
			objects:           []runtime.Object{&v1.Pod{}},
			node:              getNode(OutOfScopeLabelValue),
			expectedValue:     OutOfScopeLabelValue,
			expectedOutOfDate: false,
			expectedError:     "error path test",
		},
		{
			name:              "no label - in-scope --> update needed",
			configName:        "draino1",
			nodeFilterFunc:    func(obj interface{}) bool { return true }, // in scope
			podFilterFunc:     NewPodFilters(),
			objects:           []runtime.Object{},
			node:              getNode(""),
			expectedValue:     "draino1",
			expectedOutOfDate: true,
		},
		{
			name:              "out of scope label - in-scope --> update needed",
			configName:        "draino1",
			nodeFilterFunc:    func(obj interface{}) bool { return true }, // in scope
			podFilterFunc:     NewPodFilters(),
			objects:           []runtime.Object{},
			node:              getNode(OutOfScopeLabelValue),
			expectedValue:     "draino1",
			expectedOutOfDate: true,
		},
		{
			name:              "out of scope label - in-scope (node and pod) --> update needed",
			configName:        "draino1",
			nodeFilterFunc:    func(obj interface{}) bool { return true },                                        // in scope node
			podFilterFunc:     func(p v1.Pod) (pass bool, reason string, err error) { return true, "test", nil }, // in scope pod
			objects:           []runtime.Object{&v1.Pod{}},
			node:              getNode(OutOfScopeLabelValue),
			expectedValue:     "draino1",
			expectedOutOfDate: true,
		},
		{
			name:              "label present - in-scope --> no update needed",
			configName:        "draino1",
			nodeFilterFunc:    func(obj interface{}) bool { return true }, // in scope
			podFilterFunc:     NewPodFilters(),
			objects:           []runtime.Object{},
			node:              getNode("draino1"),
			expectedValue:     "draino1",
			expectedOutOfDate: false,
		},
		{
			name:              "other label present - in-scope --> update needed",
			configName:        "draino1",
			nodeFilterFunc:    func(obj interface{}) bool { return true }, // in scope
			podFilterFunc:     NewPodFilters(),
			objects:           []runtime.Object{},
			node:              getNode("draino2"),
			expectedValue:     "draino1,draino2",
			expectedOutOfDate: true,
		},
		{
			name:              "label present - not in-scope --> update needed",
			configName:        "draino1",
			nodeFilterFunc:    func(obj interface{}) bool { return false }, // not in scope
			podFilterFunc:     NewPodFilters(),
			objects:           []runtime.Object{},
			node:              getNode("draino1"),
			expectedValue:     OutOfScopeLabelValue,
			expectedOutOfDate: true,
		},
		{
			name:              "label present with other - not in-scope --> update needed",
			configName:        "draino1",
			nodeFilterFunc:    func(obj interface{}) bool { return false }, // not in scope
			podFilterFunc:     NewPodFilters(),
			objects:           []runtime.Object{},
			node:              getNode("draino1,other-draino"),
			expectedValue:     "other-draino",
			expectedOutOfDate: true,
		},
		{
			name:              "sorts values",
			configName:        "draino1",
			nodeFilterFunc:    func(obj interface{}) bool { return true }, // in scope
			podFilterFunc:     NewPodFilters(),
			objects:           []runtime.Object{},
			node:              getNode("draino2,draino1"),
			expectedValue:     "draino1,draino2",
			expectedOutOfDate: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kclient := fake.NewSimpleClientset(tt.objects...)
			runtimeObjectStore, closeFunc := RunStoreForTest(kclient)
			defer closeFunc()
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

			actualValue, actualOutOfDate, err := s.getLabelUpdate(tt.node)
			if tt.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedValue, actualValue)
				assert.Equal(t, tt.expectedOutOfDate, actualOutOfDate)
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
		objects        []runtime.Object
		validationFunc func(node *v1.Node) bool
	}{
		{
			name:           "updates when out of date",
			configName:     "draino1",
			nodeFilterFunc: func(obj interface{}) bool { return true },
			objects: []runtime.Object{
				&v1.Node{
					ObjectMeta: meta.ObjectMeta{
						Name: "node1",
					},
				},
			},
			nodeName: "node1",
			validationFunc: func(node *v1.Node) bool {
				return node.Labels[ConfigurationLabelKey] == "draino1"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kclient := fake.NewSimpleClientset(tt.objects...)
			runtimeObjectStore, closeFunc := RunStoreForTest(kclient)
			defer closeFunc()
			s := &DrainoConfigurationObserverImpl{
				kclient:            kclient,
				runtimeObjectStore: runtimeObjectStore,
				configName:         tt.configName,
				conditions:         tt.conditions,
				nodeFilterFunc:     tt.nodeFilterFunc,
				podFilterFunc:      NewPodFilters(),
				logger:             zap.NewNop(),
			}
			err := s.updateNodeLabels(tt.nodeName)
			require.NoError(t, err)
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
