package pre_processor

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"
)

func TestPreActivitiesPreProcessor(t *testing.T) {
	now := time.Now()
	tests := []struct {
		Name           string
		DefaultTimeout time.Duration

		Node    *corev1.Node
		Objects []runtime.Object

		ExpectedIsDone bool
		ExpectedReason PreProcessNotDoneReason
	}{
		{
			Name:           "Should return true as the only pre activity is done",
			DefaultTimeout: time.Minute,
			Node: createPreActivityNode(createPreActivityNodeOptions{
				preActivities: map[string]string{
					PreActivityAnnotationPrefix + "foobar": PreActivityAnnotationDone,
				},
			}),
			ExpectedIsDone: true,
		},
		{
			Name:           "Should return true if no activity was found",
			DefaultTimeout: time.Minute,
			Node:           createPreActivityNode(createPreActivityNodeOptions{}),
			ExpectedIsDone: true,
		},
		{
			Name:           "Should return false if pre activity is in processing",
			DefaultTimeout: time.Minute,
			Node: createPreActivityNode(createPreActivityNodeOptions{
				preActivities: map[string]string{
					PreActivityAnnotationPrefix + "foobar": PreActivityAnnotationProcessing,
				},
			}),
			ExpectedIsDone: false,
			ExpectedReason: PreProcessNotDoneReasonProcessing,
		},
		{
			Name:           "Should return false if pre activity hasn't started yet",
			DefaultTimeout: time.Minute,
			Node: createPreActivityNode(createPreActivityNodeOptions{
				preActivities: map[string]string{
					PreActivityAnnotationPrefix + "foobar": PreActivityAnnotationNotStarted,
				},
			}),
			ExpectedIsDone: false,
			ExpectedReason: PreProcessNotDoneReasonProcessing,
		},
		{
			Name:           "Should return timeout error if timed out",
			DefaultTimeout: time.Minute,
			Node: createPreActivityNode(createPreActivityNodeOptions{
				NLATaintSince: now.Add(-15 * time.Minute),
				preActivities: map[string]string{
					PreActivityAnnotationPrefix + "foobar": PreActivityAnnotationProcessing,
				},
			}),
			ExpectedIsDone: false,
			ExpectedReason: PreProcessNotDoneReasonTimeout,
		},
		{
			Name:           "Should respect pre activity custom timeout",
			DefaultTimeout: time.Minute,
			Node: createPreActivityNode(createPreActivityNodeOptions{
				NLATaintSince: now.Add(-15 * time.Minute),
				preActivities: map[string]string{
					PreActivityAnnotationPrefix + "foobar":        PreActivityAnnotationProcessing,
					PreActivityTimeoutAnnotationPrefix + "foobar": "20m",
				},
			}),
			ExpectedIsDone: false,
			ExpectedReason: PreProcessNotDoneReasonProcessing,
		},
		{
			Name:           "Should fail if pre activity custom timeout was reached",
			DefaultTimeout: time.Minute,
			Node: createPreActivityNode(createPreActivityNodeOptions{
				NLATaintSince: now.Add(-21 * time.Minute),
				preActivities: map[string]string{
					PreActivityAnnotationPrefix + "foobar":        PreActivityAnnotationProcessing,
					PreActivityTimeoutAnnotationPrefix + "foobar": "20m",
				},
			}),
			ExpectedIsDone: false,
			ExpectedReason: PreProcessNotDoneReasonTimeout,
		},
		{
			Name:           "Should ignore invalid custom timeout value",
			DefaultTimeout: time.Minute,
			Node: createPreActivityNode(createPreActivityNodeOptions{
				NLATaintSince: now.Add(-15 * time.Minute),
				preActivities: map[string]string{
					PreActivityAnnotationPrefix + "foobar":        PreActivityAnnotationProcessing,
					PreActivityTimeoutAnnotationPrefix + "foobar": "20",
				},
			}),
			ExpectedIsDone: false,
			ExpectedReason: PreProcessNotDoneReasonTimeout,
		},
		{
			Name:           "Should ignore pre activity that only has a timeout",
			DefaultTimeout: time.Minute,
			Node: createPreActivityNode(createPreActivityNodeOptions{
				NLATaintSince: now.Add(-15 * time.Minute),
				preActivities: map[string]string{
					PreActivityTimeoutAnnotationPrefix + "foobar": "20m",
				},
			}),
			ExpectedIsDone: true,
		},
		{
			Name:           "Should respect multiple activities",
			DefaultTimeout: time.Minute,
			Node: createPreActivityNode(createPreActivityNodeOptions{
				preActivities: map[string]string{
					PreActivityAnnotationPrefix + "foobar": PreActivityAnnotationProcessing,
					PreActivityAnnotationPrefix + "other":  PreActivityAnnotationDone,
				},
			}),
			ExpectedIsDone: false,
			ExpectedReason: PreProcessNotDoneReasonProcessing,
		},
		{
			Name:           "Should return true if all the pre activities finished",
			DefaultTimeout: time.Minute,
			Node: createPreActivityNode(createPreActivityNodeOptions{
				preActivities: map[string]string{
					PreActivityAnnotationPrefix + "foobar": PreActivityAnnotationDone,
					PreActivityAnnotationPrefix + "other":  PreActivityAnnotationDone,
				},
			}),
			ExpectedIsDone: true,
		},
		{
			Name:           "Should timeout if one of the pre activities timed out",
			DefaultTimeout: time.Minute,
			Node: createPreActivityNode(createPreActivityNodeOptions{
				NLATaintSince: now.Add(-6 * time.Minute),
				preActivities: map[string]string{
					PreActivityAnnotationPrefix + "foobar":       PreActivityAnnotationProcessing,
					PreActivityAnnotationPrefix + "other":        PreActivityAnnotationDone,
					PreActivityTimeoutAnnotationPrefix + "other": "5m",
				},
			}),
			ExpectedIsDone: false,
			ExpectedReason: PreProcessNotDoneReasonTimeout,
		},
		{
			Name:           "Should fail if one of the pre activities failed",
			DefaultTimeout: time.Minute,
			Node: createPreActivityNode(createPreActivityNodeOptions{
				preActivities: map[string]string{
					PreActivityAnnotationPrefix + "foobar": PreActivityAnnotationDone,
					PreActivityAnnotationPrefix + "other":  PreActivityAnnotationFailed,
				},
			}),
			ExpectedIsDone: false,
			ExpectedReason: PreProcessNotDoneReasonFailure,
		},
		{
			Name:           "Should be able to find pre activities along the chain (node -> pod -> controller)",
			DefaultTimeout: time.Minute,
			Node: createPreActivityNode(createPreActivityNodeOptions{
				preActivities: map[string]string{
					PreActivityAnnotationPrefix + "foobar": PreActivityAnnotationDone,
				},
			}),
			Objects: []runtime.Object{
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pod",
						Namespace: "default",
						Annotations: map[string]string{
							PreActivityAnnotationPrefix + "pod": PreActivityAnnotationProcessing,
						},
					},
					Spec: corev1.PodSpec{NodeName: "test-node"},
				},
			},
			ExpectedIsDone: false,
			ExpectedReason: PreProcessNotDoneReasonProcessing,
		},
		{
			Name:           "Should fail if a pre activities along the chain failed (node -> pod -> controller)",
			DefaultTimeout: time.Minute,
			Node: createPreActivityNode(createPreActivityNodeOptions{
				preActivities: map[string]string{
					PreActivityAnnotationPrefix + "foobar": PreActivityAnnotationDone,
				},
			}),
			Objects: []runtime.Object{
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pod",
						Namespace: "default",
						Annotations: map[string]string{
							PreActivityAnnotationPrefix + "pod": PreActivityAnnotationFailed,
						},
					},
					Spec: corev1.PodSpec{NodeName: "test-node"},
				},
			},
			ExpectedIsDone: false,
			ExpectedReason: PreProcessNotDoneReasonFailure,
		},
		{
			Name:           "Should fail if a pre activities along the chain timed out (node -> pod -> controller)",
			DefaultTimeout: time.Minute,
			Node: createPreActivityNode(createPreActivityNodeOptions{
				NLATaintSince: now.Add(-6 * time.Minute),
				preActivities: map[string]string{
					PreActivityAnnotationPrefix + "foobar": PreActivityAnnotationDone,
				},
			}),
			Objects: []runtime.Object{
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pod",
						Namespace: "default",
						Annotations: map[string]string{
							PreActivityAnnotationPrefix + "pod":        PreActivityAnnotationProcessing,
							PreActivityTimeoutAnnotationPrefix + "pod": "5m",
						},
					},
					Spec: corev1.PodSpec{NodeName: "test-node"},
				},
			},
			ExpectedIsDone: false,
			ExpectedReason: PreProcessNotDoneReasonTimeout,
		},
	}

	logger := logr.Discard()
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			objects := append(tt.Objects, tt.Node)
			wrapper, err := k8sclient.NewFakeClient(k8sclient.FakeConf{Objects: objects})
			assert.NoError(t, err, "failed to create fake clients")

			recorder := kubernetes.NewEventRecorder(record.NewFakeRecorder(1000))
			kclientFake := fake.NewSimpleClientset(objects...)

			idx, err := index.New(wrapper.GetManagerClient(), wrapper.GetCache(), logger)
			assert.NoError(t, err, "failed to craete indexer")

			ctx, cancel := context.WithCancel(context.Background())
			store, closeStore := kubernetes.RunStoreForTest(ctx, kclientFake)
			defer cancel()
			defer closeStore()

			ch := make(chan struct{})
			defer close(ch)
			wrapper.Start(ch)

			preProcessor := NewPreActivitiesPreProcessor(idx, store, logger, recorder, clock.RealClock{}, tt.DefaultTimeout)

			done, reason, err := preProcessor.IsDone(ctx, tt.Node)
			assert.Equal(t, tt.ExpectedIsDone, done)
			assert.Equal(t, tt.ExpectedReason, reason)
			assert.NoError(t, err, "did not expect error from isDone")
		})
	}
}

type createPreActivityNodeOptions struct {
	hasNoNLATaint bool
	NLATaintSince time.Time
	preActivities map[string]string
}

func createPreActivityNode(opts createPreActivityNodeOptions) *corev1.Node {
	taints := []corev1.Taint{}
	if !opts.hasNoNLATaint {
		since := time.Now()
		if !opts.NLATaintSince.IsZero() {
			since = opts.NLATaintSince
		}
		taints = append(taints, *k8sclient.CreateNLATaint(k8sclient.TaintDrainCandidate, since))
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-node",
			Annotations: opts.preActivities,
		},
		Spec: corev1.NodeSpec{
			Taints: taints,
		},
	}
}
