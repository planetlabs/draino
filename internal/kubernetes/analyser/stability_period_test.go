package analyser

import (
	"context"
	v1 "k8s.io/api/apps/v1"
	"testing"
	"time"

	"github.com/go-logr/zapr"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fakeclient "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
)

func Test_getLatestDisruption(t *testing.T) {
	date14 := time.Date(2022, 11, 18, 14, 0, 0, 0, time.UTC)
	date15 := time.Date(2022, 11, 18, 15, 0, 0, 0, time.UTC)
	date16 := time.Date(2022, 11, 18, 16, 0, 0, 0, time.UTC)

	pdbBlocked := &policyv1.PodDisruptionBudget{
		ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "pdbblocked"},
		Status: policyv1.PodDisruptionBudgetStatus{
			Conditions: []meta.Condition{
				{
					Type:               policyv1.DisruptionAllowedCondition,
					Status:             meta.ConditionFalse,
					LastTransitionTime: meta.Time{Time: date14},
				},
			},
		},
	}
	pdbOk := &policyv1.PodDisruptionBudget{
		ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "pdbok"},
		Status: policyv1.PodDisruptionBudgetStatus{
			Conditions: []meta.Condition{
				{
					Type:               policyv1.DisruptionAllowedCondition,
					Status:             meta.ConditionTrue,
					LastTransitionTime: meta.Time{Time: date15},
				},
			},
		},
	}
	pdbOk2 := &policyv1.PodDisruptionBudget{
		ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "pdbok2"},
		Status: policyv1.PodDisruptionBudgetStatus{
			Conditions: []meta.Condition{
				{
					Type:               policyv1.DisruptionAllowedCondition,
					Status:             meta.ConditionTrue,
					LastTransitionTime: meta.Time{Time: date16},
				},
			},
		},
	}
	pdbNoCondition := &policyv1.PodDisruptionBudget{
		ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "pdbNoConditions", CreationTimestamp: meta.Time{Time: date16}},
		Status: policyv1.PodDisruptionBudgetStatus{
			Conditions: []meta.Condition{},
		},
	}
	pdbNoTransitionTime := &policyv1.PodDisruptionBudget{
		ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "pdbNoTransitionTime", CreationTimestamp: meta.Time{Time: date16}},
		Status: policyv1.PodDisruptionBudgetStatus{
			Conditions: []meta.Condition{
				{
					Type:               policyv1.DisruptionAllowedCondition,
					Status:             meta.ConditionTrue,
					LastTransitionTime: meta.Time{},
				},
			},
		},
	}
	pdbNoTransitionTimeButBlocked := &policyv1.PodDisruptionBudget{
		ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "pdbNoTransitionTime", CreationTimestamp: meta.Time{Time: date16}},
		Status: policyv1.PodDisruptionBudgetStatus{
			Conditions: []meta.Condition{
				{
					Type:               policyv1.DisruptionAllowedCondition,
					Status:             meta.ConditionFalse,
					LastTransitionTime: meta.Time{},
				},
			},
		},
	}
	testLogger := zapr.NewLogger(zap.NewNop())
	tests := []struct {
		name                  string
		pdbsForPods           []*policyv1.PodDisruptionBudget
		wantDisruptionAllowed bool
		wantStableSince       time.Time
		wantDisruptedPDB      *policyv1.PodDisruptionBudget
	}{
		{
			name:                  "nil",
			pdbsForPods:           nil,
			wantDisruptionAllowed: true,
			wantStableSince:       time.Time{},
			wantDisruptedPDB:      nil,
		},
		{
			name:                  "no pdb",
			pdbsForPods:           []*policyv1.PodDisruptionBudget{},
			wantDisruptionAllowed: true,
			wantStableSince:       time.Time{},
			wantDisruptedPDB:      nil,
		},
		{
			name:                  "2 pdbs with no disruption allowed",
			pdbsForPods:           []*policyv1.PodDisruptionBudget{pdbBlocked, pdbOk},
			wantDisruptionAllowed: false,
			wantStableSince:       time.Time{},
			wantDisruptedPDB:      pdbBlocked,
		},
		{
			name:                  "2 pdbs with disruption allowed",
			pdbsForPods:           []*policyv1.PodDisruptionBudget{pdbOk2, pdbOk},
			wantDisruptionAllowed: true,
			wantStableSince:       date16,
			wantDisruptedPDB:      pdbOk2,
		},
		{
			name:                  "no conditions",
			pdbsForPods:           []*policyv1.PodDisruptionBudget{pdbNoCondition, pdbOk},
			wantDisruptionAllowed: true,
			wantStableSince:       date16,
			wantDisruptedPDB:      pdbNoCondition,
		},
		{
			name:                  "no transitionTime",
			pdbsForPods:           []*policyv1.PodDisruptionBudget{pdbNoTransitionTime, pdbOk},
			wantDisruptionAllowed: true,
			wantStableSince:       date16,
			wantDisruptedPDB:      pdbNoTransitionTime,
		},
		{
			name:                  "no transitionTime and blocked",
			pdbsForPods:           []*policyv1.PodDisruptionBudget{pdbNoTransitionTimeButBlocked, pdbOk},
			wantDisruptionAllowed: false,
			wantStableSince:       time.Time{},
			wantDisruptedPDB:      pdbNoTransitionTimeButBlocked,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDisruptionAllowed, gotStableSince, gotDisruptedPDB := getLatestDisruption(testLogger, tt.pdbsForPods)
			assert.Equalf(t, tt.wantDisruptionAllowed, gotDisruptionAllowed, "disruptionAllowed")
			assert.Equalf(t, tt.wantStableSince, gotStableSince, "stableSince")
			assert.Equalf(t, tt.wantDisruptedPDB, gotDisruptedPDB, "pdb")
		})
	}
}

func Test_stabilityPeriodChecker_StabilityPeriodAcceptsDrain(t *testing.T) {
	now := time.Date(2022, 11, 19, 18, 00, 00, 00, time.UTC)
	stabilityPeriodOneHour := time.Hour
	stabilityPeriodNegativeOneHour := -time.Hour
	tests := []struct {
		name                   string
		objects                []runtime.Object
		node                   *corev1.Node
		defaultStabilityPeriod *time.Duration
		want                   bool
	}{
		{
			name:    "just node, can drain",
			objects: nil,
			node: &corev1.Node{
				ObjectMeta: meta.ObjectMeta{
					Name: "node1",
				},
			},
			want: true,
		},
		{
			name: "new pdb, using pdb creation timestamp with a stability period",
			objects: []runtime.Object{&corev1.Pod{
				ObjectMeta: meta.ObjectMeta{Name: "pod1", Namespace: "ns",
					Annotations: map[string]string{StabilityPeriodAnnotationKey: "3h"},
					Labels:      map[string]string{"app": "test"}},
				Spec: corev1.PodSpec{NodeName: "node1"},
			},
				&policyv1.PodDisruptionBudget{
					ObjectMeta: meta.ObjectMeta{Name: "pdb1", Namespace: "ns", CreationTimestamp: meta.Time{Time: now}},
					Spec: policyv1.PodDisruptionBudgetSpec{
						Selector: &meta.LabelSelector{
							MatchLabels: map[string]string{"app": "test"},
						},
					},
					Status: policyv1.PodDisruptionBudgetStatus{},
				},
			},
			node: &corev1.Node{
				ObjectMeta: meta.ObjectMeta{
					Name: "node1",
				},
			},
			want: false,
		},
		{
			name: "new pdb, using pdb creation timestamp, stability period deactivated",
			objects: []runtime.Object{&corev1.Pod{
				ObjectMeta: meta.ObjectMeta{Name: "pod1", Namespace: "ns",
					Labels: map[string]string{"app": "test"}},
				Spec: corev1.PodSpec{NodeName: "node1"},
			},
				&policyv1.PodDisruptionBudget{
					ObjectMeta: meta.ObjectMeta{Name: "pdb1", Namespace: "ns", CreationTimestamp: meta.Time{Time: now}},
					Spec: policyv1.PodDisruptionBudgetSpec{
						Selector: &meta.LabelSelector{
							MatchLabels: map[string]string{"app": "test"},
						},
					},
					Status: policyv1.PodDisruptionBudgetStatus{},
				},
			},
			node: &corev1.Node{
				ObjectMeta: meta.ObjectMeta{
					Name: "node1",
				},
			},
			want: true,
		},
		{
			name:                   "new pdb, using pdb creation timestamp old enough to accept disruption",
			defaultStabilityPeriod: &stabilityPeriodOneHour,
			objects: []runtime.Object{&corev1.Pod{
				ObjectMeta: meta.ObjectMeta{Name: "pod1", Namespace: "ns",
					Labels: map[string]string{"app": "test"}},
				Spec: corev1.PodSpec{NodeName: "node1"},
			},
				&policyv1.PodDisruptionBudget{
					ObjectMeta: meta.ObjectMeta{Name: "pdb1", Namespace: "ns", CreationTimestamp: meta.Time{Time: now.Add(-2 * stabilityPeriodOneHour)}},
					Spec: policyv1.PodDisruptionBudgetSpec{
						Selector: &meta.LabelSelector{
							MatchLabels: map[string]string{"app": "test"},
						},
					},
					Status: policyv1.PodDisruptionBudgetStatus{},
				},
			},
			node: &corev1.Node{
				ObjectMeta: meta.ObjectMeta{
					Name: "node1",
				},
			},
			want: true,
		},
		{
			name:                   "pdb not allowing disruption for long enough",
			defaultStabilityPeriod: &stabilityPeriodOneHour,
			objects: []runtime.Object{
				&corev1.Pod{
					ObjectMeta: meta.ObjectMeta{Name: "pod1", Namespace: "ns",
						Labels: map[string]string{"app": "test"}},
					Spec: corev1.PodSpec{NodeName: "node1"},
				},
				&policyv1.PodDisruptionBudget{
					ObjectMeta: meta.ObjectMeta{Name: "pdb1", Namespace: "ns"},
					Spec: policyv1.PodDisruptionBudgetSpec{
						Selector: &meta.LabelSelector{
							MatchLabels: map[string]string{"app": "test"},
						},
					},
					Status: policyv1.PodDisruptionBudgetStatus{
						Conditions: []meta.Condition{
							{
								Type:               policyv1.DisruptionAllowedCondition,
								Status:             meta.ConditionTrue,
								LastTransitionTime: meta.Time{Time: now.Add(-stabilityPeriodOneHour).Add(time.Minute)}, // missing one minute to allow
							},
						},
					},
				},
			},
			node: &corev1.Node{
				ObjectMeta: meta.ObjectMeta{
					Name: "node1",
				},
			},
			want: false,
		},
		{
			name:                   "pdb not allowing disruption for long enough, but stability period deactivated (negative value)",
			defaultStabilityPeriod: &stabilityPeriodNegativeOneHour,
			objects: []runtime.Object{
				&corev1.Pod{
					ObjectMeta: meta.ObjectMeta{Name: "pod1", Namespace: "ns",
						Labels: map[string]string{"app": "test"}},
					Spec: corev1.PodSpec{NodeName: "node1"},
				},
				&policyv1.PodDisruptionBudget{
					ObjectMeta: meta.ObjectMeta{Name: "pdb1", Namespace: "ns"},
					Spec: policyv1.PodDisruptionBudgetSpec{
						Selector: &meta.LabelSelector{
							MatchLabels: map[string]string{"app": "test"},
						},
					},
					Status: policyv1.PodDisruptionBudgetStatus{
						Conditions: []meta.Condition{
							{
								Type:               policyv1.DisruptionAllowedCondition,
								Status:             meta.ConditionTrue,
								LastTransitionTime: meta.Time{Time: now.Add(-stabilityPeriodOneHour).Add(time.Minute)}, // missing one minute to allow
							},
						},
					},
				},
			},
			node: &corev1.Node{
				ObjectMeta: meta.ObjectMeta{
					Name: "node1",
				},
			},
			want: true,
		},
		{
			name:                   "pdb allowing disruption +1 minute after stability period",
			defaultStabilityPeriod: &stabilityPeriodOneHour,
			objects: []runtime.Object{
				&corev1.Pod{
					ObjectMeta: meta.ObjectMeta{Name: "pod1", Namespace: "ns",
						Labels: map[string]string{"app": "test"}},
					Spec: corev1.PodSpec{NodeName: "node1"},
				},
				&corev1.Pod{ // unrelated pod (just to create noise)
					ObjectMeta: meta.ObjectMeta{Name: "podx", Namespace: "ns",
						Labels: map[string]string{"app": "x"}},
					Spec: corev1.PodSpec{NodeName: "nodex"},
				},
				&policyv1.PodDisruptionBudget{
					ObjectMeta: meta.ObjectMeta{Name: "pdb1", Namespace: "ns"},
					Spec: policyv1.PodDisruptionBudgetSpec{
						Selector: &meta.LabelSelector{
							MatchLabels: map[string]string{"app": "test"},
						},
					},
					Status: policyv1.PodDisruptionBudgetStatus{
						Conditions: []meta.Condition{
							{
								Type:               policyv1.DisruptionAllowedCondition,
								Status:             meta.ConditionTrue,
								LastTransitionTime: meta.Time{Time: now.Add(-stabilityPeriodOneHour).Add(-time.Minute)}, // one extra minute: allowed
							},
						},
					},
				},
				&policyv1.PodDisruptionBudget{ // unrelated PDB just to create noise
					ObjectMeta: meta.ObjectMeta{Name: "pdbx", Namespace: "ns"},
					Spec: policyv1.PodDisruptionBudgetSpec{
						Selector: &meta.LabelSelector{
							MatchLabels: map[string]string{"app": "x"},
						},
					},
					Status: policyv1.PodDisruptionBudgetStatus{
						Conditions: []meta.Condition{
							{
								Type:               policyv1.DisruptionAllowedCondition,
								Status:             meta.ConditionTrue,
								LastTransitionTime: meta.Time{Time: now.Add(-stabilityPeriodOneHour).Add(+time.Minute)}, // unrelated pod would block another node
							},
						},
					},
				},
				&corev1.Node{ // unrelated node just to create noise
					ObjectMeta: meta.ObjectMeta{
						Name: "nodex",
					},
				},
			},
			node: &corev1.Node{
				ObjectMeta: meta.ObjectMeta{
					Name: "node1",
				},
			},
			want: true,
		},
		{
			name:                   "one of the 2 pods of node1 is blocking",
			defaultStabilityPeriod: &stabilityPeriodOneHour,
			objects: []runtime.Object{
				&corev1.Pod{
					ObjectMeta: meta.ObjectMeta{Name: "pod1", Namespace: "ns",
						Labels: map[string]string{"app": "test"}},
					Spec: corev1.PodSpec{NodeName: "node1"},
				},
				&corev1.Pod{ // unrelated pod (just to create noise)
					ObjectMeta: meta.ObjectMeta{Name: "pod2", Namespace: "ns",
						Labels: map[string]string{"app": "2"}},
					Spec: corev1.PodSpec{NodeName: "node1"},
				},
				&policyv1.PodDisruptionBudget{
					ObjectMeta: meta.ObjectMeta{Name: "pdb1", Namespace: "ns"},
					Spec: policyv1.PodDisruptionBudgetSpec{
						Selector: &meta.LabelSelector{
							MatchLabels: map[string]string{"app": "test"},
						},
					},
					Status: policyv1.PodDisruptionBudgetStatus{
						Conditions: []meta.Condition{
							{
								Type:               policyv1.DisruptionAllowedCondition,
								Status:             meta.ConditionTrue,
								LastTransitionTime: meta.Time{Time: now.Add(-stabilityPeriodOneHour).Add(-time.Minute)}, // one extra minute: allowed
							},
						},
					},
				},
				&policyv1.PodDisruptionBudget{
					ObjectMeta: meta.ObjectMeta{Name: "pdb2", Namespace: "ns"},
					Spec: policyv1.PodDisruptionBudgetSpec{
						Selector: &meta.LabelSelector{
							MatchLabels: map[string]string{"app": "2"},
						},
					},
					Status: policyv1.PodDisruptionBudgetStatus{
						Conditions: []meta.Condition{
							{
								Type:               policyv1.DisruptionAllowedCondition,
								Status:             meta.ConditionTrue,
								LastTransitionTime: meta.Time{Time: now.Add(-stabilityPeriodOneHour).Add(+time.Minute)}, // unrelated pod would block another node
							},
						},
					},
				},
			},
			node: &corev1.Node{
				ObjectMeta: meta.ObjectMeta{
					Name: "node1",
				},
			},
			want: false,
		},
		{
			name:                   "ignore filtered out pods",
			defaultStabilityPeriod: &stabilityPeriodOneHour,
			objects: []runtime.Object{
				&corev1.Pod{
					ObjectMeta: meta.ObjectMeta{
						Name:      "pod1",
						Namespace: "ns",
						Labels:    map[string]string{"app": "test1"},
					},
					Spec: corev1.PodSpec{NodeName: "node1"},
				},
				&corev1.Pod{
					ObjectMeta: meta.ObjectMeta{
						Name:      "pod2",
						Namespace: "ns",
						Labels:    map[string]string{"filter-out": "yes", "app": "test2"},
					},
					Spec: corev1.PodSpec{NodeName: "node1"},
				},
				&policyv1.PodDisruptionBudget{
					ObjectMeta: meta.ObjectMeta{Name: "pdb1", Namespace: "ns"},
					Spec: policyv1.PodDisruptionBudgetSpec{
						Selector: &meta.LabelSelector{MatchLabels: map[string]string{"app": "test1"}},
					},
					Status: policyv1.PodDisruptionBudgetStatus{
						Conditions: []meta.Condition{
							{
								Type:               policyv1.DisruptionAllowedCondition,
								Status:             meta.ConditionTrue,
								LastTransitionTime: meta.Time{Time: now.Add(-stabilityPeriodOneHour)},
							},
						},
					},
				},
				&policyv1.PodDisruptionBudget{
					ObjectMeta: meta.ObjectMeta{Name: "pdb2", Namespace: "ns"},
					Spec: policyv1.PodDisruptionBudgetSpec{
						Selector: &meta.LabelSelector{MatchLabels: map[string]string{"app": "test2"}},
					},
					Status: policyv1.PodDisruptionBudgetStatus{
						Conditions: []meta.Condition{
							{
								Type:               policyv1.DisruptionAllowedCondition,
								Status:             meta.ConditionFalse,
								LastTransitionTime: meta.Time{Time: now.Add(-stabilityPeriodOneHour).Add(10 * time.Minute)},
							},
						},
					},
				},
			},
			node: &corev1.Node{
				ObjectMeta: meta.ObjectMeta{Name: "node1"},
			},
			want: true,
		},
	}

	testLogger := zapr.NewLogger(zap.NewNop())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.objects = append(tt.objects, tt.node)

			wrapper, err := k8sclient.NewFakeClient(k8sclient.FakeConf{Objects: tt.objects})
			assert.NoError(t, err)

			fakeKubeClient := fakeclient.NewSimpleClientset(tt.objects...)
			er := kubernetes.NewEventRecorder(record.NewFakeRecorder(1000))
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
			d := NewStabilityPeriodChecker(context.Background(), testLogger, wrapper.GetManagerClient(), er, store, fakeIndexer,
				StabilityPeriodCheckerConfiguration{
					DefaultStabilityPeriod: tt.defaultStabilityPeriod,
				}, podFilter)
			assert.Equalf(t, tt.want, d.StabilityPeriodAcceptsDrain(context.Background(), tt.node, now), "StabilityPeriodAcceptsDrain bad result")
		})
	}
}

func podFilter(pod corev1.Pod) (bool, string, error) {
	if val, ok := pod.Labels["filter-out"]; ok {
		return false, val, nil
	}
	return true, "", nil
}

func Test_stabilityPeriodChecker_getPodStabilityPeriodConfiguration(t *testing.T) {

	podWithAnnotation := &corev1.Pod{
		ObjectMeta: meta.ObjectMeta{
			Name:        "p-with-annotation",
			Namespace:   "ns1",
			Annotations: map[string]string{StabilityPeriodAnnotationKey: "10m"},
		},
	}
	podWithoutAnnotation := &corev1.Pod{
		ObjectMeta: meta.ObjectMeta{
			Name:      "p-without-annotation",
			Namespace: "ns1",
		},
	}

	podWithoutAnnotationFromSts := &corev1.Pod{
		ObjectMeta: meta.ObjectMeta{
			Name:      "p-without-annotation-sts-0",
			Namespace: "ns1",
			OwnerReferences: []meta.OwnerReference{
				{
					Kind: "StatefulSet",
					Name: "p-without-annotation-sts",
				},
			},
		},
	}

	podWithoutAnnotationFromDeployment := &corev1.Pod{
		ObjectMeta: meta.ObjectMeta{
			Name:      "p-without-deployment-abc-123",
			Namespace: "ns1",
			OwnerReferences: []meta.OwnerReference{
				{
					Kind: "ReplicaSet",
					Name: "p-without-annotation-deployment-abc",
				},
			},
		},
	}

	tests := []struct {
		name string
		pod  *corev1.Pod

		objects []runtime.Object
		want    stabilityPeriodInfo
		found   bool
	}{
		{
			name:    "not Found, pod only",
			pod:     podWithoutAnnotation,
			objects: nil,
			want:    stabilityPeriodInfo{},
			found:   false,
		},
		{
			name:    "found on pod",
			pod:     podWithAnnotation,
			objects: nil,
			want:    stabilityPeriodInfo{length: 10 * time.Minute, sourceType: "user/pod", sourceName: "ns1/p-with-annotation"},
			found:   true,
		},
		{
			name: "search on sts",
			pod:  podWithoutAnnotationFromSts,
			objects: []runtime.Object{
				&v1.StatefulSet{
					ObjectMeta: meta.ObjectMeta{
						Namespace:   podWithoutAnnotationFromSts.Namespace,
						Name:        podWithoutAnnotationFromSts.OwnerReferences[0].Name,
						Annotations: map[string]string{StabilityPeriodAnnotationKey: "9m"},
					},
				},
			},
			want:  stabilityPeriodInfo{length: 9 * time.Minute, sourceType: "user/controller", sourceName: podWithoutAnnotationFromSts.Namespace + "/" + podWithoutAnnotationFromSts.OwnerReferences[0].Name},
			found: true,
		},
		{
			name: "search on sts, nothing found",
			pod:  podWithoutAnnotationFromSts,
			objects: []runtime.Object{
				&v1.StatefulSet{
					ObjectMeta: meta.ObjectMeta{
						Namespace: podWithoutAnnotationFromSts.Namespace,
						Name:      podWithoutAnnotationFromSts.OwnerReferences[0].Name,
					},
				},
			},
			want:  stabilityPeriodInfo{},
			found: false,
		},
		{
			name: "search on deployment",
			pod:  podWithoutAnnotationFromDeployment,
			objects: []runtime.Object{
				&v1.Deployment{
					ObjectMeta: meta.ObjectMeta{
						Namespace:   podWithoutAnnotationFromDeployment.Namespace,
						Name:        "p-without-annotation-deployment",
						Annotations: map[string]string{StabilityPeriodAnnotationKey: "8m"},
					},
				},
			},
			want:  stabilityPeriodInfo{length: 8 * time.Minute, sourceType: "user/controller", sourceName: podWithoutAnnotationFromSts.Namespace + "/p-without-annotation-deployment"},
			found: true,
		},
		{
			name: "search on deployment",
			pod:  podWithoutAnnotationFromDeployment,
			objects: []runtime.Object{
				&v1.Deployment{
					ObjectMeta: meta.ObjectMeta{
						Namespace: podWithoutAnnotationFromDeployment.Namespace,
						Name:      "p-without-annotation-deployment",
					},
				},
			},
			want:  stabilityPeriodInfo{},
			found: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeKubeClient := fakeclient.NewSimpleClientset(tt.objects...)

			store, closeFunc := kubernetes.RunStoreForTest(context.Background(), fakeKubeClient)
			defer closeFunc()

			d := &stabilityPeriodChecker{
				store: store,
			}
			got, got1 := d.getPodStabilityPeriodConfiguration(context.Background(), tt.pod)
			assert.Equalf(t, tt.want, got, "getPodStabilityPeriodConfiguration()")
			assert.Equalf(t, tt.found, got1, "getPodStabilityPeriodConfiguration()")
		})
	}
}
