package drain

import (
	"context"
	"sort"
	"testing"

	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func TestSimulator_SimulateDrain(t *testing.T) {
	testLabels := map[string]string{
		"app": "foo",
	}
	tests := []struct {
		Name        string
		IsDrainable bool
		Reason      []string
		Node        corev1.Node
		Objects     []runtime.Object
		PodFilter   kubernetes.PodFilterFunc
	}{
		{
			Name:        "Should drain empty node",
			IsDrainable: true,
			Reason:      nil,
			Node: corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "foo-node",
				},
			},
			Objects:   []runtime.Object{},
			PodFilter: noopPodFilter,
		},
		// {
		// Name:        "Should drain with pods that are not covered by PDBs",
		// IsDrainable: true,
		// Reason:      nil,
		// PodFilter:   noopPodFilter,
		// Node:        corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "foo-node"}},
		// Objects: []runtime.Object{
		// createPod(createPodOpts{Name: "foo-pod", NodeName: "foo-node"}),
		// },
		// },
		// {
		// Name:        "Should drain if PDB has enough budget",
		// IsDrainable: true,
		// Reason:      nil,
		// PodFilter:   noopPodFilter,
		// Node:        corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "foo-node"}},
		// Objects: []runtime.Object{
		// createPod(createPodOpts{Name: "foo-pod", Labels: testLabels, NodeName: "foo-node"}),
		// createPDB(createPDBOpts{Name: "foo-pdb", Labels: testLabels, Des: 2, Healthy: 3}),
		// },
		// },
		// {
		// Name:        "Should drain if failing pod is taking PDB budget",
		// IsDrainable: true,
		// Reason:      nil,
		// PodFilter:   noopPodFilter,
		// Node:        corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "foo-node"}},
		// Objects: []runtime.Object{
		// createPod(createPodOpts{Name: "foo-pod", Labels: testLabels, NodeName: "foo-node", IsNotReady: true}),
		// createPDB(createPDBOpts{Name: "foo-pdb", Labels: testLabels, Des: 2, Healthy: 2}),
		// },
		// },
		{
			Name:        "Should not drain with pods that are blocked by PDBs with missing budget",
			IsDrainable: false,
			Reason:      []string{"Cannot drain pod 'foo-pod', because: PDB 'foo-pdb' does not allow any disruptions"},
			PodFilter:   noopPodFilter,
			Node:        corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "foo-node"}},
			Objects: []runtime.Object{
				createPod(createPodOpts{Name: "foo-pod", Labels: testLabels, NodeName: "foo-node", IsNotReady: false}),
				createPDB(createPDBOpts{Name: "foo-pdb", Labels: testLabels, Des: 2, Healthy: 1}),
			},
		},
		{
			Name:        "Should not drain if PDB is blocked (lockness)",
			IsDrainable: false,
			Reason:      []string{"Cannot drain pod 'foo-pod1', because: PDB 'foo-pdb' does not allow any disruptions", "Cannot drain pod 'foo-pod2', because: PDB 'foo-pdb' does not allow any disruptions"},
			PodFilter:   noopPodFilter,
			Node:        corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "foo-node"}},
			Objects: []runtime.Object{
				createPod(createPodOpts{Name: "foo-pod1", Labels: testLabels, NodeName: "foo-node"}),
				createPod(createPodOpts{Name: "foo-pod2", Labels: testLabels, NodeName: "foo-node"}),
				createPDB(createPDBOpts{Name: "foo-pdb", Labels: testLabels, Des: 2, Healthy: 2}),
			},
		},
		{
			Name:        "Should not care about pods on other nodes",
			IsDrainable: true,
			Reason:      nil,
			PodFilter:   noopPodFilter,
			Node:        corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "foo-node"}},
			Objects: []runtime.Object{
				createPod(createPodOpts{Name: "foo-pod", Labels: testLabels, NodeName: "other-foo-node", IsNotReady: false}),
				createPDB(createPDBOpts{Name: "foo-pdb", Labels: testLabels, Des: 2, Healthy: 1}),
			},
		},
		{
			Name:        "Should filter out pods using the pod filter",
			IsDrainable: true,
			Reason:      nil,
			PodFilter:   kubernetes.LocalStoragePodFilter,
			Node:        corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "foo-node"}},
			Objects: []runtime.Object{
				createPodWithVolume(createPodOpts{Name: "foo-pod", Labels: testLabels, NodeName: "foo-node", IsNotReady: true}),
				createPDB(createPDBOpts{Name: "foo-pdb", Labels: testLabels, Des: 2, Healthy: 0}),
			},
		},
		{
			Name:        "Should not drain if one pod has multiple PDBs",
			IsDrainable: false,
			Reason:      []string{"Cannot drain pod 'foo-pod', because: Pod has more than one associated PDB 2 > 1"},
			PodFilter:   noopPodFilter,
			Node:        corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "foo-node"}},
			Objects: []runtime.Object{
				createPod(createPodOpts{Name: "foo-pod", Labels: testLabels, NodeName: "foo-node"}),
				createPDB(createPDBOpts{Name: "foo-pdb1", Labels: testLabels, Des: 2, Healthy: 3}),
				createPDB(createPDBOpts{Name: "foo-pdb2", Labels: testLabels, Des: 2, Healthy: 3}),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			ch := make(chan struct{})
			defer close(ch)
			simulator, err := NewFakeDrainSimulator(
				&FakeSimulatorOptions{
					Chan:      ch,
					Objects:   append(tt.Objects, &tt.Node),
					PodFilter: tt.PodFilter,
				},
			)
			assert.NoError(t, err)

			drainable, reason, err := simulator.SimulateDrain(context.Background(), &tt.Node)
			sort.Strings(tt.Reason)
			assert.Equal(t, tt.IsDrainable, drainable, "Node drainability is not as expected")
			assert.Equal(t, tt.Reason, reason, "Reason is not as expected")
		})
	}
}

type createPodOpts struct {
	Name       string
	NodeName   string
	Labels     map[string]string
	IsNotReady bool
}

func createEvictionPPPod(opts createPodOpts) *corev1.Pod {
	pod := createPod(opts)
	pod.ObjectMeta.Annotations = map[string]string{
		kubernetes.EvictionAPIURLAnnotationKey: "test-api",
	}
	return pod
}

func createPodWithVolume(opts createPodOpts) *corev1.Pod {
	pod := createPod(opts)
	pod.Spec.Volumes = []corev1.Volume{
		{
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{
					Medium: "medium",
				},
			},
		},
	}
	return pod
}

func createPod(opts createPodOpts) *corev1.Pod {
	condition := corev1.PodCondition{
		Type:   corev1.ContainersReady,
		Status: corev1.ConditionTrue,
	}
	if opts.IsNotReady {
		condition.Status = corev1.ConditionFalse
	}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      opts.Name,
			Namespace: "default",
			Labels:    opts.Labels,
		},
		Spec: corev1.PodSpec{
			NodeName: opts.NodeName,
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{condition},
		},
	}
}

type createPDBOpts struct {
	Name    string
	Labels  map[string]string
	Des     int32
	Healthy int32
}

func createPDB(opts createPDBOpts) *policyv1.PodDisruptionBudget {
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      opts.Name,
			Namespace: "default",
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: opts.Labels,
			},
			MaxUnavailable: intStrPtr(int(opts.Des)),
		},
		Status: policyv1.PodDisruptionBudgetStatus{
			DesiredHealthy: opts.Des,
			CurrentHealthy: opts.Healthy,
		},
	}
}

func noopPodFilter(p corev1.Pod) (pass bool, reason string, err error) {
	return true, "", nil
}

func intStrPtr(val int) *intstr.IntOrString {
	res := intstr.FromInt(val)
	return &res
}
