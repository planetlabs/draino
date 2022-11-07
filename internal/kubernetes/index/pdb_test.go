package index

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/slices"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
)

func Test_PDBIndexer(t *testing.T) {
	labels := map[string]string{"foo": "bar"}
	tests := []struct {
		Name             string
		TestPodName      string
		TestPodNamespace string
		ExpectedPDBNames []string
		Objects          []runtime.Object
	}{
		{
			Name:             "Should find only one PDB",
			TestPodName:      "foo",
			TestPodNamespace: "default",
			ExpectedPDBNames: []string{"my-pdb"},
			Objects: []runtime.Object{
				createPod(createPodOptions{Name: "foo", Ns: "default", NodeName: "my-node", IsReady: false, LS: labels}),
				createPDB("my-pdb", "default", labels),
				createPDB("my-pdb-2", "default", map[string]string{"jaha": "isso"}),
			},
		},
		{
			Name:             "Should find multiple PDBs if defined",
			TestPodName:      "foo",
			TestPodNamespace: "default",
			ExpectedPDBNames: []string{"my-pdb", "my-pdb-2"},
			Objects: []runtime.Object{
				createPod(createPodOptions{Name: "foo", Ns: "default", NodeName: "my-node", IsReady: false, LS: labels}),
				createPDB("my-pdb", "default", labels),
				createPDB("my-pdb-2", "default", labels),
			},
		},
		{
			Name:             "Should return one PDB if multiple pods are matching",
			TestPodName:      "foo",
			TestPodNamespace: "default",
			ExpectedPDBNames: []string{"my-pdb"},
			Objects: []runtime.Object{
				createPod(createPodOptions{Name: "foo", Ns: "default", NodeName: "my-node", IsReady: false, LS: labels}),
				createPod(createPodOptions{Name: "foo2", Ns: "default", NodeName: "my-node", IsReady: false, LS: labels}),
				createPDB("my-pdb", "default", labels),
				createPDB("my-pdb-2", "default", map[string]string{"jaha": "isso"}),
			},
		},
		{
			Name:             "Should not find any matching PDBs",
			TestPodName:      "foo",
			TestPodNamespace: "default",
			ExpectedPDBNames: []string{},
			Objects: []runtime.Object{
				createPod(createPodOptions{Name: "foo", Ns: "default", NodeName: "my-node", IsReady: false, LS: labels}),
				createPDB("my-pdb", "default", map[string]string{"foobar": "bar"}),
				createPDB("my-pdb-2", "default", map[string]string{"jaha": "isso"}),
			},
		},
		{
			Name:             "Should not find any matching PDBs in other namespaces",
			TestPodName:      "foo",
			TestPodNamespace: "default",
			ExpectedPDBNames: []string{},
			Objects: []runtime.Object{
				createPod(createPodOptions{Name: "foo", Ns: "default", NodeName: "my-node", IsReady: true, LS: labels}),
				createPDB("my-pdb", "default", labels),
				createPod(createPodOptions{Name: "foo", Ns: "kube-system", NodeName: "my-node", IsReady: false, LS: labels}),
				createPDB("my-pdb", "kube-system", labels),
			},
		},
		{
			Name:             "Should not find any PDBs as pod is ready",
			TestPodName:      "foo",
			TestPodNamespace: "default",
			ExpectedPDBNames: []string{},
			Objects: []runtime.Object{
				createPod(createPodOptions{Name: "foo", Ns: "default", NodeName: "my-node", IsReady: true, LS: labels}),
				createPDB("my-pdb", "default", labels),
				createPDB("my-pdb-2", "default", map[string]string{"jaha": "isso"}),
			},
		},
		{
			Name:             "Should return empty list if no pdbs are defined",
			TestPodName:      "foo",
			TestPodNamespace: "default",
			ExpectedPDBNames: []string{},
			Objects:          []runtime.Object{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			ch := make(chan struct{})
			defer close(ch)

			informer, err := NewFakePDBIndexer(ch, tt.Objects)
			assert.NoError(t, err)

			pdbs, err := informer.GetPDBsBlockedByPod(context.TODO(), tt.TestPodName, tt.TestPodNamespace)
			assert.NoError(t, err)

			assert.Equal(t, len(tt.ExpectedPDBNames), len(pdbs), "received amount of pods to not match expected amount")
			for _, pdb := range pdbs {
				assert.True(t, slices.Contains(tt.ExpectedPDBNames, pdb.GetName()), "found pod is not expected", pdb.GetName())
			}
		})
	}
}

func createPDB(name, ns string, selector labels.Set) *policyv1.PodDisruptionBudget {
	return &policyv1.PodDisruptionBudget{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "policy/v1",
			Kind:       "PodDisruptionBudget",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: selector,
			},
		},
	}
}
