package utils

import policyv1 "k8s.io/api/policy/v1"

func GetPDBNames(pdbs []*policyv1.PodDisruptionBudget) []string {
	res := make([]string, 0, len(pdbs))
	for _, pdb := range pdbs {
		res = append(res, pdb.Name)
	}
	return res
}
