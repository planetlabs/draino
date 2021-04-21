package kubernetes

import (
	"context"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	core "k8s.io/api/core/v1"
)

// SuppliedCondition defines the condition will be watched.
type SuppliedCondition struct {
	Type            core.NodeConditionType
	Status          core.ConditionStatus
	MinimumDuration time.Duration
}

func GetNodeOffendingConditions(n *core.Node, suppliedConditions []SuppliedCondition) []SuppliedCondition {
	var conditions []SuppliedCondition
	for _, suppliedCondition := range suppliedConditions {
		for _, nodeCondition := range n.Status.Conditions {
			if suppliedCondition.Type == nodeCondition.Type &&
				suppliedCondition.Status == nodeCondition.Status &&
				time.Since(nodeCondition.LastTransitionTime.Time) >= suppliedCondition.MinimumDuration {
				conditions = append(conditions, suppliedCondition)
			}
		}
	}
	return conditions
}

func GetConditionsTypes(conditions []SuppliedCondition) []string {
	result := make([]string, len(conditions))
	for i := range conditions {
		result[i] = string(conditions[i].Type)
	}
	return result
}

func StatRecordForEachCondition(ctx context.Context, node *core.Node, conditions []SuppliedCondition, m stats.Measurement) {
	tagsWithNg, _ := nodeTags(ctx, node)
	for _, c := range GetConditionsTypes(conditions) {
		tags, _ := tag.New(tagsWithNg, tag.Upsert(TagConditions, c))
		stats.Record(tags, m)
	}
}
