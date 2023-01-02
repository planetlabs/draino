package kubernetes

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	core "k8s.io/api/core/v1"
)

// SuppliedCondition defines the condition will be watched.
type SuppliedCondition struct {
	Type     core.NodeConditionType `json:"type"`
	Status   core.ConditionStatus   `json:"conditionStatus"`
	Delay    string                 `json:"delay"`
	Priority int                    `json:"priority"` // higher value first in priority, default is 0, negative value are accepted

	parsedDelay time.Duration
}

func GetNodeOffendingConditions(n *core.Node, suppliedConditions []SuppliedCondition) []SuppliedCondition {
	var conditions []SuppliedCondition
	for _, suppliedCondition := range suppliedConditions {
		for _, nodeCondition := range n.Status.Conditions {
			if suppliedCondition.Type == nodeCondition.Type &&
				suppliedCondition.Status == nodeCondition.Status &&
				time.Since(nodeCondition.LastTransitionTime.Time) >= suppliedCondition.parsedDelay {
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

func ParseConditions(conditions []string) ([]SuppliedCondition, error) {
	parsed := make([]SuppliedCondition, len(conditions))
	for i, c := range conditions {
		ts := strings.SplitN(c, "=", 2)
		if len(ts) != 2 {
			// Keep backward compatibility
			ts = []string{c, "{}"}
		}
		var condition SuppliedCondition
		if err := json.Unmarshal([]byte(ts[1]), &condition); err != nil {
			return nil, err
		}
		condition.Type = core.NodeConditionType(ts[0])
		if condition.Delay != "" {
			var errParse error
			if condition.parsedDelay, errParse = time.ParseDuration(condition.Delay); errParse != nil {
				return nil, errParse
			}
		}
		if condition.Status == "" {
			condition.Status = core.ConditionTrue
		}

		parsed[i] = condition

	}
	return parsed, nil
}

func parseConditionsFromAnnotation(n *core.Node) ([]SuppliedCondition, error) {
	if n.Annotations == nil {
		return nil, nil
	}
	if n.Annotations[drainoConditionsAnnotationKey] == "" {
		return nil, nil
	}
	var conditions []SuppliedCondition
	if err := json.Unmarshal([]byte(n.Annotations[drainoConditionsAnnotationKey]), &conditions); err != nil {
		return nil, err
	}
	return conditions, nil
}
