package kubernetes

import (
	"reflect"
	"testing"
	"time"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
)

func TestOffendingConditions(t *testing.T) {
	cases := []struct {
		name       string
		obj        *core.Node
		conditions []string
		expected   []SuppliedCondition
	}{
		{
			name: "SingleMatchingCondition",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					{Type: "Cool", Status: core.ConditionTrue},
				}},
			},
			conditions: []string{"Cool"},
			expected:   []SuppliedCondition{{Type: "Cool", Status: core.ConditionTrue}},
		},
		{
			name: "ManyMatchingConditions",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					{Type: "Cool", Status: core.ConditionTrue},
					{Type: "Rad", Status: core.ConditionTrue},
				}},
			},
			conditions: []string{"Cool", "Rad"},
			expected: []SuppliedCondition{
				{Type: "Cool", Status: core.ConditionTrue},
				{Type: "Rad", Status: core.ConditionTrue},
			},
		},
		{
			name: "PartiallyMatchingConditions",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					{Type: "Cool", Status: core.ConditionTrue},
					{Type: "Rad", Status: core.ConditionFalse},
				}},
			},
			conditions: []string{"Cool", "Rad"},
			expected: []SuppliedCondition{
				{Type: "Cool", Status: core.ConditionTrue},
			},
		},
		{
			name: "PartiallyAbsentConditions",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					{Type: "Rad", Status: core.ConditionTrue},
				}},
			},
			conditions: []string{"Cool", "Rad"},
			expected: []SuppliedCondition{
				{Type: "Rad", Status: core.ConditionTrue},
			},
		},
		{
			name: "SingleFalseCondition",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					{Type: "Cool", Status: core.ConditionFalse},
				}},
			},
			conditions: []string{"Cool"},
			expected:   nil,
		},
		{
			name:       "NoNodeConditions",
			obj:        &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
			conditions: []string{"Cool"},
			expected:   nil,
		},
		{
			name: "NoFilterConditions",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					{Type: "Cool", Status: core.ConditionFalse},
				}},
			},
			expected: nil,
		},
		{
			name: "NewConditionFormat",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					{Type: "Cool", Status: core.ConditionUnknown},
				}},
			},
			conditions: []string{"Cool=Unknown" + SuppliedConditionDurationSeparator + "10m"},
			expected: []SuppliedCondition{
				{Type: "Cool", Status: core.ConditionUnknown, MinimumDuration: 10 * time.Minute},
			},
		},
		{
			name: "NewConditionFormatDurationNotEnough",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					{Type: "Cool", Status: core.ConditionUnknown, LastTransitionTime: meta.NewTime(time.Now().Add(time.Duration(-9) * time.Minute))},
				}},
			},
			conditions: []string{"Cool=Unknown" + SuppliedConditionDurationSeparator + "10m"},
			expected:   nil,
		},
		{
			name: "NewConditionFormatDurationIsEnough",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					{Type: "Cool", Status: core.ConditionUnknown, LastTransitionTime: meta.NewTime(time.Now().Add(time.Duration(-15) * time.Minute))},
				}},
			},
			conditions: []string{"Cool=Unknown" + SuppliedConditionDurationSeparator + "14m"},
			expected: []SuppliedCondition{
				{Type: "Cool", Status: core.ConditionUnknown, MinimumDuration: 14 * time.Minute},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			suppliedConditions, _ := ParseConditions(tc.conditions)
			h := NewDrainingResourceEventHandler(fake.NewSimpleClientset(), &NoopCordonDrainer{}, nil, NewEventRecorder(&record.FakeRecorder{}), WithGlobalConfigHandler(GlobalConfig{SuppliedConditions: suppliedConditions}))
			badConditions := GetNodeOffendingConditions(tc.obj, h.globalConfig.SuppliedConditions)
			if !reflect.DeepEqual(badConditions, tc.expected) {
				t.Errorf("offendingConditions(tc.obj): want %#v, got %#v", tc.expected, badConditions)
			}
		})
	}
}
