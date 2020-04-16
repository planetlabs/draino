/*
Copyright 2018 Planet Labs Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing permissions
and limitations under the License.
*/

package kubernetes

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNodeLabelFilter(t *testing.T) {
	cases := []struct {
		name         string
		logicType    string
		obj          interface{}
		expression   string
		passesFilter bool
	}{
		{
			name: "SingleMatchingLabel",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:   nodeName,
					Labels: map[string]string{"cool": "very"},
				},
			},
			expression:   "metadata.labels.cool == 'very'",
			passesFilter: true,
		},
		{
			name: "MatchesAllLabels",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:   nodeName,
					Labels: map[string]string{"cool": "very", "lame": "nope"},
				},
			},
			expression:   "metadata.labels.cool == 'very' && metadata.labels.lame == 'nope'",
			passesFilter: true,
		},
		{
			name: "DoesntMatchWrongLabel",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:   nodeName,
					Labels: map[string]string{"cool": "nope"},
				},
			},
			expression:   "metadata.labels.cool == 'very'",
			passesFilter: false,
		},
		{
			name: "PR75Example1",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{
					Name: nodeName,
					Labels: map[string]string{
						"region": "us-west-2",
						"app":    "nginx",
						"type":   "sup",
					},
				},
			},
			expression:   "(metadata.labels.region == 'us-west-2' && metadata.labels.app == 'nginx') || (metadata.labels.region == 'us-west-2' && metadata.labels.foo == 'bar') || (metadata.labels.type == 'toolbox')",
			passesFilter: true,
		},
		{
			name: "PR75Example2",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{
					Name: nodeName,
					Labels: map[string]string{
						"region": "us-west-2",
						"app":    "nginx",
						"type":   "sup",
					},
				},
			},
			expression:   "(metadata.labels.region == 'us-west-1' && metadata.labels.app == 'nginx') || (metadata.labels.region == 'us-west-2' && metadata.labels.foo == 'bar') || (metadata.labels.type == 'toolbox')",
			passesFilter: false,
		},
		{
			name: "MatchesSomeLabels",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:   nodeName,
					Labels: map[string]string{"cool": "very"},
				},
			},
			expression:   "(metadata.labels.cool == 'lame') || (metadata.labels.cool == 'very')",
			passesFilter: true,
		},
		{
			name: "MatchesNoLabels",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:   nodeName,
					Labels: map[string]string{"cool": "nope"},
				},
			},
			expression:   "(metadata.labels.cool == 'lame') || (metadata.labels.cool == 'very')",
			passesFilter: false,
		},
		{
			name: "InOperatorMatches",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:   nodeName,
					Labels: map[string]string{"cool": "very"},
				},
			},
			expression:   "metadata.labels.cool in ['very', 'lame']",
			passesFilter: true,
		},
		{
			name: "InOperatorFails",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:   nodeName,
					Labels: map[string]string{"cool": "very"},
				},
			},
			expression:   "metadata.labels.cool in ['lame', 'nope']",
			passesFilter: false,
		},
		{
			name: "NoNodeLabels",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:   nodeName,
					Labels: make(map[string]string),
				},
			},
			expression:   "metadata.labels.cool == 'very'",
			passesFilter: false,
		},
		{
			name: "NoFilterLabels",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:   nodeName,
					Labels: map[string]string{"cool": "very"},
				},
			},
			expression:   "",
			passesFilter: true,
		},
		{
			name: "FilterNodeThatHasLabel_InSyntax",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:   nodeName,
					Labels: map[string]string{"cool": "very"},
				},
			},
			expression:   "'cool' in metadata.labels",
			passesFilter: true,
		},
		{
			name: "FilterNodeThatHasLabel_AccessorSyntax",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:   nodeName,
					Labels: map[string]string{"cool": "very"},
				},
			},
			expression:   "metadata.labels.cool != ''",
			passesFilter: true,
		},
		{
			name: "FilterNodeThatIsMissingLabel_AccessorSyntax",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:   nodeName,
					Labels: map[string]string{"sup": "very"},
				},
			},
			expression:   "metadata.labels.cool != ''",
			passesFilter: false,
		},
	}
	log, _ := zap.NewDevelopment()

	for _, tc := range cases {

		t.Run(tc.name, func(t *testing.T) {
			filter, err := NewNodeLabelFilter(&tc.expression, log)
			if err != nil {
				t.Errorf("Filter expression: %v, did not compile", err)
				t.FailNow()
			}

			passesFilter := filter(tc.obj)
			assert.Equal(t, tc.passesFilter, passesFilter)
		})
	}
}

func TestOldNodeLabelFilter(t *testing.T) {
	cases := []struct {
		name         string
		obj          interface{}
		labels       map[string]string
		passesFilter bool
	}{
		{
			name: "SingleMatchingLabel",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:   nodeName,
					Labels: map[string]string{"cool": "very"},
				},
			},
			labels:       map[string]string{"cool": "very"},
			passesFilter: true,
		},
		{
			name: "ManyMatchingLabels",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:   nodeName,
					Labels: map[string]string{"cool": "very", "lame": "nope"},
				},
			},
			labels:       map[string]string{"cool": "very", "lame": "nope"},
			passesFilter: true,
		},
		{
			name: "SingleUnmatchingLabel",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:   nodeName,
					Labels: map[string]string{"cool": "notsocool"},
				},
			},
			labels:       map[string]string{"cool": "very"},
			passesFilter: false,
		},
		{
			name: "PartiallyMatchingLabels",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:   nodeName,
					Labels: map[string]string{"cool": "very", "lame": "somehowyes"},
				},
			},
			labels:       map[string]string{"cool": "very", "lame": "nope"},
			passesFilter: false,
		}, {
			name: "PartiallyAbsentLabels",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:   nodeName,
					Labels: map[string]string{"cool": "very"},
				},
			},
			labels:       map[string]string{"cool": "very", "lame": "nope"},
			passesFilter: false,
		},
		{
			name: "NoNodeLabels",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:   nodeName,
					Labels: map[string]string{},
				},
			},
			labels:       map[string]string{"cool": "very"},
			passesFilter: false,
		},
		{
			name: "NoFilterLabels",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:   nodeName,
					Labels: map[string]string{"cool": "very"},
				},
			},
			passesFilter: true,
		},
		{
			name: "FilterWithEmptyValue",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:   nodeName,
					Labels: map[string]string{"cool": "very"},
				},
			},
			labels:       map[string]string{"keyWithNoValue": ""},
			passesFilter: false,
		},
		{
			name: "FilterWithEmptyValueAndNodeWithEmptyValue",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:   nodeName,
					Labels: map[string]string{"cool": "very", "keyWithNoValue": ""},
				},
			},
			labels:       map[string]string{"keyWithNoValue": ""},
			passesFilter: true,
		},
		{
			name: "NotANode",
			obj: &core.Pod{
				ObjectMeta: meta.ObjectMeta{
					Name:   podName,
					Labels: map[string]string{"cool": "very"},
				},
			},
			labels:       map[string]string{"cool": "very"},
			passesFilter: false,
		},
	}

	log, _ := zap.NewDevelopment()

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			labelExpr := ConvertLabelsToFilterExpr(tc.labels)

			filter, err := NewNodeLabelFilter(labelExpr, log)
			if err != nil {
				t.Errorf("Filter expression: %v, did not compile", err)
				t.FailNow()
			}

			passesFilter := filter(tc.obj)
			assert.Equal(t, tc.passesFilter, passesFilter)
		})
	}
}

func TestNodeConditionFilter(t *testing.T) {
	cases := []struct {
		name         string
		obj          interface{}
		conditions   []string
		passesFilter bool
	}{
		{
			name: "SingleMatchingCondition",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					core.NodeCondition{Type: "Cool", Status: core.ConditionTrue},
				}},
			},
			conditions:   []string{"Cool"},
			passesFilter: true,
		},
		{
			name: "ManyMatchingConditions",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					core.NodeCondition{Type: "Cool", Status: core.ConditionTrue},
					core.NodeCondition{Type: "Rad", Status: core.ConditionTrue},
				}},
			},
			conditions:   []string{"Cool", "Rad"},
			passesFilter: true,
		},
		{
			name: "PartiallyMatchingConditions",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					core.NodeCondition{Type: "Cool", Status: core.ConditionTrue},
					core.NodeCondition{Type: "Rad", Status: core.ConditionFalse},
				}},
			},
			conditions:   []string{"Cool", "Rad"},
			passesFilter: true,
		},
		{
			name: "PartiallyAbsentConditions",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					core.NodeCondition{Type: "Rad", Status: core.ConditionTrue},
				}},
			},
			conditions:   []string{"Cool", "Rad"},
			passesFilter: true,
		},
		{
			name: "SingleFalseCondition",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					core.NodeCondition{Type: "Cool", Status: core.ConditionFalse},
				}},
			},
			conditions:   []string{"Cool"},
			passesFilter: false,
		},
		{
			name:         "NoNodeConditions",
			obj:          &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
			conditions:   []string{"Cool"},
			passesFilter: false,
		},
		{
			name: "NoFilterConditions",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					core.NodeCondition{Type: "Cool", Status: core.ConditionFalse},
				}},
			},
			passesFilter: true,
		},
		{
			name: "NotANode",
			obj: &core.Pod{
				ObjectMeta: meta.ObjectMeta{Name: podName},
			},
			conditions:   []string{"Cool"},
			passesFilter: false,
		},
		{
			name: "NewConditionFormat",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					core.NodeCondition{Type: "Cool", Status: core.ConditionUnknown},
				}},
			},
			conditions:   []string{"Cool=Unknown,10m"},
			passesFilter: true,
		},
		{
			name: "NewConditionFormatDurationNotEnough",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					core.NodeCondition{Type: "Cool", Status: core.ConditionUnknown, LastTransitionTime: meta.NewTime(time.Now().Add(time.Duration(-9) * time.Minute))},
				}},
			},
			conditions:   []string{"Cool=Unknown,10m"},
			passesFilter: false,
		},
		{
			name: "NewConditionFormatDurationIsEnough",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{Name: nodeName},
				Status: core.NodeStatus{Conditions: []core.NodeCondition{
					core.NodeCondition{Type: "Cool", Status: core.ConditionUnknown, LastTransitionTime: meta.NewTime(time.Now().Add(time.Duration(-15) * time.Minute))},
				}},
			},
			conditions:   []string{"Cool=Unknown,14m"},
			passesFilter: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			filter := NewNodeConditionFilter(tc.conditions)
			passesFilter := filter(tc.obj)
			if passesFilter != tc.passesFilter {
				t.Errorf("filter(tc.obj): want %v, got %v", tc.passesFilter, passesFilter)
			}
		})
	}
}

func TestNodeProcessedFilter(t *testing.T) {
	cases := []struct {
		name         string
		existing     interface{}
		obj          interface{}
		passesFilter bool
	}{
		{
			name:         "NoNodesProcessed",
			obj:          &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName, UID: "a"}},
			passesFilter: true,
		},
		{
			name:         "DifferentNodeProcessed",
			existing:     &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName + "-b", UID: "b"}},
			obj:          &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName, UID: "a"}},
			passesFilter: true,
		},
		{
			name:         "NodeAlreadyProcessed",
			existing:     &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName, UID: "a"}},
			obj:          &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName, UID: "a"}},
			passesFilter: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			np := NewNodeProcessed()
			np.Filter(tc.existing)
			passesFilter := np.Filter(tc.obj)
			if passesFilter != tc.passesFilter {
				t.Errorf("np.Filter(tc.obj): want %v, got %v", tc.passesFilter, passesFilter)
			}
		})
	}
}

func TestParseConditions(t *testing.T) {
	cases := []struct {
		name       string
		conditions []string
		expect     []SuppliedCondition
	}{
		{
			name:       "OldFormat",
			conditions: []string{"Ready"},
			expect:     []SuppliedCondition{SuppliedCondition{core.NodeConditionType("Ready"), core.ConditionStatus("True"), time.Duration(0) * time.Second}},
		},
		{
			name:       "Mixed",
			conditions: []string{"Ready", "OutOfDisk=True,10m"},
			expect: []SuppliedCondition{
				SuppliedCondition{core.NodeConditionType("Ready"), core.ConditionStatus("True"), time.Duration(0) * time.Second},
				SuppliedCondition{core.NodeConditionType("OutOfDisk"), core.ConditionStatus("True"), time.Duration(10) * time.Minute},
			},
		},
		{
			name:       "NewFormat",
			conditions: []string{"Ready=Unknown,30m"},
			expect:     []SuppliedCondition{SuppliedCondition{core.NodeConditionType("Ready"), core.ConditionStatus("Unknown"), time.Duration(30) * time.Minute}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsed := ParseConditions(tc.conditions)
			if !reflect.DeepEqual(tc.expect, parsed) {
				t.Errorf("expect %v, got: %v", tc.expect, parsed)
			}
		})
	}
}

func TestConvertLabelsToFilterExpr(t *testing.T) {
	input := map[string]string{
		"foo": "bar",
		"sup": "cool",
	}

	desired := "metadata.labels.foo == 'bar' && metadata.labels.sup == 'cool'"
	actual := ConvertLabelsToFilterExpr(input)

	assert.Equal(t, desired, *actual)
}
