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
		labels       []string
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
			labels:       []string{"cool=very"},
			passesFilter: true,
		},
		{
			name: "SingleMatchingLabel.WithDomain",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:   nodeName,
					Labels: map[string]string{"planetlabs.com/cool": "very"},
				},
			},
			labels:       []string{"planetlabs.com/cool=very"},
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
			labels:       []string{"cool=very", "lame=nope"},
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
			labels:       []string{"cool=very"},
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
			labels:       []string{"cool=very", "lame=nope"},
			passesFilter: false,
		}, {
			name: "PartiallyAbsentLabels",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{
					Name:   nodeName,
					Labels: map[string]string{"cool": "very"},
				},
			},
			labels:       []string{"cool=very", "lame=nope"},
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
			labels:       []string{"cool=very"},
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
			labels:       []string{"keyWithNoValue="},
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
			labels:       []string{"keyWithNoValue="},
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
			labels:       []string{"cool=very"},
			passesFilter: false,
		},
	}

	log, _ := zap.NewDevelopment()

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			labelExpr, err := ConvertLabelsToFilterExpr(tc.labels)

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
	cases := []struct {
		name     string
		input    []string
		expected string
		wantErr  bool
	}{
		{
			name:     "2 labels",
			input:    []string{"foo=bar", "sup=cool"},
			expected: "metadata.labels['foo'] == 'bar' && metadata.labels['sup'] == 'cool'",
		},
		{
			name:     "2 labels same key",
			input:    []string{"foo=bar", "foo=cool"},
			expected: "",
			wantErr:  true,
		},
		{
			name:     "no filter",
			input:    nil,
			expected: "",
			wantErr:  false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := ConvertLabelsToFilterExpr(tc.input)
			if tc.wantErr && err == nil {
				t.Errorf("error was expected for that case")
				return
			}
			if !tc.wantErr && err != nil {
				t.Errorf("no error was expected for that case")
				return
			}
			if tc.wantErr && err != nil {
				return
			}
			if actual == nil {
				t.Errorf("string value was expected")
				return
			}
			got := *actual
			if !reflect.DeepEqual(tc.expected, got) {
				t.Errorf("expect %v, got: %v", tc.expected, got)
			}
		})
	}
}
