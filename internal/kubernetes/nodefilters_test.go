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
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

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
			name: "Exclusion on match",
			obj: &core.Node{
				ObjectMeta: meta.ObjectMeta{
					Name: nodeName,
					Labels: map[string]string{
						"region": "us-west-2",
						"app":    "nginx",
						"type":   "sup-xyz",
					},
				},
			},
			expression:   "metadata.labels['region'] == 'us-west-2' && metadata.labels['app'] == 'nginx' && not ( metadata.labels['type'] matches 'sup-x.+')",
			passesFilter: false,
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
			filter, err := NewNodeLabelFilter(tc.expression, log)
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

			filter, err := NewNodeLabelFilter(*labelExpr, log)
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
		expectErr  bool
	}{
		{
			name:       "OldFormat",
			conditions: []string{"Ready"},
			expect:     []SuppliedCondition{SuppliedCondition{Type: core.NodeConditionType("Ready"), Status: core.ConditionStatus("True"), parsedDelay: time.Duration(0) * time.Second}},
		},
		{
			name:       "Mixed",
			conditions: []string{"Ready", `OutOfDisk={ "delay":"10m"}`},
			expect: []SuppliedCondition{
				{Type: core.NodeConditionType("Ready"), Status: core.ConditionStatus("True"), parsedDelay: time.Duration(0) * time.Second},
				{Type: core.NodeConditionType("OutOfDisk"), Status: core.ConditionStatus("True"), parsedDelay: time.Duration(10) * time.Minute, Delay: "10m"},
			},
		},
		{
			name:       "NewFormat",
			conditions: []string{`Ready={"conditionStatus":"Unknown","delay":"30m"}`},
			expect:     []SuppliedCondition{{Type: core.NodeConditionType("Ready"), Status: core.ConditionStatus("Unknown"), parsedDelay: time.Duration(30) * time.Minute, Delay: "30m"}},
		},
		{
			name:       "FormatError",
			conditions: []string{"Ready=Unknown;30err"},
			expect:     nil,
			expectErr:  true,
		},
		{
			name:       "FormatErrorDuration",
			conditions: []string{`Ready={"delay":"30err"}`},
			expect:     nil,
			expectErr:  true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := ParseConditions(tc.conditions)
			if !(tc.expectErr == (err != nil)) {
				t.Errorf("expect err is correct. Expected %v", tc.expect)
			}
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

func TestGetPodsBoundToNodeByPV(t *testing.T) {
	hostname := "ip-10-128-208-156"
	node0 := &core.Node{
		ObjectMeta: meta.ObjectMeta{
			Name:   "ip-10-128-208-156.ec2.internal",
			Labels: map[string]string{hostNameLabelKey: hostname},
		},
	}
	nodeOther := &core.Node{
		ObjectMeta: meta.ObjectMeta{
			Name:   "ip-10-123-231-001.ec2.internal",
			Labels: map[string]string{hostNameLabelKey: "ip-10-123-231-001"},
		},
	}
	pv0 := &core.PersistentVolume{
		ObjectMeta: meta.ObjectMeta{
			Name:   "pv0",
			Labels: map[string]string{hostNameLabelKey: hostname},
		},
		Spec: core.PersistentVolumeSpec{
			ClaimRef: &core.ObjectReference{
				Name:      "claim0",
				Namespace: "ns0",
			},
		},
	}
	pvc0 := &core.PersistentVolumeClaim{
		ObjectMeta: meta.ObjectMeta{
			Name:      "claim0",
			Namespace: "ns0",
		},
	}
	pvc1Deleted := &core.PersistentVolumeClaim{
		ObjectMeta: meta.ObjectMeta{
			Name:              "claim0",
			Namespace:         "ns0",
			DeletionTimestamp: &meta.Time{Time: time.Now()},
		},
	}

	pod0 := &core.Pod{
		ObjectMeta: meta.ObjectMeta{
			Name:      "pod0",
			Namespace: "ns0",
		},
		Spec: core.PodSpec{
			Volumes: []core.Volume{
				{
					Name: "v0",
					VolumeSource: core.VolumeSource{
						PersistentVolumeClaim: &core.PersistentVolumeClaimVolumeSource{
							ClaimName: "claim0",
						},
					},
				},
			},
		},
	}
	podScheduled := &core.Pod{
		ObjectMeta: meta.ObjectMeta{
			Name:      "pod0",
			Namespace: "ns0",
		},
		Spec: core.PodSpec{
			NodeName: node0.Name,
			Volumes: []core.Volume{
				{
					Name: "v0",
					VolumeSource: core.VolumeSource{
						PersistentVolumeClaim: &core.PersistentVolumeClaimVolumeSource{
							ClaimName: "claim0",
						},
					},
				},
			},
		},
	}

	tests := []struct {
		name    string
		objects []runtime.Object
		node    *core.Node
		want    []*core.Pod
		wantErr bool
	}{
		{
			name:    "nothing",
			node:    nodeOther,
			objects: []runtime.Object{nodeOther},
			want:    nil,
		},
		{
			name:    "no match",
			node:    nodeOther,
			objects: []runtime.Object{pv0, pvc0, nodeOther},
			want:    nil,
		},
		{
			name:    "match",
			node:    node0,
			objects: []runtime.Object{pv0, pvc0, pod0, node0},
			want:    []*core.Pod{pod0},
		},
		{
			name:    "match but deleted PVC",
			node:    node0,
			objects: []runtime.Object{pv0, pvc1Deleted, pod0, node0},
			want:    nil,
		},
		{
			name:    "match but scheduled",
			node:    node0,
			objects: []runtime.Object{pv0, pvc0, podScheduled, node0},
			want:    nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kclient := fake.NewSimpleClientset(tt.objects...)
			store, closeCh := RunStoreForTest(context.Background(), kclient)
			defer closeCh()
			got, err := GetUnscheduledPodsBoundToNodeByPV(tt.node, store, false, zap.NewNop())
			if (err != nil) != tt.wantErr {
				t.Errorf("GetUnscheduledPodsBoundToNodeByPV() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetUnscheduledPodsBoundToNodeByPV() got = %v, want %v", got, tt.want)
			}
		})
	}
}
