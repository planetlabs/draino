package kubernetes

import (
	"reflect"
	"testing"
	"time"

	"errors"
	"go.uber.org/zap"
	core "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/util/flowcontrol"
)

var nodesTestMap = map[string]*core.Node{
	"A": &core.Node{
		ObjectMeta: v1.ObjectMeta{
			Name:   "A",
			Labels: map[string]string{"A": "A"},
		},
		Spec: core.NodeSpec{
			Taints: []core.Taint{
				{Key: "A", Value: "A"},
			},
		},
		Status: core.NodeStatus{
			Conditions: []core.NodeCondition{
				{Type: core.NodeReady, Status: core.ConditionTrue},
			},
		},
	},
	"A-cordon": &core.Node{
		ObjectMeta: v1.ObjectMeta{
			Name:   "A-cordon",
			Labels: map[string]string{"A": "A"},
		},
		Spec: core.NodeSpec{
			Taints: []core.Taint{
				{Key: "A", Value: "A"},
			},
			Unschedulable: true,
		},
		Status: core.NodeStatus{
			Conditions: []core.NodeCondition{
				{Type: core.NodeReady, Status: core.ConditionTrue},
			},
		},
	},
	"AB": &core.Node{
		ObjectMeta: v1.ObjectMeta{
			Name: "AB",
			Labels: map[string]string{
				"A": "A", "B": "B",
			},
		},
		Spec: core.NodeSpec{
			Taints: []core.Taint{
				{Key: "A", Value: "A"}, {Key: "B", Value: "B"},
			},
		},
		Status: core.NodeStatus{
			Conditions: []core.NodeCondition{
				{Type: core.NodeReady, Status: core.ConditionTrue},
			},
		},
	},
	"AB-Cordon": &core.Node{
		ObjectMeta: v1.ObjectMeta{
			Name: "AB-cordon",
			Labels: map[string]string{
				"A": "A", "B": "B",
			},
		},
		Spec: core.NodeSpec{
			Taints: []core.Taint{
				{Key: "A", Value: "A"}, {Key: "B", Value: "B"},
			},
			Unschedulable: true,
		},
		Status: core.NodeStatus{
			Conditions: []core.NodeCondition{
				{Type: core.NodeReady, Status: core.ConditionTrue},
			},
		},
	},
	"ABC": &core.Node{
		ObjectMeta: v1.ObjectMeta{
			Name: "ABC-cordon",
			Labels: map[string]string{
				"A": "A", "B": "B", "C": "C",
			},
		},
		Spec: core.NodeSpec{
			Taints: []core.Taint{
				{Key: "A", Value: "A"}, {Key: "B", Value: "B"}, {Key: "C", Value: "C"},
			},
		},
		Status: core.NodeStatus{
			Conditions: []core.NodeCondition{
				{Type: core.NodeReady, Status: core.ConditionTrue},
			},
		},
	},
	"ABC-Cordon": &core.Node{
		ObjectMeta: v1.ObjectMeta{
			Name: "AB-cordon",
			Labels: map[string]string{
				"A": "A", "B": "B", "C": "C",
			},
		},
		Spec: core.NodeSpec{
			Taints: []core.Taint{
				{Key: "A", Value: "A"}, {Key: "B", Value: "B"}, {Key: "C", Value: "C"},
			},
			Unschedulable: true,
		},
		Status: core.NodeStatus{
			Conditions: []core.NodeCondition{
				{Type: core.NodeReady, Status: core.ConditionTrue},
			},
		},
	},
	"D": &core.Node{
		ObjectMeta: v1.ObjectMeta{
			Name: "D",
			Labels: map[string]string{
				"D": "D",
			},
		},
		Spec: core.NodeSpec{
			Taints: []core.Taint{
				{Key: "D", Value: "D"}, {Key: TaintNodeNotReady, Value: "NotReady"},
			},
		},
		Status: core.NodeStatus{
			Conditions: []core.NodeCondition{
				{Type: core.NodeNetworkUnavailable, Status: core.ConditionTrue},
			},
		},
	},
}

func NodesMapAsSlice() []*core.Node {
	list := make([]*core.Node, len(nodesTestMap))
	i := 0
	for _, v := range nodesTestMap {
		list[i] = v
		i++
	}
	return list
}

type testNodeLister struct{}

func (t *testNodeLister) ListNodes() []*core.Node {
	return NodesMapAsSlice()
}

type testNodestore struct{}

func (f testNodestore) HasSynced() bool                     { return true }
func (f testNodestore) Get(name string) (*core.Node, error) { return nil, nil }
func (f testNodestore) ListNodes() []*core.Node {
	return NodesMapAsSlice()
}

var nodeStore = &testNodestore{}

func Test_getMatchingNodesForTaintCount(t *testing.T) {
	tests := []struct {
		name                 string
		selector             map[string]string
		nodes                []*core.Node
		wantCordonMatchCount int
		wantTotalMatchCount  int
	}{
		{
			name:                 "Nothing selected",
			selector:             map[string]string{"Other": "Value"},
			nodes:                NodesMapAsSlice(),
			wantCordonMatchCount: 0,
			wantTotalMatchCount:  0,
		},
		{
			name:                 "A=A Selection",
			selector:             map[string]string{"A": "A"},
			nodes:                NodesMapAsSlice(),
			wantCordonMatchCount: 3,
			wantTotalMatchCount:  6,
		},
		{
			name:                 "C=C Selection",
			selector:             map[string]string{"C": "C"},
			nodes:                NodesMapAsSlice(),
			wantCordonMatchCount: 1,
			wantTotalMatchCount:  2,
		},
		{
			name:                 "A=A,B=B Selection",
			selector:             map[string]string{"A": "A", "B": "B"},
			nodes:                NodesMapAsSlice(),
			wantCordonMatchCount: 2,
			wantTotalMatchCount:  4,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCordonMatchCount, gotTotalMatchCount := getMatchingNodesForTaintCount(tt.selector, tt.nodes)
			if gotCordonMatchCount != tt.wantCordonMatchCount {
				t.Errorf("getMatchingNodesForTaintCount() gotCordonMatchCount = %v, want %v", gotCordonMatchCount, tt.wantCordonMatchCount)
			}
			if gotTotalMatchCount != tt.wantTotalMatchCount {
				t.Errorf("getMatchingNodesForTaintCount() gotTotalMatchCount = %v, want %v", gotTotalMatchCount, tt.wantTotalMatchCount)
			}
		})
	}
}

func Test_getMatchingNodesForLabelsCount(t *testing.T) {
	tests := []struct {
		name                 string
		selector             map[string]string
		nodes                []*core.Node
		wantCordonMatchCount int
		wantTotalMatchCount  int
	}{
		{
			name:                 "Nothing selected",
			selector:             map[string]string{"Other": "Value"},
			nodes:                NodesMapAsSlice(),
			wantCordonMatchCount: 0,
			wantTotalMatchCount:  0,
		},
		{
			name:                 "A=A Selection",
			selector:             map[string]string{"A": "A"},
			nodes:                NodesMapAsSlice(),
			wantCordonMatchCount: 3,
			wantTotalMatchCount:  6,
		},
		{
			name:                 "C=C Selection",
			selector:             map[string]string{"C": "C"},
			nodes:                NodesMapAsSlice(),
			wantCordonMatchCount: 1,
			wantTotalMatchCount:  2,
		},
		{
			name:                 "A=A,B=B Selection",
			selector:             map[string]string{"A": "A", "B": "B"},
			nodes:                NodesMapAsSlice(),
			wantCordonMatchCount: 2,
			wantTotalMatchCount:  4,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCordonMatchCount, gotTotalMatchCount := getMatchingNodesCount(labels.SelectorFromSet(tt.selector), tt.nodes)
			if gotCordonMatchCount != tt.wantCordonMatchCount {
				t.Errorf("getMatchingNodesForTaintCount() gotCordonMatchCount = %v, want %v", gotCordonMatchCount, tt.wantCordonMatchCount)
			}
			if gotTotalMatchCount != tt.wantTotalMatchCount {
				t.Errorf("getMatchingNodesForTaintCount() gotTotalMatchCount = %v, want %v", gotTotalMatchCount, tt.wantTotalMatchCount)
			}
		})
	}
}

func TestLimiter_CanCordon(t *testing.T) {
	var isGloballyBlocked *bool = new(bool) // Limit by %NodeUnReqdy
	tests := []struct {
		name         string
		limiterfuncs map[string]LimiterFunc
		node         *core.Node
		want         bool
		want1        string
	}{
		{
			name:  "not limited",
			node:  nodesTestMap["AB"],
			want:  true,
			want1: "",
		},
		{
			name:  "already-cordon",
			node:  nodesTestMap["AB-Cordon"],
			want:  true,
			want1: "",
		},
		{
			name:         "global limit 3",
			node:         nodesTestMap["AB"],
			limiterfuncs: map[string]LimiterFunc{"limiter3": MaxSimultaneousCordonLimiterFunc(3, false)},
			want:         false,
			want1:        "limiter3",
		},
		{
			name:         "global limit 75% not met",
			node:         nodesTestMap["AB"],
			limiterfuncs: map[string]LimiterFunc{"limiter75%": MaxSimultaneousCordonLimiterFunc(75, true)},
			want:         true,
			want1:        "",
		},
		{
			name:         "global limit 40% met",
			node:         nodesTestMap["AB"],
			limiterfuncs: map[string]LimiterFunc{"limiter40%": MaxSimultaneousCordonLimiterFunc(40, true)},
			want:         false,
			want1:        "limiter40%",
		},
		{
			name: "global limit ok, but limit on taint block",
			node: nodesTestMap["AB"],
			limiterfuncs: map[string]LimiterFunc{
				"limiter75%":       MaxSimultaneousCordonLimiterFunc(75, true),
				"limiter40%-taint": MaxSimultaneousCordonLimiterForTaintsFunc(40, true, []string{"B"}),
				"limiter10-taint":  MaxSimultaneousCordonLimiterForTaintsFunc(10, false, []string{"B"}),
			},
			want:  false,
			want1: "limiter40%-taint",
		},
		{
			name: "limit on taint ok, but limit on labels block",
			node: nodesTestMap["AB"],
			limiterfuncs: map[string]LimiterFunc{
				"limiter75%-taint":   MaxSimultaneousCordonLimiterForTaintsFunc(75, true, []string{"A"}),
				"limiter-label-A3":   MaxSimultaneousCordonLimiterForLabelsFunc(3, false, []string{"A"}),
				"limiter-label-B80%": MaxSimultaneousCordonLimiterForLabelsFunc(80, true, []string{"B"}),
			},
			want:  false,
			want1: "limiter-label-A3",
		},
		{
			name: "limit on %labels ok",
			node: nodesTestMap["AB"],
			limiterfuncs: map[string]LimiterFunc{
				"limiter-label-B80%": MaxSimultaneousCordonLimiterForLabelsFunc(80, true, []string{"B"}),
			},
			want:  true,
			want1: "",
		},
		{
			name: "allow first node of the group",
			node: nodesTestMap["D"],
			limiterfuncs: map[string]LimiterFunc{
				"limiter75%-taint": MaxSimultaneousCordonLimiterForTaintsFunc(75, true, []string{"D"}),
			},
			want:  true,
			want1: "",
		},
		{
			name: "limit on 15%nodes NotReady m=10",
			node: nodesTestMap["D"],
			limiterfuncs: map[string]LimiterFunc{
				"limiter-notReady-10%": MaxNotReadyNodesFunc(10, true, nodeStore, isGloballyBlocked),
			},
			want:  false,
			want1: "limiter-notReady-10%",
		},
		{
			name: "limit on 15%nodes NotReady m=20",
			node: nodesTestMap["D"],
			limiterfuncs: map[string]LimiterFunc{
				"limiter-notReady-20%": MaxNotReadyNodesFunc(20, true, nodeStore, isGloballyBlocked),
			},
			want:  true,
			want1: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Limiter{
				logger:      zap.NewNop(),
				rateLimiter: flowcontrol.NewTokenBucketRateLimiter(200, 200),
			}
			l.SetNodeLister(&testNodeLister{})
			for k, v := range tt.limiterfuncs {
				l.AddLimiter(k, v)
			}

			got, got1 := l.CanCordon(tt.node)
			if got != tt.want {
				t.Errorf("CanCordon() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("CanCordon() got1 = %v, want %v", got1, tt.want1)
			}
			if *isGloballyBlocked == true && *isGloballyBlocked == tt.want {
				t.Errorf("CanCordon() isGloballyBlocked got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseCordonMaxForKeys(t *testing.T) {
	tests := []struct {
		param   string
		value   int
		percent bool
		keys    []string
		wantErr bool
	}{
		{
			param:   "0", //missing token
			value:   -1,
			percent: false,
			wantErr: true,
		},
		{
			param:   "1,one",
			value:   1,
			percent: false,
			keys:    []string{"one"},
		},
		{
			param:   "1%,one",
			value:   1,
			percent: true,
			keys:    []string{"one"},
		},
		{
			param:   "1%%,one",
			value:   -1,
			percent: true,
			wantErr: true,
		},
		{
			param:   "23,app,cluster",
			value:   23,
			percent: false,
			keys:    []string{"app", "cluster"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.param, func(t *testing.T) {
			got, got1, got2, err := ParseCordonMaxForKeys(tt.param)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseCordonMaxForKeys() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.value {
				t.Errorf("ParseCordonMaxForKeys() got = %v, want %v", got, tt.value)
			}
			if got1 != tt.percent {
				t.Errorf("ParseCordonMaxForKeys() got1 = %v, want %v", got1, tt.percent)
			}
			if !reflect.DeepEqual(got2, tt.keys) {
				t.Errorf("ParseCordonMaxForKeys() got2 = %v, want %v", got2, tt.keys)
			}
		})
	}
}

func TestIsLimiterError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "No",
			err:  errors.New("No"),
			want: false,
		},
		{
			name: "Yes",
			err:  NewLimiterError("Yes"),
			want: true,
		},
		{
			name: "nil",
			err:  nil,
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err != nil {
				if tt.err.Error() != tt.name {
					t.Errorf("errorMsg = %v, want %v", tt.err.Error(), tt.name)
				}
			}
			if got := IsLimiterError(tt.err); got != tt.want {
				t.Errorf("IsLimiterError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNodeReplacementLimiter(t *testing.T) {
	limiter := NewNodeReplacementLimiter(2, time.Now().Add(-2*time.Hour))
	if !limiter.CanAskForNodeReplacement() {
		t.FailNow()
	}
	if limiter.CanAskForNodeReplacement() {
		t.FailNow()
	}
	if limiter.CanAskForNodeReplacement() {
		t.FailNow()
	}
}
