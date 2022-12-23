package candidate_runner

import (
	"context"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/drain"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/kubernetes/pkg/util/taints"
	"k8s.io/utils/clock"
	testing2 "k8s.io/utils/clock/testing"
	"reflect"
	"strings"
	"testing"
	"time"
)

var clockInTest clock.Clock
var nowInTest = time.Now()

func setClockForTest() clock.Clock {
	if clockInTest != nil {
		return clockInTest
	}
	clockInTest = testing2.NewFakeClock(nowInTest)
	return clockInTest
}

func Test_candidateRunner_checkNodesLabels(t *testing.T) {

	n1In := &corev1.Node{
		ObjectMeta: v1.ObjectMeta{
			Labels: map[string]string{"in": "ok"},
		},
	}
	n2Out := &corev1.Node{
		ObjectMeta: v1.ObjectMeta{
			Labels: map[string]string{"in": "notIn"},
		},
	}
	n3Out := &corev1.Node{}

	tests := []struct {
		name  string
		nodes []*corev1.Node
		want  []*corev1.Node
	}{
		{name: "test", nodes: []*corev1.Node{n1In, n2Out, n3Out}, want: []*corev1.Node{n1In}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runner := &candidateRunner{
				nodeLabelsFilterFunc: func(o interface{}) bool {
					n, ok := o.(*corev1.Node)
					if !ok {
						panic("nota node")
					}
					if n.Labels == nil {
						return false
					}
					return n.Labels["in"] == "ok"
				},
			}
			if got := runner.checkNodesLabels(tt.nodes); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("checkNodesLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}

type fakeRetryWallWithDefinedTimeStamp struct {
	timestamp map[*corev1.Node]time.Time
}

var _ drain.RetryWall = &fakeRetryWallWithDefinedTimeStamp{}

func (f *fakeRetryWallWithDefinedTimeStamp) GetRetryWallTimestamp(node *corev1.Node) time.Time {
	return f.timestamp[node]
}

func (f *fakeRetryWallWithDefinedTimeStamp) SetNewRetryWallTimestamp(context.Context, *corev1.Node, string, time.Time) error {
	panic("implement me")
}
func (f *fakeRetryWallWithDefinedTimeStamp) GetDrainRetryAttemptsCount(*corev1.Node) int {
	panic("implement me")
}
func (f *fakeRetryWallWithDefinedTimeStamp) ResetRetryCount(context.Context, *corev1.Node) error {
	panic("implement me")
}
func (f *fakeRetryWallWithDefinedTimeStamp) IsAboveAlertingThreshold(*corev1.Node) bool {
	panic("implement me")
}

func Test_candidateRunner_checkNodesRetryWall(t *testing.T) {
	setClockForTest()
	n1, n2, n3 := &corev1.Node{}, &corev1.Node{}, &corev1.Node{}

	tests := []struct {
		name          string
		retryWall     drain.RetryWall
		nodes         []*corev1.Node
		wantKeep      []*corev1.Node
		wantFilterOut []*corev1.Node
	}{
		{
			name: "retryWall all in future",
			retryWall: &fakeRetryWallWithDefinedTimeStamp{timestamp: map[*corev1.Node]time.Time{
				n1: nowInTest.Add(time.Hour),
				n2: nowInTest.Add(time.Hour),
				n3: nowInTest.Add(time.Hour),
			}},
			nodes:         []*corev1.Node{n1, n2, n3},
			wantKeep:      []*corev1.Node{},
			wantFilterOut: []*corev1.Node{n1, n2, n3},
		},
		{
			name:          "retryWall all zero",
			retryWall:     &fakeRetryWallWithDefinedTimeStamp{timestamp: map[*corev1.Node]time.Time{}},
			nodes:         []*corev1.Node{n1, n2, n3},
			wantKeep:      []*corev1.Node{n1, n2, n3},
			wantFilterOut: []*corev1.Node{},
		},
		{
			name: "retryWall one zero,one future, one past",
			retryWall: &fakeRetryWallWithDefinedTimeStamp{timestamp: map[*corev1.Node]time.Time{
				n2: nowInTest.Add(time.Hour),
				n3: nowInTest.Add(-1 * time.Hour),
			}},
			nodes:         []*corev1.Node{n1, n2, n3},
			wantKeep:      []*corev1.Node{n1, n3},
			wantFilterOut: []*corev1.Node{n2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runner := &candidateRunner{
				retryWall: tt.retryWall,
				clock:     setClockForTest(),
			}
			gotKeep, gotFilterOut := runner.checkNodesRetryWall(tt.nodes)
			if !reflect.DeepEqual(gotKeep, tt.wantKeep) {
				t.Errorf("checkNodesRetryWall() gotKeep = %v, want %v", gotKeep, tt.wantKeep)
			}
			if !reflect.DeepEqual(gotFilterOut, tt.wantFilterOut) {
				t.Errorf("checkNodesRetryWall() gotFilterOut = %v, want %v", gotFilterOut, tt.wantFilterOut)
			}
		})
	}
}

func Test_candidateRunner_checkNodesTerminating(t *testing.T) {
	n1, n2, n3 := &corev1.Node{}, &corev1.Node{}, &corev1.Node{
		ObjectMeta: v1.ObjectMeta{
			DeletionTimestamp: &v1.Time{Time: nowInTest},
		},
	}
	tests := []struct {
		name          string
		nodes         []*corev1.Node
		wantKeep      []*corev1.Node
		wantFilterOut []*corev1.Node
	}{
		{
			name:          "keep non terminating nodes",
			nodes:         []*corev1.Node{n1, n2, n3},
			wantKeep:      []*corev1.Node{n1, n2},
			wantFilterOut: []*corev1.Node{n3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runner := &candidateRunner{}
			gotKeep, gotFilterOut := runner.checkNodesTerminating(tt.nodes)
			if !reflect.DeepEqual(gotKeep, tt.wantKeep) {
				t.Errorf("checkNodesTerminating() gotKeep = %v, want %v", gotKeep, tt.wantKeep)
			}
			if !reflect.DeepEqual(gotFilterOut, tt.wantFilterOut) {
				t.Errorf("checkNodesTerminating() gotFilterOut = %v, want %v", gotFilterOut, tt.wantFilterOut)
			}
		})
	}
}

func Test_candidateRunner_checkAlreadyCandidates(t *testing.T) {
	n0 := &corev1.Node{}
	n10 := &corev1.Node{}
	n100 := &corev1.Node{}

	n1Candidate := &corev1.Node{}
	taint := k8sclient.CreateNLATaint(k8sclient.TaintDrainCandidate, nowInTest)
	n1Candidate, _, _ = taints.AddOrUpdateTaint(n1Candidate, taint)

	n2Draining := &corev1.Node{}
	taint = k8sclient.CreateNLATaint(k8sclient.TaintDraining, nowInTest)
	n2Draining, _, _ = taints.AddOrUpdateTaint(n2Draining, taint)

	n3Drained := &corev1.Node{}
	taint = k8sclient.CreateNLATaint(k8sclient.TaintDrained, nowInTest)
	n3Drained, _, _ = taints.AddOrUpdateTaint(n3Drained, taint)

	tests := []struct {
		name                      string
		nodes                     []*corev1.Node
		maxCandidate              int
		wantRemainingNodes        []*corev1.Node
		wantAlreadyCandidateNodes []*corev1.Node
		wantMaxCandidateReached   bool
	}{
		{
			name:                      "check taints no max",
			nodes:                     []*corev1.Node{n0, n1Candidate, n2Draining, n10, n100, n3Drained},
			wantRemainingNodes:        []*corev1.Node{n0, n10, n100},
			wantAlreadyCandidateNodes: []*corev1.Node{n1Candidate, n2Draining, n3Drained},
			wantMaxCandidateReached:   false,
		},
		{
			name:                      "check taints max=2",
			maxCandidate:              2,
			nodes:                     []*corev1.Node{n0, n1Candidate, n2Draining, n10, n100, n3Drained},
			wantRemainingNodes:        nil,
			wantAlreadyCandidateNodes: []*corev1.Node{n1Candidate, n2Draining},
			wantMaxCandidateReached:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runner := &candidateRunner{
				maxSimultaneousCandidates: tt.maxCandidate,
			}
			gotRemainingNodes, gotAlreadyCandidateNodes, gotMaxCandidateReached := runner.checkAlreadyCandidates(tt.nodes)
			if !reflect.DeepEqual(gotRemainingNodes, tt.wantRemainingNodes) {
				t.Errorf("checkAlreadyCandidates() gotRemainingNodes = %v, want %v", gotRemainingNodes, tt.wantRemainingNodes)
			}
			if !reflect.DeepEqual(gotAlreadyCandidateNodes, tt.wantAlreadyCandidateNodes) {
				t.Errorf("checkAlreadyCandidates() gotAlreadyCandidateNodes = %v, want %v", gotAlreadyCandidateNodes, tt.wantAlreadyCandidateNodes)
			}
			if gotMaxCandidateReached != tt.wantMaxCandidateReached {
				t.Errorf("checkAlreadyCandidates() gotMaxCandidateReached = %v, want %v", gotMaxCandidateReached, tt.wantMaxCandidateReached)
			}
		})
	}
}

func Test_candidateRunner_checkCordonFilters(t *testing.T) {

	podFilterFuncBasedOnNodeName := func(p corev1.Pod) (pass bool, reason string, err error) {
		if strings.HasSuffix(p.Spec.NodeName, "ok") {
			return true, "", nil
		}
		return false, "testReason", nil
	}

	n1ok := &corev1.Node{
		ObjectMeta: v1.ObjectMeta{
			Name: "n1-ok",
		},
	}
	n2Notok := &corev1.Node{
		ObjectMeta: v1.ObjectMeta{
			Name: "n2-no",
		},
	}
	p1 := &corev1.Pod{
		ObjectMeta: v1.ObjectMeta{
			Name: "p1",
		},
		Spec: corev1.PodSpec{
			NodeName: "n1-ok",
		},
	}
	p2 := &corev1.Pod{
		ObjectMeta: v1.ObjectMeta{
			Name: "p2",
		},
		Spec: corev1.PodSpec{
			NodeName: "n2-no",
		},
	}
	tests := []struct {
		name          string
		podFilterFunc kubernetes.PodFilterFunc
		objects       []runtime.Object
		nodes         []*corev1.Node
		want          []*corev1.Node
	}{
		{
			name:          "test cordon filter",
			podFilterFunc: podFilterFuncBasedOnNodeName,
			objects:       []runtime.Object{n1ok, n2Notok, p1, p2},
			nodes:         []*corev1.Node{n1ok, n2Notok},
			want:          []*corev1.Node{n1ok},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, closingFunc := kubernetes.RunStoreForTest(context.Background(), fake.NewSimpleClientset(tt.objects...))
			defer closingFunc()

			runner := &candidateRunner{
				objectsStore: store,
				cordonFilter: tt.podFilterFunc,
			}
			if got := runner.checkCordonFilters(tt.nodes); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("checkCordonFilters() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_candidateRunner_checkNodesHaveAtLeastOneCondition(t *testing.T) {
	n1 := &corev1.Node{
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{
					Type:               "unknown",
					Status:             corev1.ConditionTrue,
					LastTransitionTime: v1.Time{Time: nowInTest.Add(-time.Hour)},
				},
				{
					Type:               "known",
					Status:             corev1.ConditionTrue,
					LastTransitionTime: v1.Time{Time: nowInTest.Add(-time.Hour)},
				},
			},
		},
	}
	n2 := &corev1.Node{
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{
					Type:               "unknown",
					Status:             corev1.ConditionTrue,
					LastTransitionTime: v1.Time{Time: nowInTest.Add(-time.Hour)},
				},
			},
		},
	}
	n3 := &corev1.Node{
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{
					Type:               "known",
					Status:             corev1.ConditionTrue,
					LastTransitionTime: v1.Time{Time: nowInTest.Add(-time.Hour)},
				},
			},
		},
	}
	n4 := &corev1.Node{
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{
					Type:               "known",
					Status:             corev1.ConditionFalse,
					LastTransitionTime: v1.Time{Time: nowInTest.Add(-time.Hour)},
				},
			},
		},
	}
	tests := []struct {
		name       string
		conditions []kubernetes.SuppliedCondition
		nodes      []*corev1.Node
		want       []*corev1.Node
	}{
		{
			name: "no matching conditions",
			conditions: []kubernetes.SuppliedCondition{
				{
					Type:   "other",
					Status: corev1.ConditionTrue,
				},
			},
			nodes: []*corev1.Node{n1, n2, n3, n4},
			want:  []*corev1.Node{},
		},
		{
			name: "matching known condition",
			conditions: []kubernetes.SuppliedCondition{
				{
					Type:   "known",
					Status: corev1.ConditionTrue,
				},
			},
			nodes: []*corev1.Node{n1},
			want:  []*corev1.Node{n1},
		},

		{
			name: "matching known condition",
			conditions: []kubernetes.SuppliedCondition{
				{
					Type:   "known",
					Status: corev1.ConditionTrue,
				},
			},
			nodes: []*corev1.Node{n1, n2, n3, n4},
			want:  []*corev1.Node{n1, n3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runner := &candidateRunner{
				clock: setClockForTest(),
				globalConfig: kubernetes.GlobalConfig{
					SuppliedConditions: tt.conditions,
				},
			}
			if got := runner.checkNodesHaveAtLeastOneCondition(tt.nodes); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("checkNodesHaveAtLeastOneCondition() = %v, want %v", got, tt.want)
			}
		})
	}
}
