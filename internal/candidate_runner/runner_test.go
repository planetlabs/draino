package candidate_runner

import (
	"reflect"
	"testing"
	"time"

	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/util/taints"
	"k8s.io/utils/clock"
	testing2 "k8s.io/utils/clock/testing"
)

var clockInTest clock.Clock

func setClockForTest() clock.Clock {
	if clockInTest != nil {
		return clockInTest
	}

	var nowInTest = time.Now()

	clockInTest = testing2.NewFakeClock(nowInTest)
	return clockInTest
}

func Test_candidateRunner_checkAlreadyCandidates(t *testing.T) {
	setClockForTest()
	n0 := &corev1.Node{}
	n10 := &corev1.Node{}
	n100 := &corev1.Node{}

	n1Candidate := &corev1.Node{}
	taint := k8sclient.CreateNLATaint(k8sclient.TaintDrainCandidate, clockInTest.Now())
	n1Candidate, _, _ = taints.AddOrUpdateTaint(n1Candidate, taint)

	n2Draining := &corev1.Node{}
	taint = k8sclient.CreateNLATaint(k8sclient.TaintDraining, clockInTest.Now())
	n2Draining, _, _ = taints.AddOrUpdateTaint(n2Draining, taint)

	n3Drained := &corev1.Node{}
	taint = k8sclient.CreateNLATaint(k8sclient.TaintDrained, clockInTest.Now())
	n3Drained, _, _ = taints.AddOrUpdateTaint(n3Drained, taint)

	n4Drained := &corev1.Node{}
	taint = k8sclient.CreateNLATaint(k8sclient.TaintDrained, clockInTest.Now())
	n4Drained, _, _ = taints.AddOrUpdateTaint(n4Drained, taint)

	tests := []struct {
		name                      string
		nodes                     []*corev1.Node
		maxCandidate              int
		maxDrained                int
		wantRemainingNodes        []*corev1.Node
		wantAlreadyCandidateNodes []*corev1.Node
		wantAlreadyDrainedNodes   []*corev1.Node
		wantMaxCandidateReached   bool
		wantMaxDrainedReached     bool
	}{
		{
			name:                      "check taints no max",
			nodes:                     []*corev1.Node{n0, n1Candidate, n2Draining, n10, n100, n3Drained},
			wantRemainingNodes:        []*corev1.Node{n0, n10, n100},
			wantAlreadyCandidateNodes: []*corev1.Node{n1Candidate, n2Draining},
			wantMaxCandidateReached:   false,
		},
		{
			name:                      "check taints maxCandidate=2",
			maxCandidate:              2,
			nodes:                     []*corev1.Node{n0, n1Candidate, n2Draining, n10, n100, n3Drained},
			wantRemainingNodes:        nil,
			wantAlreadyCandidateNodes: []*corev1.Node{n1Candidate, n2Draining},
			wantMaxCandidateReached:   true,
		},
		{
			name:                      "maxCandidate=1, maxDrained=2, maxDrained reached",
			maxCandidate:              1,
			maxDrained:                2,
			nodes:                     []*corev1.Node{n0, n3Drained, n4Drained, n10, n100},
			wantRemainingNodes:        nil,
			wantAlreadyCandidateNodes: []*corev1.Node{},
			wantMaxCandidateReached:   false,
			wantMaxDrainedReached:     true,
		},
		{
			name:                      "maxCandidate=1, maxDrained=2, maxDrained not reached",
			maxCandidate:              1,
			maxDrained:                2,
			nodes:                     []*corev1.Node{n0, n3Drained, n10, n100},
			wantRemainingNodes:        []*corev1.Node{n0, n10, n100},
			wantAlreadyCandidateNodes: []*corev1.Node{},
			wantMaxCandidateReached:   false,
			wantMaxDrainedReached:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runner := &candidateRunner{
				maxSimultaneousCandidates: tt.maxCandidate,
				maxSimultaneousDrained:    tt.maxDrained,
			}
			gotRemainingNodes, gotAlreadyCandidateNodes, gotAlreadyDrainedNodes, gotMaxCandidateReached, gotMaxDrainedReached := runner.checkAlreadyCandidates(tt.nodes)
			if !reflect.DeepEqual(gotRemainingNodes, tt.wantRemainingNodes) {
				t.Errorf("checkAlreadyCandidates() gotRemainingNodes = %v, want %v", gotRemainingNodes, tt.wantRemainingNodes)
			}
			if !reflect.DeepEqual(gotAlreadyCandidateNodes, tt.wantAlreadyCandidateNodes) {
				t.Errorf("checkAlreadyCandidates() gotAlreadyCandidateNodes = %v, want %v", gotAlreadyCandidateNodes, tt.wantAlreadyCandidateNodes)
			}
			if !reflect.DeepEqual(gotAlreadyCandidateNodes, tt.wantAlreadyCandidateNodes) {
				t.Errorf("checkAlreadyCandidates() gotAlreadyDrainedNodes = %v, want %v", gotAlreadyDrainedNodes, tt.wantAlreadyDrainedNodes)
			}
			if gotMaxCandidateReached != tt.wantMaxCandidateReached {
				t.Errorf("checkAlreadyCandidates() gotMaxCandidateReached = %v, want %v", gotMaxCandidateReached, tt.wantMaxCandidateReached)
			}
			if gotMaxDrainedReached != tt.wantMaxDrainedReached {
				t.Errorf("checkAlreadyCandidates() gotMaxDrainedReached = %v, want %v", gotMaxDrainedReached, tt.wantMaxDrainedReached)
			}
		})
	}
}
