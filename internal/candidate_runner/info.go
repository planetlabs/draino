package candidate_runner

import (
	"encoding/json"
	"time"

	"github.com/planetlabs/draino/internal/scheduler"
	v1 "k8s.io/api/core/v1"
)

const (
	CandidateRunnerInfoKey        = "CandidateRunnerInfo"
	CandidateRunnerInfoCleanupKey = "CandidateRunnerCleanupInfo"
)

type DataInfo struct {
	// Candidate Run
	NodeCount                int           // initial node count in the group
	FilteredOutCount         int           // How many nodes were filtered out
	Slots                    int           // How many slots are available in total
	ProcessingDuration       time.Duration // How long does the loop took to run entirely
	LastRunTime              time.Time     // When was the runner loop launched the last time
	LastNodeIteratorTime     time.Time     // When did the loop had to node to iterate on AFTER the filtering part
	LastCandidates           []string      // Last candidate(s) that were tainted
	LastCandidatesTime       time.Time     // When was the last candidate(s) tainted
	LastSimulationRejections []string      // Nodes that were rejected by the drain simulation during the candidate evaluation
	LastRunRateLimited       bool          // Indicates if the last run was stopped because of client side rate limiting
	CurrentCandidates        []string      // Nodes that are currently in candidate state always: len(CurrentCandidtaes) <= Slots

	// private filed that should not go through the serialization
	lastNodeIterator scheduler.ItemProvider[*v1.Node] // Pointer to the last SortingTreeRepresentation as it was left by the last run.
}

type DataInfoForCleanupActivity struct {
	// Cleanup data
	CleanupLastTime           time.Time
	CleanupProcessingDuration time.Duration
}

func (d *DataInfo) Import(i interface{}) error {
	b, err := json.Marshal(i)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, d)
}

func (d *DataInfoForCleanupActivity) Import(i interface{}) error {
	b, err := json.Marshal(i)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, d)
}

// CandidateInfo Read only interface that is able to mimic he Candidate Runner behavior
type CandidateInfo interface {
	GetNodeIterator(node []*v1.Node) scheduler.ItemProvider[*v1.Node] // TODO consume this in a CLI command to display the tree
}

// CandidateRunnerInfo Read only interface that gives access to runtime information collected in the DataInfo
type CandidateRunnerInfo interface {
	GetLastNodeIteratorGraph(url bool) string
}

func (d *DataInfo) GetLastNodeIteratorGraph(url bool) string {
	if d.lastNodeIterator == nil {
		return "no last graph"
	}
	g := d.lastNodeIterator.(scheduler.SortingTree[*v1.Node])
	return g.AsDotGraph(url, func(n *v1.Node) string { return n.GetName() })
}

func (d *DataInfo) importLongLastingData(info DataInfo) {
	d.lastNodeIterator = info.lastNodeIterator

	d.LastNodeIteratorTime = info.LastNodeIteratorTime
	d.LastCandidates = info.LastCandidates
	d.LastCandidatesTime = info.LastCandidatesTime
}
