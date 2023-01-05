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
	NodeCount          int
	FilteredOutCount   int
	Slots              string
	ProcessingDuration time.Duration
	LastTime           time.Time
	// private filed that should not go through the serialization
	lastNodeIterator scheduler.ItemProvider[*v1.Node]
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

func (d DataInfo) GetLastNodeIteratorGraph(url bool) string {
	if d.lastNodeIterator == nil {
		return ""
	}
	g := d.lastNodeIterator.(scheduler.SortingTree[*v1.Node])
	return g.AsDotGraph(url, func(n *v1.Node) string { return n.GetName() })
}
