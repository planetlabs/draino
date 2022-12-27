package candidate_runner

import (
	"encoding/json"
	"github.com/planetlabs/draino/internal/scheduler"
	v1 "k8s.io/api/core/v1"
	"time"
)

const (
	CandidateRunnerInfo = "CandidateRunnerInfo"
)

type DataInfo struct {
	NodeCount          int
	FilteredOutCount   int
	Slots              string
	ProcessingDuration string
	LastTime           time.Time
}

func (d *DataInfo) Import(i interface{}) error {
	b, err := json.Marshal(i)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, d)
}

type CandidateInfo interface {
	GetNodeIterator(node []*v1.Node) scheduler.ItemProvider[*v1.Node] // TODO consume this in a CLI command to display the tree
}
