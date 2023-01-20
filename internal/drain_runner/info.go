package drain_runner

import (
	"encoding/json"
	"time"
)

const (
	DrainRunnerInfo = "DrainRunnerInfo"
)

type DataInfo struct {
	ProcessingDuration time.Duration
	DrainBufferTill    time.Time
}

func (d *DataInfo) Import(i interface{}) error {
	b, err := json.Marshal(i)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, d)
}

type DrainInfo interface {
}
