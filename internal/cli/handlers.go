package cli

import (
	"encoding/json"
	"github.com/go-logr/logr"
	"github.com/gorilla/mux"
	"github.com/planetlabs/draino/internal/candidate_runner"
	"github.com/planetlabs/draino/internal/drain_runner"
	"github.com/planetlabs/draino/internal/groups"
	"net/http"
)

type CLIHandlers struct {
	keysGetter    groups.RunnerInfoGetter
	candidateInfo candidate_runner.CandidateInfo
	drainInfo     drain_runner.DrainInfo
	logger        logr.Logger
}

func (c *CLIHandlers) Initialize(logger logr.Logger,
	keysGetter groups.RunnerInfoGetter,
	candidateInfo candidate_runner.CandidateInfo,
	drainInfo drain_runner.DrainInfo) error {

	c.keysGetter = keysGetter
	c.candidateInfo = candidateInfo
	c.drainInfo = drainInfo
	c.logger = logger.WithName("cliHandler")
	c.logger.Info("Initialized")
	return nil
}

func (c *CLIHandlers) RegisterRoute(m *mux.Router) {
	s := m.PathPrefix("/groups").Subrouter() //Handler(groupRouter)
	s.HandleFunc("/list", c.handleGroupsList)
}

// handleGroupsList list all groups
func (h *CLIHandlers) handleGroupsList(writer http.ResponseWriter, request *http.Request) {
	h.logger.Info("handleGroupsList", "path", request.URL.Path)
	var groups []groups.RunnerInfo

	for _, v := range h.keysGetter.GetRunnerInfo() {
		groups = append(groups, v)
	}

	writer.WriteHeader(http.StatusOK)
	data, err := json.Marshal(groups)
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	writer.Write(data)
}
