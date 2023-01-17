package cli

import (
	"context"
	"encoding/json"
	"github.com/go-logr/logr"
	"github.com/gorilla/mux"
	"github.com/planetlabs/draino/internal/candidate_runner"
	"github.com/planetlabs/draino/internal/diagnostics"
	"github.com/planetlabs/draino/internal/drain_runner"
	"github.com/planetlabs/draino/internal/groups"
	"net/http"
)

type CLIHandlers struct {
	keysGetter    groups.RunnerInfoGetter
	candidateInfo candidate_runner.CandidateInfo
	drainInfo     drain_runner.DrainInfo
	diagnostics   diagnostics.Diagnostician
	logger        logr.Logger
}

func (c *CLIHandlers) Initialize(logger logr.Logger,
	keysGetter groups.RunnerInfoGetter,
	candidateInfo candidate_runner.CandidateInfo,
	drainInfo drain_runner.DrainInfo,
	diagnostics diagnostics.Diagnostician) error {

	c.keysGetter = keysGetter
	c.candidateInfo = candidateInfo
	c.drainInfo = drainInfo
	c.logger = logger.WithName("cliHandler")
	c.diagnostics = diagnostics
	c.logger.Info("Initialized")
	return nil
}

func (c *CLIHandlers) RegisterRoute(m *mux.Router) {
	sg := m.PathPrefix("/groups").Subrouter() //Handler(groupRouter)
	sg.HandleFunc("/list", c.handleGroupsList)
	sg.HandleFunc("/graph/last", c.handleGroupsGraphLast)

	sn := m.PathPrefix("/nodes").Subrouter() //Handler(groupRouter)
	sn.HandleFunc("/diagnostics", c.handleNodesDiagnostics)
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

// handleGroupsList list all groups
func (h *CLIHandlers) handleGroupsGraphLast(writer http.ResponseWriter, request *http.Request) {
	groupName := request.URL.Query().Get("group-name")
	h.logger.Info("handleGroupsGraphLast", "path", request.URL.Path, "groupName", groupName)

	var group groups.RunnerInfo

	for k, v := range h.keysGetter.GetRunnerInfo() {
		if string(k) == groupName {
			group = v
		}
	}

	if group.Key == "" {
		h.logger.Info("handleGroupsGraphLast group not found", "groupName", groupName)
		writer.WriteHeader(http.StatusNotFound)
		return
	}

	if group.Data == nil {
		writer.WriteHeader(http.StatusNoContent)
		return
	}

	di, ok := group.Data.Get(candidate_runner.CandidateRunnerInfoKey)
	if !ok {
		writer.WriteHeader(http.StatusNoContent)
		return
	}

	candidateRunnerInfo, ok := (di).(candidate_runner.CandidateRunnerInfo)
	if !ok {
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}

	writer.WriteHeader(http.StatusOK)
	data, err := json.Marshal(candidateRunnerInfo.GetLastNodeIteratorGraph(true))
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	writer.Write(data)
}

// handleGroupsList list all groups
func (h *CLIHandlers) handleNodesDiagnostics(writer http.ResponseWriter, request *http.Request) {
	nodeName := request.URL.Query().Get("node-name")
	h.logger.Info("handleNodesDiagnostics", "path", request.URL.Path, "nodeName", nodeName)

	result := h.diagnostics.GetNodeDiagnostic(context.Background(), nodeName)

	writer.WriteHeader(http.StatusOK)
	data, err := json.Marshal(result)
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	writer.Write(data)
}
