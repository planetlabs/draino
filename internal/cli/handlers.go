package cli

import (
	"context"
	"encoding/json"
	"fmt"
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
	sg.HandleFunc("/nodes", c.handleGroupsNodes)
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

	data, err := json.Marshal(groups)
	if err != nil {
		h.logger.Error(err, "failed to marshal groups")
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	writer.WriteHeader(http.StatusOK)
	writer.Write(data)
}

// handleGroupsNodes display the list of nodes (and diagnostics) associated with a group
func (h *CLIHandlers) handleGroupsNodes(writer http.ResponseWriter, request *http.Request) {
	groupName := request.URL.Query().Get("group-name")
	h.logger.Info("handleGroupsNodes", "path", request.URL.Path, "groupName", groupName)

	nodes, err := h.candidateInfo.GetNodes(context.Background(), groups.GroupKey(groupName))
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	var result []interface{}
	for _, n := range nodes {
		// Here we are potentially making a lot of calls to layers like drainSimulation that could end up into rate limiting behind the scene
		// If we see an increasing usage (slack bot, or diagnostic service) of this service, we might need to rate limit the access to the diagnostics (or part of it) function
		// to ensure that regular operations still have tokens to operate.
		// Let's note that at the same time most of the layers have caches and that diagnostics contribute to cache warmup.
		result = append(result, h.diagnostics.GetNodeDiagnostic(context.Background(), n.Name))
	}
	data, err := json.Marshal(result)
	if err != nil {
		h.logger.Error(err, "failed to marshal diagnostic result")
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	writer.WriteHeader(http.StatusOK)
	writer.Write(data)
}

// handleGroupsGraphLast display the last sortingTree of the group as a graph
func (h *CLIHandlers) handleGroupsGraphLast(writer http.ResponseWriter, request *http.Request) {
	groupName := request.URL.Query().Get("group-name")
	h.logger.Info("handleGroupsGraphLast", "path", request.URL.Path, "groupName", groupName)

	candidateRunnerInfo, ok := h.GetCandidateRunnerInfo(writer, groupName)
	if !ok {
		return
	}

	data, err := json.Marshal(candidateRunnerInfo.GetLastNodeIteratorGraph(true))
	if err != nil {
		h.logger.Error(err, "failed to GetLastNodeIteratorGraph")
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	writer.WriteHeader(http.StatusOK)
	writer.Write(data)
}

func (h *CLIHandlers) GetCandidateRunnerInfo(writer http.ResponseWriter, groupName string) (candidate_runner.CandidateRunnerInfo, bool) {
	var group groups.RunnerInfo

	for k, v := range h.keysGetter.GetRunnerInfo() {
		if string(k) == groupName {
			group = v
		}
	}

	if group.Key == "" {
		h.logger.Info("handleGroupsGraphLast group not found", "groupName", groupName)
		writer.WriteHeader(http.StatusNotFound)
		return nil, false
	}

	if group.Data == nil {
		writer.WriteHeader(http.StatusNoContent)
		return nil, false
	}

	di, ok := group.Data.Get(candidate_runner.CandidateRunnerInfoKey)
	if !ok {
		writer.WriteHeader(http.StatusNoContent)
		return nil, false
	}
	candidateRunnerInfo, ok := (di).(candidate_runner.DataInfo)
	if !ok {
		h.logger.Error(fmt.Errorf("failed to cast CandidateRunnerInfo"), "failed to GetCandidateRunnerInfo")
		writer.WriteHeader(http.StatusInternalServerError)
		return nil, false
	}
	return &candidateRunnerInfo, true
}

// handleGroupsList list all groups
func (h *CLIHandlers) handleNodesDiagnostics(writer http.ResponseWriter, request *http.Request) {
	nodeName := request.URL.Query().Get("node-name")
	h.logger.Info("handleNodesDiagnostics", "path", request.URL.Path, "nodeName", nodeName)

	result := h.diagnostics.GetNodeDiagnostic(context.Background(), nodeName)

	data, err := json.Marshal(result)
	if err != nil {
		h.logger.Error(err, "failed to marshal diagnostic result")
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	writer.WriteHeader(http.StatusOK)
	writer.Write(data)
}
