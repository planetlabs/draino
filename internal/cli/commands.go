package cli

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/DataDog/compute-go/table"
	"k8s.io/apimachinery/pkg/util/duration"

	"github.com/planetlabs/draino/internal/candidate_runner"
	"github.com/planetlabs/draino/internal/diagnostics"
	"github.com/planetlabs/draino/internal/drain_runner"
	"github.com/planetlabs/draino/internal/groups"
)

type CLICommands struct {
	ServerAddr *string

	groupName string

	tableOutputParams table.OutputParameters
	outputFormat      OutputFormatType
	perferDuration    bool
	nodeName          string
}

func (h *CLICommands) Commands() []*cobra.Command {
	return []*cobra.Command{h.buildGroupCmd(), h.buildNodeCmd()}
}

func (h *CLICommands) setTableFlags(f *pflag.FlagSet) {
	f.VarP(&h.outputFormat, "output", "o", "output format (table|json)")
	f.BoolVarP(&h.perferDuration, "prefer-duration", "", true, "display duration instead of timestamp where it makes sense")
	SetTableOutputParameters(&h.tableOutputParams, f)
}

func SetTableOutputParameters(t *table.OutputParameters, f *pflag.FlagSet) {
	f.BoolVarP(&t.NoHeader, "no-header", "", false, "do not display table header")
	f.StringVarP(&t.Separator, "separator", "s", "\t|", "column Separator in table output")
	f.IntVarP(&t.Padding, "padding", "", 3, "Padding in table output")
	f.StringArrayVarP(&t.Sort, "sort", "", []string{"group"}, "comma separated list of columns for sorting table output")
	f.StringArrayVarP(&t.ColumnsVisible, "visible", "", nil, "comma separated list of visible columns for table output")
	f.StringArrayVarP(&t.ColumnsHide, "hidden", "", nil, "comma separated list of hidden columns for table output")
	f.StringArrayVarP(&t.Filter, "filter", "", nil, "filtering expression for table output")
}

func (h *CLICommands) buildGroupCmd() *cobra.Command {
	groupCmd := &cobra.Command{
		Use:        "group",
		SuggestFor: []string{"group", "groups"},
		Args:       cobra.MaximumNArgs(2),
		Run:        func(cmd *cobra.Command, args []string) {},
	}

	h.setTableFlags(groupCmd.PersistentFlags())
	groupCmd.PersistentFlags().StringVarP(&h.groupName, "group-name", "", "", "name of the group")

	groupListCmd := &cobra.Command{
		Use:        "list",
		SuggestFor: []string{"list"},
		Args:       cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return h.cmdGroupList()
		},
	}
	groupNodesCmd := &cobra.Command{
		Use:        "nodes",
		SuggestFor: []string{"nodes"},
		Args:       cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return h.cmdGroupNodes()
		},
	}

	groupGraphCmd := &cobra.Command{
		Use:        "graph",
		SuggestFor: []string{"graph"},
		Args:       cobra.ExactArgs(1),
		Run:        func(cmd *cobra.Command, args []string) {},
	}
	groupGraphLastCmd := &cobra.Command{
		Use:        "last",
		SuggestFor: []string{"last"},
		Args:       cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return h.cmdGroupGraphLast()
		},
	}
	groupGraphCmd.AddCommand(groupGraphLastCmd)

	groupCmd.AddCommand(groupListCmd, groupNodesCmd, groupGraphCmd)
	return groupCmd
}

func (h *CLICommands) buildNodeCmd() *cobra.Command {
	nodeCmd := &cobra.Command{
		Use:        "node",
		SuggestFor: []string{"node", "nodes"},
		Args:       cobra.MaximumNArgs(2),
		Run:        func(cmd *cobra.Command, args []string) {},
	}

	h.setTableFlags(nodeCmd.PersistentFlags())
	nodeCmd.PersistentFlags().StringVarP(&h.nodeName, "node-name", "", "", "name of the node")

	nodeDiagnosticsCmd := &cobra.Command{
		Use:        "diagnostics",
		SuggestFor: []string{"diagnostics"},
		Args:       cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return h.cmdNodeDiagnostics()
		},
	}

	nodeCmd.AddCommand(nodeDiagnosticsCmd)
	return nodeCmd
}

func ReadFromURL(url string) ([]byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("Error: " + err.Error())
		return nil, err
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error: " + err.Error())
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("%s\n%s", resp.Status, string(b))
	}
	return b, nil
}

func (h *CLICommands) cmdNodeDiagnostics() error {
	params := url.Values{}
	params.Add("node-name", h.nodeName)
	b, err := ReadFromURL("http://" + *h.ServerAddr + "/nodes/diagnostics?" + params.Encode())
	if err != nil {
		return err
	}

	var nd diagnostics.NodeDiagnostics
	if errMarshall := json.Unmarshal(b, &nd); errMarshall != nil {
		return errMarshall
	}

	bPretty, errIndent := json.MarshalIndent(nd, "", "  ")
	if errIndent != nil {
		return errIndent
	}
	fmt.Println(string(bPretty))
	return nil
}

func (h *CLICommands) cmdGroupGraphLast() error {
	params := url.Values{}
	params.Add("group-name", h.groupName)
	b, err := ReadFromURL("http://" + *h.ServerAddr + "/groups/graph/last?" + params.Encode())
	if err != nil {
		return err
	}

	fmt.Println(string(b))
	return nil
}

func (h *CLICommands) outputDurationOrTimestamp(t time.Time) string {
	if t.IsZero() {
		return "NA"
	}
	if h.perferDuration {
		now := time.Now()
		if t.After(now) {
			return "+" + duration.ShortHumanDuration(t.Sub(now))
		}
		return "-" + duration.ShortHumanDuration(time.Since(t))
	}
	return t.Format(time.RFC3339)
}

func (h *CLICommands) cmdGroupNodes() error {
	params := url.Values{}
	params.Add("group-name", h.groupName)
	b, err := ReadFromURL("http://" + *h.ServerAddr + "/groups/nodes?" + params.Encode())
	if err != nil {
		return err
	}

	if h.outputFormat == FormatJSON {
		fmt.Printf("%s", string(b))
		return nil
	}

	var result []diagnostics.NodeDiagnostics
	if err := json.Unmarshal(b, &result); err != nil {
		return err
	}

	table := table.NewTable([]string{
		"Node", "Namespace", "NodeGroup", "Zone", "Taint", "FilteredOut", "Retry", "Retry After", "Conditions", "StabilityPeriod", "Drain Buffer Cfg", "CanDrain",
	}, func(obj interface{}) []string {
		item := obj.(diagnostics.NodeDiagnostics)

		retryCount := "-"
		retryAfter := "-"
		if item.Retry != nil {
			retryCount = strconv.Itoa(item.Retry.RetryCount)
			retryAfter = item.Retry.NextAttemptAfter.Format(time.RFC3339)
		}
		drainBufferCfg := "-"
		if item.DrainBufferConfig != nil {
			var values []string
			for _, v := range item.DrainBufferConfig.ValuesWithoutDupe() {
				values = append(values, duration.ShortHumanDuration(v))
			}
			if len(values) > 0 {
				drainBufferCfg = strings.Join(values, ",")
			}
		}
		var conditions []string
		for _, c := range item.Conditions {
			conditions = append(conditions, string(c.Type))
		}
		return []string{
			item.Node,
			item.Namespace,
			item.Nodegroup,
			item.Zone,
			item.TaintNLA,
			strconv.FormatBool(!item.Filters.Keep),
			retryCount,
			retryAfter,
			strings.Join(conditions, ","),
			strconv.FormatBool(item.StabilityPeriodOk),
			drainBufferCfg,
			strconv.FormatBool(item.DrainSimulation.CanDrain),
		}
	})
	for _, s := range result {
		table.Add(s)
	}
	h.tableOutputParams.Apply(table)
	table.Display(os.Stdout)
	return nil
}

func (h *CLICommands) cmdGroupList() error {
	b, err := ReadFromURL("http://" + *h.ServerAddr + "/groups/list")
	if err != nil {
		return err
	}

	if h.outputFormat == FormatJSON {
		fmt.Printf("%s", string(b))
		return nil
	}

	var result []groups.RunnerInfo
	if err := json.Unmarshal(b, &result); err != nil {
		return err
	}

	table := table.NewTable([]string{
		"Group", "Nodes", "Slot", "Filtered", "Simulation failed", "Cond. Rate Limited", "Warn", "Last Run Aborted", "Last Candidate Run", "Candidate Duration", "Last Candidate(s)", "Last Candidate(s) At", "Last Candidates Sort", "Drain Duration", "Drain Buffer",
	},
		func(obj interface{}) []string {
			item := obj.(groups.RunnerInfo)

			raw, _ := item.Data.Get(candidate_runner.CandidateRunnerInfoKey)
			var candidateDataInfo candidate_runner.DataInfo
			candidateDataInfo.Import(raw)

			raw, _ = item.Data.Get(drain_runner.DrainRunnerInfo)
			var drainDataInfo drain_runner.DataInfo
			drainDataInfo.Import(raw)

			warn := ""
			if candidateDataInfo.NodeCount > 0 {
				// Show warning if there are free slots available and we have potential candidates that fail on the way to get the taint
				if len(candidateDataInfo.CurrentCandidates) < candidateDataInfo.Slots && candidateDataInfo.NodeCount > candidateDataInfo.FilteredOutCount {
					warn = "*"
				}

			}

			lastRunAborted := ""
			if candidateDataInfo.LastRunRateLimited {
				lastRunAborted = "*"
			}

			remainingSlots := candidateDataInfo.Slots - len(candidateDataInfo.CurrentCandidates)
			return []string{
				string(item.Key),
				fmt.Sprintf("%v", candidateDataInfo.NodeCount),
				fmt.Sprintf("%d/%d", remainingSlots, candidateDataInfo.Slots),
				fmt.Sprintf("%v", candidateDataInfo.FilteredOutCount),
				fmt.Sprintf("%v", candidateDataInfo.LastSimulationRejections),
				fmt.Sprintf("%d", len(candidateDataInfo.LastConditionRateLimitRejections)),
				fmt.Sprintf("%s", warn),
				fmt.Sprintf("%s", lastRunAborted),
				h.outputDurationOrTimestamp(candidateDataInfo.LastRunTime),
				fmt.Sprintf("%v", candidateDataInfo.ProcessingDuration.String()),
				strings.Join(candidateDataInfo.LastCandidates, ","),
				h.outputDurationOrTimestamp(candidateDataInfo.LastCandidatesTime),
				h.outputDurationOrTimestamp(candidateDataInfo.LastNodeIteratorTime),
				fmt.Sprintf("%v", drainDataInfo.ProcessingDuration.String()),
				h.outputDurationOrTimestamp(drainDataInfo.DrainBufferTill),
			}
		})

	for _, s := range result {
		table.Add(s)
	}
	h.tableOutputParams.Apply(table)
	table.Display(os.Stdout)

	return nil
}
