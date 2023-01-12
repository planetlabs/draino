package cli

import (
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/DataDog/compute-go/table"
	"github.com/planetlabs/draino/internal/candidate_runner"
	"github.com/planetlabs/draino/internal/diagnostics"
	"github.com/planetlabs/draino/internal/drain_runner"
	"github.com/planetlabs/draino/internal/groups"
)

type CLICommands struct {
	ServerAddr *string

	groupName string

	tableOutputParams table.OutputParameters
	outputFormat      outputFormatType
	nodeName          string
}

func (h *CLICommands) Commands() []*cobra.Command {
	return []*cobra.Command{h.buildGroupCmd(), h.buildNodeCmd()}
}

func (h *CLICommands) setTableFlags(f *pflag.FlagSet) {
	f.VarP(&h.outputFormat, "output", "o", "output format (table|json)")
	f.BoolVarP(&h.tableOutputParams.NoHeader, "no-header", "", false, "do not display table header")
	f.StringVarP(&h.tableOutputParams.Separator, "separator", "s", "\t|", "column Separator in table output")
	f.IntVarP(&h.tableOutputParams.Padding, "padding", "", 3, "Padding in table output")
	f.StringArrayVarP(&h.tableOutputParams.Sort, "sort", "", []string{"group"}, "comma separated list of columns for sorting table output")
	f.StringArrayVarP(&h.tableOutputParams.ColumnsVisible, "visible", "", nil, "comma separated list of visible columns for table output")
	f.StringArrayVarP(&h.tableOutputParams.ColumnsHide, "hidden", "", nil, "comma separated list of hidden columns for table output")
	f.StringArrayVarP(&h.tableOutputParams.Filter, "filter", "", nil, "filtering expression for table output")
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

	groupCmd.AddCommand(groupListCmd, groupGraphCmd)
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

func (h *CLICommands) cmdGroupList() error {
	b, err := ReadFromURL("http://" + *h.ServerAddr + "/groups/list")
	if err != nil {
		return err
	}

	if h.outputFormat == formatJSON {
		fmt.Printf("%s", string(b))
		return nil
	}

	var result []groups.RunnerInfo
	if err := json.Unmarshal(b, &result); err != nil {
		return err
	}

	table := table.NewTable([]string{
		"Group", "Nodes", "Slot", "Filtered", "Warn", "last candidate run", "candidate duration", "drain duration",
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
				if !strings.HasPrefix(candidateDataInfo.Slots, "0") && candidateDataInfo.NodeCount > candidateDataInfo.FilteredOutCount {
					warn = "*"
				}

			}
			return []string{
				string(item.Key),
				fmt.Sprintf("%v", candidateDataInfo.NodeCount),
				fmt.Sprintf("%v", candidateDataInfo.Slots),
				fmt.Sprintf("%v", candidateDataInfo.FilteredOutCount),
				fmt.Sprintf("%s", warn),
				fmt.Sprintf("%v", candidateDataInfo.LastTime),
				fmt.Sprintf("%v", candidateDataInfo.ProcessingDuration.String()),
				fmt.Sprintf("%v", drainDataInfo.ProcessingDuration.String()),
			}
		})

	for _, s := range result {
		table.Add(s)
	}
	h.tableOutputParams.Apply(table)
	table.Display(os.Stdout)

	return nil
}
