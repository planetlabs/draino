package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/DataDog/compute-go/kubeclient"
	"github.com/DataDog/compute-go/table"
	"github.com/spf13/cobra"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/planetlabs/draino/internal/cli"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
)

type taintCommandFlags struct {
	nodeName           string
	nodegroupName      string
	nodegroupNamespace string
	outputFormat       cli.OutputFormatType
	tableOutputParams  table.OutputParameters
	taintValue         string
	taintOverride      bool
}

var taintCmdFlags taintCommandFlags

func TaintCmd(kubecfg *kubeclient.Config) *cobra.Command {
	taintCmd := &cobra.Command{
		Use:     "taint",
		Aliases: []string{"taint", "taints"},
		Args:    cobra.MinimumNArgs(1),
		Run:     func(cmd *cobra.Command, args []string) {},
	}
	taintCmd.PersistentFlags().VarP(&taintCmdFlags.outputFormat, "output", "o", "output format (table|json)")
	taintCmd.PersistentFlags().StringVarP(&taintCmdFlags.nodeName, "node-name", "", "", "name of the node")
	taintCmd.PersistentFlags().StringVarP(&taintCmdFlags.nodegroupName, "nodegroup-name", "", "", "name of the nodegroup")
	taintCmd.PersistentFlags().StringVarP(&taintCmdFlags.nodegroupNamespace, "nodegroup-namespace", "", "", "namespace of the nodegroup")
	cli.SetTableOutputParameters(&taintCmdFlags.tableOutputParams, taintCmd.PersistentFlags())

	listCmd := &cobra.Command{
		Use:        "list",
		SuggestFor: []string{"list"},
		Args:       cobra.MaximumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			kclient, err := GetKubeClient(kubecfg)
			if err != nil {
				return err
			}
			nodes, err := listNodeFilter(kclient, taintCmdFlags.nodeName, taintCmdFlags.nodegroupName, taintCmdFlags.nodegroupNamespace)
			if err != nil {
				return err
			}
			output, err := FormatNodesOutput(nodes, nil)
			if err != nil {
				return err
			}
			fmt.Println(output)
			return nil
		},
	}

	deleteCmd := &cobra.Command{
		Use:        "delete",
		SuggestFor: []string{"delete"},
		Args:       cobra.MaximumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			kclient, err := GetKubeClient(kubecfg)
			if err != nil {
				return err
			}
			nodes, err := listNodeFilter(kclient, taintCmdFlags.nodeName, taintCmdFlags.nodegroupName, taintCmdFlags.nodegroupNamespace)
			if err != nil {
				return err
			}
			resultColumn := nodeExtraColumns{
				columnName: "Delete",
				data:       map[string]string{},
			}

			fmt.Printf("\nProcessing NLA candidate taint removal\n")
			for _, node := range nodes {
				t, f := k8sclient.GetNLATaint(node)
				if t == nil || !f {
					resultColumn.data[node.Name] = "no taint found"
					continue
				}
				if t.Value != taintCmdFlags.taintValue {
					resultColumn.data[node.Name] = fmt.Sprintf("skipping taint %s", t.Value)
					continue
				}

				if _, err := k8sclient.RemoveNLATaint(context.Background(), kclient, node); err != nil {
					resultColumn.data[node.Name] = fmt.Sprintf("err: %#v", err)
					continue
				}
				resultColumn.data[node.Name] = fmt.Sprintf("done")
			}
			output, err := FormatNodesOutput(nodes, []nodeExtraColumns{resultColumn})
			if err != nil {
				return err
			}

			fmt.Println(output)
			return nil
		},
	}

	addCmd := &cobra.Command{
		Use:        "add",
		SuggestFor: []string{"add"},
		Args:       cobra.MaximumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			if taintCmdFlags.nodeName == "" {
				return fmt.Errorf("a node-name must be set to add a NLA taint")
			}

			var taintValue k8sclient.DrainTaintValue
			switch taintCmdFlags.taintValue {
			case k8sclient.TaintDrained:
				taintValue = k8sclient.TaintDrained
			case k8sclient.TaintDraining:
				taintValue = k8sclient.TaintDraining
			case k8sclient.TaintDrainCandidate:
				taintValue = k8sclient.TaintDrainCandidate
			default:
				return fmt.Errorf("unknown NLA taint value")
			}

			kclient, err := GetKubeClient(kubecfg)
			if err != nil {
				return err
			}

			var node v1.Node
			if err := kclient.Get(context.Background(), types.NamespacedName{Name: taintCmdFlags.nodeName}, &node, &client.GetOptions{}); err != nil {
				return err
			}
			if _, found := k8sclient.GetNLATaint(&node); found {
				if !taintCmdFlags.taintOverride {
					return fmt.Errorf("NLA taint already present on the node. Use 'taint-override' flag if you want to set/reset it")
				}
			}

			k8sclient.AddNLATaint(context.Background(), kclient, &node, time.Now(), taintValue)
			return nil
		},
	}
	taintCmd.PersistentFlags().StringVarP(&taintCmdFlags.taintValue, "taint-value", "", k8sclient.TaintDrainCandidate, "NLA taint value to add/delete")
	taintCmd.PersistentFlags().BoolVarP(&taintCmdFlags.taintOverride, "taint-override", "", false, "allow taint override")

	taintCmd.AddCommand(listCmd, deleteCmd, addCmd)
	return taintCmd
}

type nodeExtraColumns struct {
	columnName string
	data       map[string]string
}

func FormatNodesOutput(nodes []*v1.Node, extraColumns []nodeExtraColumns) (string, error) {
	if taintCmdFlags.outputFormat == cli.FormatJSON {
		b, err := json.MarshalIndent(nodes, "", " ")
		if err != nil {
			return "", err
		}
		return string(b), nil
	}
	columns := []string{
		"Name", "NodegroupNamespace", "Nodegroup", "NLATaint",
	}
	for _, ext := range extraColumns {
		columns = append(columns, ext.columnName)
	}
	table := table.NewTable(columns,
		func(obj interface{}) []string {
			node := obj.(*v1.Node)
			ng, ngNs := NGValues(node)
			tValue := ""
			if taint, _ := k8sclient.GetNLATaint(node); taint != nil {
				tValue = taint.Value
			}
			values := []string{
				node.Name,
				ngNs,
				ng,
				tValue,
			}
			for _, ext := range extraColumns {
				values = append(values, ext.data[node.Name])
			}
			return values
		})

	for _, n := range nodes {
		table.Add(n)
	}
	taintCmdFlags.tableOutputParams.Apply(table)
	buf := bytes.NewBufferString("")
	err := table.Display(buf)
	return buf.String(), err
}

func NGValues(node *v1.Node) (name, ns string) {
	return node.Labels[kubernetes.LabelKeyNodeGroupName], node.Labels[kubernetes.LabelKeyNodeGroupNamespace]
}

func listNodeFilter(kclient client.Client, nodeName string, ngName string, ngNamespace string) ([]*v1.Node, error) {
	var nodes []v1.Node
	if nodeName != "" {
		var node v1.Node
		if err := kclient.Get(context.Background(), types.NamespacedName{Name: nodeName}, &node, &client.GetOptions{}); err != nil {
			return nil, err
		}
		if _, f := k8sclient.GetNLATaint(&node); !f {
			return nil, nil
		}

		ng, ngNs := NGValues(&node)
		if ngNamespace != "" && ngNamespace != ngNs {
			return nil, nil
		}
		if ngName != "" && ngName != ng {
			return nil, nil
		}
		nodes = append(nodes, node)
	} else {
		var listOfNodes v1.NodeList
		if err := kclient.List(context.Background(), &listOfNodes, &client.ListOptions{}); err != nil {
			return nil, err
		}
		nodes = listOfNodes.Items
	}
	var result []*v1.Node
	for i := range nodes {
		node := &nodes[i]
		if _, f := k8sclient.GetNLATaint(node); !f {
			continue
		}

		ng, ngNs := NGValues(node)
		if ngNamespace != "" && ngNamespace != ngNs {
			continue
		}
		if ngName != "" && ngName != ng {
			continue
		}
		result = append(result, node)
	}
	return result, nil
}
