package main

import (
	"fmt"
	"os"

	"github.com/DataDog/compute-go/kubeclient"
	"github.com/spf13/cobra"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func main() {
	cfg, fs := kubeclient.ConfigFromFlags()
	cfgRest, err := kubeclient.NewKubeConfig(cfg)
	if err != nil {
		fmt.Printf("Failed to build client config: %#v\n", err)
		os.Exit(1)
	}
	kclient, err := client.New(cfgRest, client.Options{})
	if err != nil {
		fmt.Printf("Failed to build client: %#v\n", err)
		os.Exit(1)
	}
	// Read application flags

	root := &cobra.Command{
		Short:        "draino-tool",
		Long:         "draino-tool",
		SilenceUsage: true,
	}
	root.PersistentFlags().AddFlagSet(fs)
	root.AddCommand(TaintCmd(kclient))

	if err := root.Execute(); err != nil {
		fmt.Printf("root command exit with error: %#v", err)
	}
}
