package main

import (
	"fmt"

	"github.com/DataDog/compute-go/kubeclient"
	"github.com/spf13/cobra"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func main() {
	cfg, fs := kubeclient.ConfigFromFlags()
	// Read application flags

	root := &cobra.Command{
		Short:        "draino-tool",
		Long:         "draino-tool",
		SilenceUsage: true,
	}
	root.PersistentFlags().AddFlagSet(fs)
	root.AddCommand(TaintCmd(cfg))

	if err := root.Execute(); err != nil {
		fmt.Printf("root command exit with error: %#v", err)
	}
}

func GetKubeClient(cfg *kubeclient.Config) (client.Client, error) {
	cfgRest, err := kubeclient.NewKubeConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("Failed to build client config: %#v\n", err)
	}
	kclient, err := client.New(cfgRest, client.Options{})
	if err != nil {
		return nil, fmt.Errorf("Failed to build client: %#v\n", err)
	}
	return kclient, nil
}
