package cmd

import (
	"github.com/spf13/cobra"
	"github.com/totoview/xin/cmd/bench"
)

func init() {
	benchCmd.AddCommand(bench.ProducerQueueCmd, bench.BoltQueueCmd)
}

var benchCmd = &cobra.Command{
	Use:              "bench",
	Short:            "Run benchmarks",
	Long:             ``,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {},
}
