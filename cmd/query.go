package cmd

import (
	"github.com/spf13/cobra"
)

var queryCmd = &cobra.Command{
	Use:    "query",
	Short:  "Run query service",
	Long:   ``,
	PreRun: func(cmd *cobra.Command, args []string) {},
	Run: func(cmd *cobra.Command, args []string) {
		runServices("query")
	},
}
