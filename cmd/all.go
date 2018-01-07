package cmd

import (
	"github.com/spf13/cobra"
)

var allCmd = &cobra.Command{
	Use:    "all",
	Short:  "Run all services (query, store, mgw, mgwapi)",
	Long:   ``,
	PreRun: func(cmd *cobra.Command, args []string) {},
	Run: func(cmd *cobra.Command, args []string) {
		runServices("query", "store", "mgw", "mgwapi")
	},
}
