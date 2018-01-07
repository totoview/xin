package cmd

import (
	"github.com/spf13/cobra"
)

var mgwCmd = &cobra.Command{
	Use:    "mgw",
	Short:  "Run message gateway service",
	Long:   ``,
	PreRun: func(cmd *cobra.Command, args []string) {},
	Run: func(cmd *cobra.Command, args []string) {
		runServices("mgw")
	},
}
