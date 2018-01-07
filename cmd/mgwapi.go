package cmd

import (
	"github.com/spf13/cobra"
)

var mgwApiCmd = &cobra.Command{
	Use:    "mgwapi",
	Short:  "Run message gateway API service",
	Long:   ``,
	PreRun: func(cmd *cobra.Command, args []string) {},
	Run: func(cmd *cobra.Command, args []string) {
		runServices("mgwapi")
	},
}
