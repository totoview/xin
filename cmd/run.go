package cmd

import (
	"github.com/spf13/cobra"
)

var runCmd = &cobra.Command{
	Use:    "run [mgwapi|mgw|query|store] ...",
	Short:  "Run selected services",
	Long:   ``,
	PreRun: func(cmd *cobra.Command, args []string) {},
	Run: func(cmd *cobra.Command, args []string) {
		runServices(args...)
	},
}
