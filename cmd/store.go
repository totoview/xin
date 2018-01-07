package cmd

import (
	"github.com/spf13/cobra"
)

var storeCmd = &cobra.Command{
	Use:    "store",
	Short:  "Run store service",
	Long:   ``,
	PreRun: func(cmd *cobra.Command, args []string) {},
	Run: func(cmd *cobra.Command, args []string) {
		runServices("store")
	},
}
