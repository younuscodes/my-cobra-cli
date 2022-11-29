package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(versionCmd)
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of App",
	Long:  `All software has versions. This is App's`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Version 1.0")
	},
}
