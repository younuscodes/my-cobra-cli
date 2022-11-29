package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(addCmd)
}

var addCmd = &cobra.Command{
	Use:   "add",
	Short: "Print the version number of Hugo",
	Long:  `All software has versions. This is add cmd`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Add triggered")
		//Add()
	},
}
