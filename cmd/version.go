package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var CommitSHA string

func init() {
	rootCmd.AddCommand(version)
}

var version = &cobra.Command{
	Use:   "version",
	Short: "git commit version",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		fmt.Println(CommitSHA)
		return nil
	},
}
