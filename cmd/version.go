package main

import (
	"fmt"

	"github.com/replicase/pgcapture"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(version)
}

var version = &cobra.Command{
	Use:   "version",
	Short: "git commit version",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		fmt.Printf("version: %s (%s)", pgcapture.Version, pgcapture.CommitSHA)
		return nil
	},
}
