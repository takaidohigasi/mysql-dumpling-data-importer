package cmd

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
)

// Version is the version of mysql-dumpling-data-importer
var Version = "dev"

// versionCmd represents the version command
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "show the version of mysql-dumpling-data-importer",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf(
			"mysql-dumpling-data-importer version %s built with %s %s %s\n",
			Version, runtime.Version(), runtime.GOOS, runtime.GOARCH,
		)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
