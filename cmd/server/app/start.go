package app

import (
	"github.com/spf13/cobra"

	"github.com/Blackdeer1524/GraphDB/src/app"
)

func initStart() {
	rootCmd.AddCommand(&cobra.Command{
		Use:   "start",
		Short: "Starts server listening",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return app.Run(
				cmd.Context(),
				&app.ServerEntrypoint{ConfigPath: rootCmd.Options.ConfigPath},
			)
		},
	})
}
