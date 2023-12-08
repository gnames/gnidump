/*
Copyright © 2020 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"log/slog"
	"os"

	"github.com/gnames/gnidump"
	"github.com/spf13/cobra"
)

// rebuildCmd represents the rebuild command
var rebuildCmd = &cobra.Command{
	Use:   "rebuild",
	Short: "Uses CSV dump files to recreate GNI database for PostgreSQL",
	Run: func(_ *cobra.Command, _ []string) {
		gnd := gnidump.NewGNIdump(opts...)
		err := gnd.PopulatePG()
		if err != nil {
			slog.Error("Cannot populate database", "error", err)
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(rebuildCmd)
}
