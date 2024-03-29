// Copyright © 2020 Dmitry Mozzherin <dmozzherin@gmail.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cmd

import (
	"log/slog"
	"os"

	"github.com/gnames/gnidump/internal/io/dumpio"
	gnidump "github.com/gnames/gnidump/pkg"
	"github.com/gnames/gnidump/pkg/config"
	"github.com/spf13/cobra"
)

// dumpCmd represents the dump command
var dumpCmd = &cobra.Command{
	Use:   "dump",
	Short: "Dumps GNI data to CSV files.",
	Run: func(_ *cobra.Command, _ []string) {
		cfg := config.New(opts...)
		gnd := gnidump.New(cfg)
		d, err := dumpio.New(cfg)
		if err != nil {
			slog.Error("Cannot create Dumper.", "error", err)
			os.Exit(1)
		}

		err = gnd.Dump(d)
		if err != nil {
			slog.Error("Cannot create data dump", "error", err)
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(dumpCmd)
}
