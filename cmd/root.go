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
	_ "embed"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	gnidump "github.com/gnames/gnidump/pkg"
	"github.com/gnames/gnidump/pkg/config"
	"github.com/gnames/gnsys"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

//go:embed gnidump.yaml
var configText string

var (
	opts []config.Option
)

type cfgData struct {
	InputDir string
	MyHost   string
	MyUser   string
	MyPass   string
	MyDB     string
	PgHost   string
	PgUser   string
	PgPass   string
	PgDB     string
	JobsNum  int
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "gnidump",
	Short: "A brief description of your application",
	Long: `A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		version, err := cmd.Flags().GetBool("version")
		if err != nil {
			slog.Error("Cannot get flag", "error", err)
			os.Exit(1)
		}
		if version {
			fmt.Printf("\nversion: %s\nbuild: %s\n\n", gnidump.Version, gnidump.Build)
			os.Exit(0)
		}

		fmt.Println("YML " + configText)

		if len(args) == 0 {
			_ = cmd.Help()
			os.Exit(0)
		}
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.Flags().BoolP("version", "V", false, "Returns version and build date")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	var err error
	var homeDir, cfgDir string
	configFile := "gnidump"

	// Find home directory.
	homeDir, err = os.UserHomeDir()
	if err != nil {
		slog.Error("Cannot find home dir", "error", err)
		os.Exit(1)
	}
	cfgDir = filepath.Join(homeDir, ".config")

	// Search config in home directory with name ".gnidump" (without extension).
	viper.AddConfigPath(cfgDir)
	viper.SetConfigName(configFile)

	configPath := filepath.Join(cfgDir, fmt.Sprintf("%s.yaml", configFile))
	touchConfigFile(configPath)

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err != nil {
		slog.Error("Config file gnidump.yaml not found", "error", err)
		os.Exit(1)
	}
	getOpts()
}

// getOpts imports data from the configuration file. Some of the settings can
// be overriden by command line flags.
func getOpts() []config.Option {
	cfg := cfgData{}
	err := viper.Unmarshal(&cfg)
	if err != nil {
		slog.Error("Cannot unmarshal config file", "error", err)
	}

	if cfg.InputDir != "" {
		opts = append(opts, config.OptInputDir(cfg.InputDir))
	}
	if cfg.JobsNum != 0 {
		opts = append(opts, config.OptJobsNum(cfg.JobsNum))
	}
	if cfg.MyHost != "" {
		opts = append(opts, config.OptMyHost(cfg.MyHost))
	}
	if cfg.MyUser != "" {
		opts = append(opts, config.OptMyUser(cfg.MyUser))
	}
	if cfg.MyPass != "" {
		opts = append(opts, config.OptMyPass(cfg.MyPass))
	}
	if cfg.MyDB != "" {
		opts = append(opts, config.OptMyDB(cfg.MyDB))
	}
	if cfg.PgHost != "" {
		opts = append(opts, config.OptPgHost(cfg.PgHost))
	}
	if cfg.PgUser != "" {
		opts = append(opts, config.OptPgUser(cfg.PgUser))
	}
	if cfg.PgPass != "" {
		opts = append(opts, config.OptPgPass(cfg.PgPass))
	}
	if cfg.PgDB != "" {
		opts = append(opts, config.OptPgDB(cfg.PgDB))
	}
	return opts
}

// touchConfigFile checks if config file exists, and if not, it gets created.
func touchConfigFile(configPath string) {
	fileExists, _ := gnsys.FileExists(configPath)
	if fileExists {
		return
	}

	slog.Info("Creating config file", "path", configPath)
	createConfig(configPath)
}

// createConfig creates config file.
func createConfig(path string) {
	err := gnsys.MakeDir(filepath.Dir(path))
	if err != nil {
		slog.Error("Cannot create config dir", "error", err)
		os.Exit(1)
	}

	err = os.WriteFile(path, []byte(configText), 0644)
	if err != nil {
		slog.Error("Cannot write to config file", "error", err)
		os.Exit(1)
	}
}
