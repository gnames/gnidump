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
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/gnames/gnidump"
	"github.com/gnames/gnidump/sys"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const configText = `---

# Path to keep downloaded data and key-value stores
InputDir: /tmp/gnidump

# MySQL host
MyHost: localhost

# MySQL user
MyUser: root

# MySQL password
MyPass:

# MySQL database
MyDB: gni

# Postgresql host
PgHost: localhost

# Postgresql user
PgUser: postgres

# Postgresql password
PgPass:

# Postgresql database
PgDB: gnames

# Number of jobs for parallel tasks
JobsNum: 4
`

var (
	cfgFile string
	opts    []gnidump.Option
)

// config purpose is to achieve automatic import of data from the
// configuration file, if it exists.
type config struct {
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
			log.Println(err)
			os.Exit(1)
		}
		if version {
			fmt.Printf("\nversion: %s\nbuild: %s\n\n", gnidump.Version, gnidump.Build)
			os.Exit(0)
		}

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

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.gnidump.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	rootCmd.Flags().BoolP("version", "V", false, "Returns version and build date")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	var err error
	var home string
	configFile := "gnidump"

	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err = homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		home = filepath.Join(home, ".config")

		// Search config in home directory with name ".gnidump" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(configFile)
	}

	viper.AutomaticEnv() // read in environment variables that match

	configPath := filepath.Join(home, fmt.Sprintf("%s.yaml", configFile))
	touchConfigFile(configPath, configFile)

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		//log.Println("Using config file:", viper.ConfigFileUsed())
	} else {
		fmt.Println("Config file $HOME/.gnidump.yaml not found")
		os.Exit(1)
	}
	getOpts()
}

// getOpts imports data from the configuration file. Some of the settings can
// be overriden by command line flags.
func getOpts() []gnidump.Option {
	cfg := &config{}
	err := viper.Unmarshal(cfg)
	if err != nil {
		log.Fatal(err)
	}

	if cfg.InputDir != "" {
		opts = append(opts, gnidump.OptInputDir(cfg.InputDir))
	}
	if cfg.JobsNum != 0 {
		opts = append(opts, gnidump.OptJobsNum(cfg.JobsNum))
	}
	if cfg.MyHost != "" {
		opts = append(opts, gnidump.OptMyHost(cfg.MyHost))
	}
	if cfg.MyUser != "" {
		opts = append(opts, gnidump.OptMyUser(cfg.MyUser))
	}
	if cfg.MyPass != "" {
		opts = append(opts, gnidump.OptMyPass(cfg.MyPass))
	}
	if cfg.MyDB != "" {
		opts = append(opts, gnidump.OptMyDB(cfg.MyDB))
	}
	if cfg.PgHost != "" {
		opts = append(opts, gnidump.OptPgHost(cfg.PgHost))
	}
	if cfg.PgUser != "" {
		opts = append(opts, gnidump.OptPgUser(cfg.PgUser))
	}
	if cfg.PgPass != "" {
		opts = append(opts, gnidump.OptPgPass(cfg.PgPass))
	}
	if cfg.PgDB != "" {
		opts = append(opts, gnidump.OptPgDB(cfg.PgDB))
	}
	return opts
}

// touchConfigFile checks if config file exists, and if not, it gets created.
func touchConfigFile(configPath string, configFile string) {
	if sys.FileExists(configPath) {
		return
	}

	log.Println("Creating config file:", configPath)
	createConfig(configPath, configFile)
}

// createConfig creates config file.
func createConfig(path string, file string) {
	err := sys.MakeDir(filepath.Dir(path))
	if err != nil {
		log.Fatal(err)
	}

	err = ioutil.WriteFile(path, []byte(configText), 0644)
	if err != nil {
		log.Fatal(err)
	}
}
