package gnidump

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/gnames/gnidump/dump"
	"github.com/gnames/gnidump/rebuild"
	"github.com/mitchellh/go-homedir"
)

// GNIdump is an coordinator of all gnidump functionality. It contains complete
// set of configuration variable necessary for dump and restore of the data.
type GNIdump struct {
	// MyDB is needed for dumping data from previous version of GNI.
	dump.MyDB
	// PyDB is needed for loading data to PostgreSQL DB.
	rebuild.PgDB
	// InputDir contains dump data and key-values stores.
	InputDir string
	// JobsNum is a number of goroutines for parallel executions.
	JobsNum int
}

// NewGNIdump creates GNIdump instance.
func NewGNIdump(opts ...Option) GNIdump {
	gnd := GNIdump{JobsNum: 1}
	for _, opt := range opts {
		opt(&gnd)
	}
	if strings.HasPrefix(gnd.InputDir, "~/") ||
		strings.HasPrefix(gnd.InputDir, "~\\") {
		home, err := homedir.Dir()
		if err != nil {
			slog.Error("Cannot find home directory", "error", err)
			os.Exit(1)
		}
		gnd.InputDir = filepath.Join(home, gnd.InputDir[2:])
	}
	return gnd
}

// Option type allows to change settings for GNIdump.
type Option func(*GNIdump)

// OptInputDir sets a directory for temporary files and key-value stores.
func OptInputDir(d string) Option {
	return func(gnd *GNIdump) {
		gnd.InputDir = d
	}
}

// OptJobsNum sets parallelism number for concurrent goroutines.
func OptJobsNum(j int) Option {
	return func(gnd *GNIdump) {
		gnd.JobsNum = j
	}
}

// OptMyHost sets host for MySQL
func OptMyHost(h string) Option {
	return func(gnd *GNIdump) {
		gnd.MyHost = h
	}
}

// OptMyUser sets user for MySQL
func OptMyUser(u string) Option {
	return func(gnd *GNIdump) {
		gnd.MyUser = u
	}
}

// OptMyPass sets password for MySQL
func OptMyPass(p string) Option {
	return func(gnd *GNIdump) {
		gnd.MyPass = p
	}
}

// OptMyDB sets database name for MySQL
func OptMyDB(d string) Option {
	return func(gnd *GNIdump) {
		gnd.MyDB.MyDB = d
	}
}

// OptPgHost sets host name for PostgreSQL
func OptPgHost(h string) Option {
	return func(gnd *GNIdump) {
		gnd.PgHost = h
	}
}

// OptPgUser sets user for PostgreSQL
func OptPgUser(u string) Option {
	return func(gnd *GNIdump) {
		gnd.PgUser = u
	}
}

// OptPgPass sets password for PostgreSQL
func OptPgPass(p string) Option {
	return func(gnd *GNIdump) {
		gnd.PgPass = p
	}
}

// OptPgDB sets database name for PostgreSQL
func OptPgDB(d string) Option {
	return func(gnd *GNIdump) {
		gnd.PgDB.PgDB = d
	}
}
