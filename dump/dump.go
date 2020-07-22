package dump

import (
	"database/sql"
	"path/filepath"
)

// Dump contains data needed for dumping names data into CSV files.
type Dump struct {
	InputDir string
	DumpDir  string
	JobsNum  int
	DB       *sql.DB
}

// NewDump is a factory for Dump.
func NewDump(d MyDB, inputDir string, jobs int) Dump {
	dumpDir := filepath.Join(inputDir, "gni-dump")
	dmp := Dump{InputDir: inputDir, DumpDir: dumpDir, JobsNum: jobs}
	dmp.DB = d.NewDb()
	return dmp
}
