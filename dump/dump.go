package dump

import (
	"database/sql"
	"path/filepath"
)

type Dump struct {
	InputDir string
	DumpDir  string
	JobsNum  int
	DB       *sql.DB
}

func NewDump(d MyDB, inputDir string, jobs int) Dump {
	dumpDir := filepath.Join(inputDir, "gni-dump")
	dmp := Dump{InputDir: inputDir, DumpDir: dumpDir, JobsNum: jobs}
	dmp.DB = d.NewDb()
	return dmp
}
