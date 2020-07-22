package rebuild

import (
	"path/filepath"

	uuid "github.com/satori/go.uuid"
)

var gnNameSpace = uuid.NewV5(uuid.NamespaceDNS, "globalnames.org")

// Rebuild provides configuration for database rebuilding process
type Rebuild struct {
	PgDB
	DumpDir         string
	ParserKeyValDir string
	JobsNum         int
	Batch           int
}

// NewRebuild creates new Rebuild structure for rebuilding process.
func NewRebuild(pgDB PgDB, inputDir string, jobsNum int) Rebuild {
	dumpDir := filepath.Join(inputDir, "gni-dump")
	parserKVDir := filepath.Join(inputDir, "parser")
	rb := Rebuild{PgDB: pgDB, DumpDir: dumpDir, ParserKeyValDir: parserKVDir,
		JobsNum: jobsNum, Batch: 50_000}
	return rb
}
