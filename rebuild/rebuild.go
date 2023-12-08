package rebuild

import (
	"path/filepath"
)

// Rebuild provides configuration for database rebuilding process
type Rebuild struct {
	PgDB
	DumpDir         string
	ParserKeyValDir string
	VernKeyValDir   string
	JobsNum         int
	Batch           int
}

// NewRebuild creates new Rebuild structure for rebuilding process.
func NewRebuild(pgDB PgDB, inputDir string, jobsNum int) Rebuild {
	dumpDir := filepath.Join(inputDir, "gni-dump")
	parserKVDir := filepath.Join(inputDir, "parser")
	vernKVDir := filepath.Join(inputDir, "vern")

	rb := Rebuild{
		PgDB:            pgDB,
		DumpDir:         dumpDir,
		ParserKeyValDir: parserKVDir,
		VernKeyValDir:   vernKVDir,
		JobsNum:         jobsNum,
		Batch:           50_000,
	}
	return rb
}
