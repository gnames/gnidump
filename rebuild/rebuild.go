package rebuild

import (
	"path/filepath"

	uuid "github.com/satori/go.uuid"
)

var gnNameSpace = uuid.NewV5(uuid.NamespaceDNS, "globalnames.org")

type Rebuild struct {
	PgDB
	DumpDir   string
	KeyValDir string
	JobsNum   int
	Batch     int
}

func NewRebuild(pgDB PgDB, inputDir string, jobsNum int) Rebuild {
	dumpDir := filepath.Join(inputDir, "gni-dump")
	keyValDir := filepath.Join(inputDir, "keyval")
	rb := Rebuild{PgDB: pgDB, DumpDir: dumpDir, KeyValDir: keyValDir,
		JobsNum: jobsNum, Batch: 50_000}
	return rb
}
