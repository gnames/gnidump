package gnidump

import (
	"github.com/gnames/gnidump/internal/ent/build"
	"github.com/gnames/gnidump/internal/ent/dump"
)

// GNIdump is an interface for dumping and building GNI data.
type GNIdump interface {
	// Dump dumps data from MySQL to CSV files.
	Dump(dump.Dumper) error

	// Build builds GNI database from CSV files to PostgreSQL.
	Build(build.Builder) error
}
