package gnidump

import (
	"github.com/gnames/gnidump/internal/ent/build"
	"github.com/gnames/gnidump/internal/ent/dump"
	"github.com/gnames/gnidump/pkg/config"
)

// gnidump is an implementation of GNIdump interface.
type gnidump struct {
	cfg config.Config
}

// New creates a new instance of GNIdump.
func New(
	cfg config.Config,
) GNIdump {
	res := gnidump{
		cfg: cfg}
	return &res
}

// Dump dumps data from MySQL to CSV files.
func (g *gnidump) Dump(d dump.Dumper) error {
	return d.Dump()
}

// Build builds GNI database from CSV files to PostgreSQL.
func (g *gnidump) Build(b build.Builder) error {
	return b.Build()
}
