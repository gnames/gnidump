package buildio

import (
	"log/slog"
	"os"

	"github.com/gnames/gnidump/internal/ent/build"
	"github.com/gnames/gnidump/internal/ent/kv"
	"github.com/gnames/gnidump/internal/io/modelio"
	"github.com/gnames/gnidump/pkg/config"
)

// buildio is a struct that implements build.Builder interface.
type buildio struct {
	cfg    config.Config
	kvSci  kv.KeyVal
	kvVern kv.KeyVal
}

// New returns a new instance of Builder
func New(
	cfg config.Config,
	kvSci, kvVern kv.KeyVal) build.Builder {
	res := buildio{
		cfg:    cfg,
		kvSci:  kvSci,
		kvVern: kvVern,
	}
	res.resetDB()
	res.migrate()
	return res
}

// Build creates a new PostgreSQL database from CSV dump files.
func (b buildio) Build() error {
	b.importNameStrings()
	b.importDataSources()
	b.importNameIndices()

	b.importVern()
	b.importVernIndices()

	b.removeOrphans()
	b.createWords()
	b.createVerification()
	return nil
}

func (b *buildio) migrate() {
	grm := gormConn(b.cfg)
	defer grm.Close()

	slog.Info("Running initial database migrations")
	m := modelio.New(grm)
	err := m.Migrate()
	if err != nil {
		slog.Error("Cannot migrate database", "error", err)
		os.Exit(1)
	}
	slog.Info("Database migrations completed")
}
