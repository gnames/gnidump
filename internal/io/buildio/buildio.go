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
	return &res
}

// Build reads CSV dump files and imports their data to Postgres DB.
func (b *buildio) Build() error {
	var err error
	if err = b.importNameStrings(); err != nil {
		slog.Error("Cannot import name-strings", "error", err)
		return err
	}
	// b.importNameIndices()
	// b.importDataSources()

	if err = b.importVern(); err != nil {
		slog.Error("Cannot import vernacular_strings", "error", err)
		return err
	}
	if err = b.importVernIndices(); err != nil {
		slog.Error("Cannot import vernacular_indices", "error", err)
		return err
	}

	// b.removeOrphans()
	// b.createWords()
	// b.createVerification()
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
