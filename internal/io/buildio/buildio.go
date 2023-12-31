package buildio

import (
	"log/slog"

	"github.com/gnames/gnidump/internal/ent/build"
	"github.com/gnames/gnidump/internal/ent/kv"
	"github.com/gnames/gnidump/internal/io/modelio"
	"github.com/gnames/gnidump/pkg/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

// buildio is a struct that implements build.Builder interface.
type buildio struct {
	db     *pgxpool.Pool
	cfg    config.Config
	kvSci  kv.KeyVal
	kvVern kv.KeyVal
}

// New returns a new instance of Builder
func New(
	cfg config.Config,
	kvSci, kvVern kv.KeyVal) (build.Builder, error) {
	var err error
	var db *pgxpool.Pool
	res := buildio{
		cfg:    cfg,
		kvSci:  kvSci,
		kvVern: kvVern,
	}
	db, err = pgxConn(cfg)
	if err != nil {
		slog.Error("Cannot connect to database", "error", err)
		return nil, err
	}
	res.db = db
	err = res.resetDB()
	if err != nil {
		slog.Error("Cannot reset database", "error", err)
		return nil, err
	}
	err = res.migrate()
	if err != nil {
		slog.Error("Cannot migrate database", "error", err)
		return nil, err
	}
	return &res, nil
}

// Build reads CSV dump files and imports their data to Postgres DB.
func (b *buildio) Build() error {
	var err error
	defer b.db.Close()
	// import scientific names data
	if err = b.importNameStrings(); err != nil {
		slog.Error("Cannot import name-strings", "error", err)
		return err
	}
	if err = b.importDataSources(); err != nil {
		slog.Error("Cannot import data-sources", "error", err)
		return err
	}
	if err = b.importNameIndices(); err != nil {
		slog.Error("Cannot import name-string-indices", "error", err)
		return err
	}

	// import vernacular data
	if err = b.importVern(); err != nil {
		slog.Error("Cannot import vernacular_strings", "error", err)
		return err
	}
	if err = b.importVernIndices(); err != nil {
		slog.Error("Cannot import vernacular_indices", "error", err)
		return err
	}

	// finish import by creating words and verification tables
	if err = b.removeOrphans(); err != nil {
		slog.Error("Cannot remove orphans", "error", err)
		return err
	}
	if err = b.createWords(); err != nil {
		slog.Error("Cannot create words", "error", err)
		return err
	}
	if err = b.createVerification(); err != nil {
		slog.Error("Cannot create verification", "error", err)
		return err
	}

	return nil
}

func (b *buildio) migrate() error {
	grm, err := gormConn(b.cfg)
	if err != nil {
		return err
	}
	defer grm.Close()

	slog.Info("Running initial database migrations")
	m := modelio.New(grm)
	err = m.Migrate()
	if err != nil {
		slog.Error("Cannot migrate database", "error", err)
		return err
	}
	slog.Info("Database migrations completed")
	return nil
}
