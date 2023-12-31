package dumpio

import (
	"database/sql"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/gnames/gnidump/internal/ent/dump"
	"github.com/gnames/gnidump/pkg/config"
	"github.com/gnames/gnsys"

	_ "github.com/go-sql-driver/mysql"
)

type dumpio struct {
	cfg config.Config
	db  *sql.DB
}

func New(cfg config.Config) (dump.Dumper, error) {
	var err error
	res := dumpio{cfg: cfg}
	res.db, err = res.initDb()
	if err != nil {
		return nil, err
	}

	err = gnsys.MakeDir(res.cfg.DumpDir)
	if err != nil {
		slog.Error("Cannot create dump directory", "error", err)
		return nil, err
	}

	return &res, nil
}

func (d *dumpio) Dump() error {

	slog.Info("Dumping data from GNI to CSV files.")

	err := d.updateDataSourcesDate()
	if err != nil {
		return err
	}
	err = d.dumpTableDataSources()
	if err != nil {
		return err
	}
	err = d.dumpTableNameStrings()
	if err != nil {
		return err
	}
	err = d.dumpTableNameStringIndices()
	if err != nil {
		return err
	}
	err = d.dumpTableVernacularStrings()
	if err != nil {
		return err
	}
	err = d.dumpTableVernacularStringIndices()
	if err != nil {
		return err
	}

	slog.Info("CSV dump is created")
	return d.db.Close()
}

func (d *dumpio) csvFile(f string) (*os.File, error) {
	path := filepath.Join(d.cfg.DumpDir, f+".csv")
	return os.Create(path)
}
