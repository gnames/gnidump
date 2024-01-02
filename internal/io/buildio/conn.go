package buildio

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"

	"github.com/gnames/gnidump/pkg/config"
	"github.com/jinzhu/gorm"
	_ "github.com/lib/pq"
)

func pgConn(cfg config.Config) *sql.DB {
	db, err := sql.Open("postgres", opts(cfg))
	if err != nil {
		slog.Error("Cannot connect Gorm to database", "error", err)
		os.Exit(1)
	}
	return db
}

func gormConn(cfg config.Config) *gorm.DB {
	db, err := gorm.Open("postgres", opts(cfg))
	if err != nil {
		slog.Error("Cannot connect to database", "error", err)
		os.Exit(1)
	}
	return db
}

func opts(cfg config.Config) string {
	return fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable",
		cfg.PgHost, cfg.PgUser, cfg.PgPass, cfg.PgDB)
}

func (b *buildio) truncateTable(tbl string) {
	db := pgConn(b.cfg)
	defer db.Close()

	_, err := db.Exec("TRUNCATE TABLE " + tbl)
	if err != nil {
		slog.Error("cannot truncate table", "table", tbl, "error", err)
		os.Exit(1)
	}
}
