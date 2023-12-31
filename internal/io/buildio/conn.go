package buildio

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/gnames/gnidump/pkg/config"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
)

func pgxConn(cfg config.Config) (*pgxpool.Pool, error) {
	pgxCfg, err := pgxpool.ParseConfig(opts(cfg))
	if err != nil {
		slog.Error("Cannot parse pgx config", "error", err)
		return nil, err
	}
	pgxCfg.MaxConns = 15

	db, err := pgxpool.NewWithConfig(
		context.Background(),
		pgxCfg,
	)
	if err != nil {
		slog.Error("Cannot connect to database", "error", err)
		return nil, err
	}
	return db, nil
}

func gormConn(cfg config.Config) (*gorm.DB, error) {
	db, err := gorm.Open("postgres", opts(cfg))
	if err != nil {
		slog.Error("Cannot connect to database", "error", err)
		return nil, err
	}
	return db, nil
}

func opts(cfg config.Config) string {
	return fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable",
		cfg.PgHost, cfg.PgUser, cfg.PgPass, cfg.PgDB)
}
