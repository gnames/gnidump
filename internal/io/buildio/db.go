package buildio

import (
	"fmt"
	"log/slog"
	"os"
)

// resetDB resets the database to a clean state.
func (b *buildio) resetDB() {
	slog.Info("Resetting database")
	q := `
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO %s;
COMMENT ON SCHEMA public IS 'standard public schema'`
	q = fmt.Sprintf(q, b.cfg.PgUser)
	_, err := b.db.Query(q)
	if err != nil {
		slog.Error("Cannot reset database", "error", err)
		os.Exit(1)
	}

	slog.Info("Database did reset successfully")
}
