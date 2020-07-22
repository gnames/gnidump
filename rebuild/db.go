package rebuild

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
)

type PgDB struct {
	PgHost string
	PgUser string
	PgPass string
	PgDB   string
}

// NewDbGorm creates a database handler from GORM library. We use it to simplify
// migrations process.
func (pdb PgDB) NewDbGorm() *gorm.DB {
	db, err := gorm.Open("postgres", pdb.opts())
	if err != nil {
		log.Fatal(err)
	}
	return db
}

// NewDb creates a database handler from sandard sql package. We use it to
// speed up import of the data.
func (pdb PgDB) NewDb() *sql.DB {
	db, err := sql.Open("postgres", pdb.opts())
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func (pdb PgDB) opts() string {
	return fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable",
		pdb.PgHost, pdb.PgUser, pdb.PgPass, pdb.PgDB)
}

// ResetDB deletes old database and its public schema and sets up a new schema
// with correct owner.
func (pdb PgDB) ResetDB() error {
	db := pdb.NewDb()
	q := `
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO %s;
COMMENT ON SCHEMA public IS 'standard public schema'`
	q = fmt.Sprintf(q, pdb.PgUser)
	_, err := db.Query(q)
	if err != nil {
		return fmt.Errorf("database reset did not work: %w", err)
	}
	return db.Close()
}

// Migrate creates all the tables and indices in the database.
func (pdb PgDB) Migrate() error {
	log.Printf("Running initial database '%s' migrations.\n", pdb.PgDB)
	db := pdb.NewDbGorm()
	db.AutoMigrate(
		&DataSource{},
		&NameString{},
		&Canonical{},
		&CanonicalFull{},
		&CanonicalStem{},
		&NameStringIndex{},
	)
	if db.Error != nil {
		return db.Error
	}
	return db.Close()
}

// QuoteString makes a string value compatible with SQL synthax by wrapping it
// in quotes and escaping internal quotes.
func QuoteString(s string) string {
	return "'" + strings.Replace(s, "'", "''", -1) + "'"
}
