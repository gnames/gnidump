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

func (pdb PgDB) NewDbGorm() *gorm.DB {
	db, err := gorm.Open("postgres", pdb.opts())
	if err != nil {
		log.Fatal(err)
	}
	return db
}

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
		log.Println("ResetDB")
		return err
	}
	return db.Close()
}

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

func QuoteString(s string) string {
	return "'" + strings.Replace(s, "'", "''", -1) + "'"
}
