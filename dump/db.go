package dump

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
)

type MyDB struct {
	MyHost string
	MyUser string
	MyPass string
	MyDB   string
}

func (d MyDB) NewDb() *sql.DB {
	db, err := sql.Open("mysql", d.opts())
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func (d MyDB) opts() string {
	url := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		d.MyUser, d.MyPass, d.MyHost, 3306, d.MyDB)
	return url
}
