package modelio

import (
	"github.com/gnames/gnidump/pkg/ent/model"
	"github.com/jinzhu/gorm"
)

type modelio struct {
	db *gorm.DB
}

// New returns a new instance of Model
func New(db *gorm.DB) model.Model {
	res := modelio{db: db}
	return &res
}

// Migrate creates tables in the database.
func (m *modelio) Migrate() error {
	m.db.AutoMigrate(
		&model.DataSource{},
		&model.NameString{},
		&model.Canonical{},
		&model.CanonicalFull{},
		&model.CanonicalStem{},
		&model.NameStringIndex{},
		&model.Word{},
		&model.WordNameString{},
		&model.VernacularString{},
		&model.VernacularStringIndex{},
	)
	if m.db.Error != nil {
		return m.db.Error
	}

	return nil
}
