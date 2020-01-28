package rebuild

import (
	"database/sql"
	"time"
)

type DataSource struct {
	ID            int    `gorm:"type:smallint;primary_key;auto_increment:false"`
	Title         string `gorm:"type:varchar(255)"`
	Description   string
	WebsiteURL    string `gorm:"type:varchar(255)"`
	DataURL       string `gorm:"type:varchar(255)"`
	IsCurated     bool
	IsAutoCurated bool
	RecordCount   int
	UpdatedAt     time.Time `gorm:"type:timestamp without time zone"`
}

type NameString struct {
	ID              string         `gorm:"type:uuid;primary_key;auto_increment:false"`
	Name            string         `gorm:"type:varchar(255);not_null"`
	Cardinality     sql.NullInt32  `gorm:"type:int"`
	CanonicalID     sql.NullString `gorm:"type:uuid;index:canonical"`
	CanonicalFullID sql.NullString `gorm:"type:uuid;index:canonical_full"`
	CanonicalStemID sql.NullString `gorm:"type:uuid;index:canonical_stem"`
}

type Canonical struct {
	ID   string `gorm:"type:uuid;primary_key;auto_increment:false"`
	Name string `gorm:"type:varchar(255);not_null"`
}

type CanonicalFull struct {
	ID   string `gorm:"type:uuid;primary_key;auto_increment:false"`
	Name string `gorm:"type:varchar(255);not_null"`
}

type CanonicalStem struct {
	ID   string `gorm:"type:uuid;primary_key;auto_increment:false"`
	Name string `gorm:"type:varchar(255);not_null"`
}

type NameStringIndex struct {
	DataSourceID int    `gorm:"primary_key;auto_increment:false"`
	TaxonID      string `gorm:"type:varchar(255);primary_key;auto_increment:false"`
	NameStringID string `gorm:"type:uuid;index:name_string_id;primary_key;auto_increment:false"`
	TaxonIDReal  bool
	GlobalID     string `gorm:"type:varchar(255)"`
	// TODO: CodeID should be nullable
	CodeID              int    `gorm:"type:smallint"`
	Rank                string `gorm:"type:varchar(255)"`
	AcceptedTaxonID     string `gorm:"type:varchar(255);index:accepted_taxon_id"`
	Classification      string
	ClassificationIDs   string
	ClassificationRanks string
}
