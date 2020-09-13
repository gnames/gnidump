package rebuild

import (
	"database/sql"
	"time"
)

// DataSource describes metadata of a dataset.
type DataSource struct {
	// Hard-coded ID that corresponds to historic IDs given by old versions
	// of resolver.
	ID int `gorm:"type:smallint;primary_key;auto_increment:false"`
	// UUID assigned to the resource during creation. UUID is not displayed to
	// users, but is important for data import from DwCA files.
	UUID string `gorm:"type:uuid;default:'00000000-0000-0000-0000-000000000000'"`
	// Long title tries to follow the name of dataset given by its creators.
	Title string `gorm:"type:varchar(255)"`
	// Shortened/Abbreviated title.
	TitleShort string `gorm:"type:varchar(50)"`
	// Some datasets have versions.
	Version string `gorm:"type:varchar(50)"`
	// Time when the dataset was created.
	// Follows a format of a 'YYYY-MM-DD' || 'YYYY-MM' || 'YYYY'.
	RevisionDate string
	// DOI of the dataset (if exists).
	DOI string `gorm:"type:varchar(50)"`
	// A reference that can be used to cite the dataset.
	Citation string
	// Authors of the dataset.
	Authors string
	// Description of the dataset. Might include unstructured metainformation
	// as well.
	Description string
	// Home URL for the dataset.
	WebsiteURL string `gorm:"type:varchar(255)"`
	// Original url used to download the dataset.
	DataURL string `gorm:"type:varchar(255)"`
	// A template for creation of an outlink for a dataset record. It contains
	// a placeholder '{}' for the record's OutlinkID.
	OutlinkURL string
	// Is true if a dataset undergoes a significant manual curation.
	IsCurated bool
	// Is true if a dataset undergoes a significant automatic curation by
	// scripts.
	IsAutoCurated bool
	// Number of records in a dataset.
	RecordCount int
	// Timestamp when the dataset was imported last time. The timeset usually
	// does not corresponds to when the dataset was created.
	UpdatedAt time.Time `gorm:"type:timestamp without time zone"`
}

// NameString is a name-string extracted from a dataset.
type NameString struct {
	// UUID v5 generated from the name-string using DNS:"globalnames.org" as
	// a seed.
	ID string `gorm:"type:uuid;primary_key;auto_increment:false"`
	// Name-string with authorships and annotations as it is given by a dataset.
	// Sometimes an authorship is concatenated with a name-string by our
	// import scripts.
	Name string `gorm:"type:varchar(255);not_null"`
	// Number of elements in a 'classic' Linnaen name: 0 - unknown, not available,
	// 1 - uninomial, 2 - binomial, 3 - trinomial etc.
	// Cardinality can be used to filter out surrogates and hybrid formulas --
	// they would have cardinality 0.
	Cardinality sql.NullInt32 `gorm:"type:int"`
	// UUID v5 generated for simple canonical form.
	CanonicalID sql.NullString `gorm:"type:uuid;index:canonical"`
	// UUID v5 generated for 'full' canonical form (with infraspecific ranks
	// and hybrid signs for named hybrids).
	CanonicalFullID sql.NullString `gorm:"type:uuid;index:canonical_full"`
	// UUID v5 for the stemmed derivative of a simple canonical form.
	CanonicalStemID sql.NullString `gorm:"type:uuid;index:canonical_stem"`
	// Virus indicates if a name-string seems to be virus-like.
	Virus bool `gorm:"type:bool"`
	// Surrogate indicates if a name-string is a surrogate name.
	Surrogate bool `gorm:"type:bool"`
}

// Canonical is a 'simple' canonical form.
type Canonical struct {
	// UUID v5 generated for simple canonical form.
	ID string `gorm:"type:uuid;primary_key;auto_increment:false"`
	// Canonical name-string
	Name string `gorm:"type:varchar(255);not_null"`
	// NameStem is a stemmed version of the canonical form.
	NameStem string `gorm:"type:varchar(255);not_null;index:canonical_canonical_stem"`
}

// CanonicalFull ia a full canonical form.
type CanonicalFull struct {
	// UUID v5 generated for 'full' canonical form (with infraspecific ranks
	// and hybrid signs for named hybrids).
	ID string `gorm:"type:uuid;primary_key;auto_increment:false"`
	// Canonical name-string
	Name string `gorm:"type:varchar(255);not_null"`
}

// CanonicalStem is a stemmed derivative of a simple canonical form.
type CanonicalStem struct {
	// UUID v5 for the stemmed derivative of a simple canonical form.
	ID string `gorm:"type:uuid;primary_key;auto_increment:false"`
	// Stemmed canonical name-string
	Name string `gorm:"type:varchar(255);not_null"`
}

// NameStringIndex is a name-strings relations to datasets.
type NameStringIndex struct {
	// Dataset ID
	DataSourceID int `gorm:"primary_key;auto_increment:false"`
	// Unique ID for record. We do our best to get it from the record IDs, either
	// global or local, but if all fails, id is assigned by gnames in a format of
	// 'gn_{int}'.
	RecordID string `gorm:"type:varchar(255);primary_key;auto_increment:false"`
	// The UUID5 of a full name-string from the dataset.
	NameStringID string `gorm:"type:uuid;index:name_string_id;primary_key;auto_increment:false"`
	// The id to create an outlink.
	OutlinkID string `gorm:"type:varchar(255)"`
	// Global id from the dataset.
	GlobalID string `gorm:"type:varchar(255)"`
	// Local id from the dataset.
	LocalID string `gorm:"type:varchar(255)"`
	// Nomenclatural code ID. 0 - no info, 1 - ICZN, 2 - ICN, 3 - ICNP, 4 - ICTV.
	CodeID int `gorm:"type:smallint"`
	// The rank of the name.
	Rank string `gorm:"type:varchar(255)"`
	// RecordID of a currently accepted name-string for the taxon.
	AcceptedRecordID string `gorm:"type:varchar(255);index:accepted_record_id"`
	// Pipe-delimited string containing classification supplied with the resource.
	Classification string
	// RecordIDs of the classificatiaon elements (if given).
	ClassificationIDs string
	// Ranks of the classification elements.
	ClassificationRanks string
}
