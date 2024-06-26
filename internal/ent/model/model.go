package model

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

	// Citation is a reference that can be used to cite the dataset.
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

	// IsOutlinkReady means that the data-source has enough metainformation,
	// URLs, harvests to be generally good to be pointed out as a 'mature'
	// data-source at gnames. Resources that are harvested too long time ago
	// or do not have WebsiteURL/OutlinkURLs would normally have this flag set
	// to false.
	IsOutlinkReady bool

	// Is true if a dataset undergoes a significant manual curation.
	IsCurated bool

	// Is true if a dataset undergoes a significant automatic curation by
	// scripts.
	IsAutoCurated bool

	// Is true if a dataset has taxon data.
	HasTaxonData bool

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
	Name string `sql:"type:CHARACTER VARYING(255) COLLATE \"C\" NOT NULL"`

	// Year is the year when a name was published
	Year sql.NullInt16 `gorm:"type:int"`
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

	// Bacteria is true if parser marks a name as from Bactrial Code.
	Bacteria bool `gorm:"type:bool;not null;default:false"`

	// Surrogate indicates if a name-string is a surrogate name.
	Surrogate bool `gorm:"type:bool"`

	// ParseQuality is numeric representation of the quality of parsing.
	// 0 - no parse, 1 - clear parse, 2 - some problems, 3 - big problems.
	ParseQuality int `gorm:"type:int;not null;default:0"`
}

// Canonical is a 'simple' canonical form.
type Canonical struct {
	// UUID v5 generated for simple canonical form.
	ID string `gorm:"type:uuid;primary_key;auto_increment:false"`

	// Canonical name-string
	Name string `sql:"type:CHARACTER VARYING(255) COLLATE \"C\"" gorm:"index:canonical_name;not null"`
}

// CanonicalFull ia a full canonical form.
type CanonicalFull struct {
	// UUID v5 generated for 'full' canonical form (with infraspecific ranks
	// and hybrid signs for named hybrids).
	ID string `gorm:"type:uuid;primary_key;auto_increment:false"`

	// Canonical name-string
	Name string `sql:"type:CHARACTER VARYING(255) COLLATE \"C\"" gorm:"not null"`
}

// CanonicalStem is a stemmed derivative of a simple canonical form.
type CanonicalStem struct {
	// UUID v5 for the stemmed derivative of a simple canonical form.
	ID string `gorm:"type:uuid;primary_key;auto_increment:false"`

	// Stemmed canonical name-string
	Name string `sql:"type:CHARACTER VARYING(255) COLLATE \"C\"" gorm:"not null"`
}

// NameStringIndex is a name-strings relations to datasets.
type NameStringIndex struct {
	// DataSourceID refers to a data-source ID.
	DataSourceID int `gorm:"primary_key;auto_increment:false"`

	// RecordID is a unique ID for record. We do our best to
	// get it from the record IDs, either global or local,
	// but if all fails, id is assigned by gnames in a format
	// of 'gn_{int}'.
	RecordID string `gorm:"type:varchar(255);primary_key;auto_increment:false"`

	// NameStringI is UUID5 of a full name-string from the dataset.
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
	Classification string `sql:"type:CHARACTER VARYING COLLATE \"C\""`

	// RecordIDs of the classificatiaon elements (if given).
	ClassificationIDs string `sql:"type:CHARACTER VARYING COLLATE \"C\""`

	// Ranks of the classification elements.
	ClassificationRanks string
}

// Word is a word from a name-string.
type Word struct {
	// ID generated by combinding modified word and type converted to integer
	//together with a pipe, and generating UUID5 from it.
	//For example: "alb|2"
	ID string `gorm:"primary_key;type:uuid;auto_increment:false"`

	// Normalized is the word normalized by GNparser. This field is used
	// for sorting results.
	Normalized string `sql:"type:CHARACTER VARYING(255) COLLATE \"C\"" gorm:"primary_key;auto_increment:false"`

	// Modified is a heavy-normalized word. This field is used for matching.
	Modified string `sql:"type:CHARACTER VARYING(255) COLLATE \"C\"" gorm:"not null;index:words_modified"`

	// TypeID is the integer representation of parsed.WordType
	// from GNparser.
	TypeID int
}

// WordNameString is the meaning of a word in a name-string.
type WordNameString struct {
	// WordID is the identifier of a word.
	WordID string `gorm:"primary_key;type:uuid;auto_increment:false"`

	// NameStringID is UUID5 of a full name-string from the dataset.
	NameStringID string `gorm:"primary_key;type:uuid;auto_increment:false"`

	// CanonicalID is UUID5 of a simple canonical form of a name
	CanonicalID string `gorm:"type:uuid;not_null"`
}

// VernacularString contains vernacular name-strings.
type VernacularString struct {
	// UUID v5 generated from the name-string using DNS:"globalnames.org" as
	// a seed.
	ID string `gorm:"type:uuid;primary_key;auto_increment:false"`

	// Name is a vernacular name as it is given by a dataset.
	Name string `sql:"type:CHARACTER VARYING(255) COLLATE \"C\" NOT NULL"`
}

type VernacularStringIndex struct {
	// DataSourceID refers to a data-source ID.
	DataSourceID int `gorm:"primary_key;auto_increment:false"`

	// RecordID is a unique ID for record. We do our best to
	// get it from the record IDs, either global or local,
	// but if all fails, id is assigned by gnames in a format
	// of 'gn_{int}'.
	RecordID string `gorm:"type:varchar(255);primary_key;auto_increment:false"`

	// VernacularStringID is UUID5 of a full name-string from the dataset.
	VernacularStringID string `gorm:"type:uuid;index:vernacular_string_id;primary_key;auto_increment:false"`

	// Language of the vernacular name.
	Language string `gorm:"type:varchar(100)"`

	// LangCode is a three-letter code of the language. The code
	// is received programmatically and might contain errors.
	LangCode string `gorm:"type:varchar(3);index:lang_code"`

	// Locality of the vernacular name.
	Locality string `gorm:"type:varchar(100)"`

	// CountryCode of the vernacular name.
	CountryCode string `gorm:"type:varchar(50)"`
}
