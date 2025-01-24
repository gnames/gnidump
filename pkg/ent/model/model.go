package model

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type NameID interface {
	StringID() string
	StringName() string
}

// DataSource stores metadata associated with a dataset.
type DataSource struct {
	// ID is a hard-coded identifier that aligns with historical IDs from
	// older versions of the resolver.
	ID int `gorm:"type:smallint;primary_key;auto_increment:false"`

	// UUID is a unique identifier assigned to the resource upon creation.
	// While not shown to users, it's handy for importing data from DwCA files.
	UUID string `gorm:"type:uuid;default:'00000000-0000-0000-0000-000000000000'"`

	// Title is the full, descriptive title of the dataset, usually as provided
	// by its creators.
	Title string `gorm:"type:varchar(255)"`

	// TitleShort is a concise or abbreviated version of the dataset title.
	TitleShort string `gorm:"type:varchar(50)"`

	// Version denotes the specific version of the dataset, if applicable.
	Version string `gorm:"type:varchar(50)"`

	// RevisionDate indicates when the dataset was created or last revised.
	// It follows the format 'YYYY-MM-DD', 'YYYY-MM', or 'YYYY'.
	RevisionDate string

	// DOI is the Digital Object Identifier for the dataset, if available.
	DOI string `gorm:"type:varchar(50)"`

	// Citation provides the recommended way to reference or cite the dataset.
	Citation string

	// Authors lists the individuals or organizations responsible for creating
	// the dataset.
	Authors string

	// Description offers a summary of the dataset's content and purpose.
	// It may also include additional, unstructured metadata.
	Description string

	// WebsiteURL is the primary web address associated with the dataset.
	WebsiteURL string `gorm:"type:varchar(255)"`

	// DataURL is the original URL from which the dataset was downloaded.
	DataURL string `gorm:"type:varchar(255)"`

	// OutlinkURL is a template for generating external links to specific records
	// within the dataset. It includes a placeholder '{}' for the record's
	// OutlinkID.
	OutlinkURL string

	// IsOutlinkReady signifies whether the data source has sufficient metadata,
	// URLs, and harvests to be considered a 'mature' and reliable source on
	// gnames. Resources with outdated harvests or missing WebsiteURL/OutlinkURLs
	// will typically have this flag set to false.
	IsOutlinkReady bool

	// IsCurated is true when we are aware if the dataset has undergone
	// significant manual curation.
	IsCurated bool

	// IsAutoCurated is true when we are aware if the dataset has undergone
	// significant automated curation using scripts.
	IsAutoCurated bool

	// HasTaxonData indicates whether the dataset contains taxonomic data.
	HasTaxonData bool

	// RecordCount represents the total number of name records in the dataset.
	RecordCount int

	// VernRecordCount is the number of vernacular string indices.
	VernRecordCount int

	// UpdatedAt records the timestamp of the dataset's last import.
	// This might not necessarily coincide with the dataset's creation date.
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
	Name string `gorm:"type:varchar(255);not null"`

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
	Name string `gorm:"type:varchar(255);not null"`
}

func (c Canonical) StringID() string   { return c.ID }
func (c Canonical) StringName() string { return c.Name }

// CanonicalFull ia a full canonical form.
type CanonicalFull struct {
	// UUID v5 generated for 'full' canonical form (with infraspecific ranks
	// and hybrid signs for named hybrids).
	ID   string `gorm:"type:uuid;primary_key;auto_increment:false"`
	Name string `gorm:"type:varchar(255);not null"`

	// Canonical name-string
}

func (c CanonicalFull) StringID() string   { return c.ID }
func (c CanonicalFull) StringName() string { return c.Name }

// CanonicalStem is a stemmed derivative of a simple canonical form.
type CanonicalStem struct {
	// UUID v5 for the stemmed derivative of a simple canonical form.
	ID string `gorm:"type:uuid;primary_key;auto_increment:false"`

	// Stemmed canonical name-string
	Name string `gorm:"type:varchar(255);not null"`
}

func (c CanonicalStem) StringID() string   { return c.ID }
func (c CanonicalStem) StringName() string { return c.Name }

// NameStringIndex is a name-strings relations to datasets.
type NameStringIndex struct {
	// DataSourceID refers to a data-source ID.
	DataSourceID int `gorm:"index:name_string_ids_idx"`

	// RecordID is a unique ID for record. We do our best to
	// get it from the record IDs, either global or local,
	// but if all fails, id is assigned by gnames in a format
	// of 'gn_{int}'.
	RecordID string `gorm:"type:varchar(255);index:name_string_ids_idx"`

	// NameStringI is UUID5 of a full name-string from the dataset.
	NameStringID string `gorm:"type:uuid;index:name_string_id;index:name_string_ids_idx"`

	// The id to create an outlink.
	OutlinkID string `gorm:"type:varchar(255)"`

	// Global id from the dataset.
	GlobalID string `gorm:"type:varchar(255)"`

	// NameID is an ID of a nomenclatural name provided by data source.
	NameID string `gorm:"type:varchar(255)"`

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

// Word is a word from a name-string.
type Word struct {
	// ID generated by combinding modified word and type converted to integer
	//together with a pipe, and generating UUID5 from it.
	//For example: "alb|2"
	ID string `gorm:"primary_key;type:uuid;auto_increment:false"`

	// Normalized is the word normalized by GNparser. This field is used
	// for sorting results.
	Normalized string `gorm:"type:varchar(250);primary_key;auto_increment:false"`

	// Modified is a heavy-normalized word. This field is used for matching.
	Modified string `gorm:"type:varchar(250);not null;index:words_modified"`

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
	Name string `gorm:"type:varchar(500);index:vern_str_name_idx;not null"`
}

type VernacularStringIndex struct {
	// DataSourceID refers to a data-source ID.
	DataSourceID int `gorm:"index:vernacular_string_idx_idx"`

	// RecordID is a unique ID for record. We do our best to
	// get it from the record IDs, either global or local,
	// but if all fails, id is assigned by gnames in a format
	// of 'gn_{int}'.
	RecordID string `gorm:"type:varchar(255);index:vernacular_string_idx_idx"`

	// VernacularStringID is UUID5 of a full name-string from the dataset.
	VernacularStringID string `gorm:"type:uuid;index:vernacular_string_id;index:vernacular_string_idx_idx"`

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

func SetCollation(db *pgxpool.Pool) error {
	ctx := context.Background()
	type d struct {
		table, column string
		varchar       int
	}
	data := []d{
		{"name_strings", "name", 500},
		{"canonicals", "name", 255},
		{"canonical_fulls", "name", 255},
		{"canonical_stems", "name", 255},
		{"words", "normalized", 255},
		{"words", "modified", 255},
		{"vernacular_strings", "name", 255},
	}
	qStr := `
ALTER TABLE %s
	ALTER COLUMN %s TYPE VARCHAR(%d) COLLATE "C"
`

	for _, v := range data {
		q := fmt.Sprintf(qStr, v.table, v.column, v.varchar)
		_, err := db.Exec(ctx, q)
		if err != nil {
			slog.Error(
				"Cannot set collation.",
				"table", v.table,
				"column", v.column,
			)
			return err
		}
	}
	return nil
}
