package rebuild

import (
	"encoding/csv"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gnames/gnidump/str"
)

// List of fields indices for data sources CSV file. The value corresponds to
// the position of a field in the row.
const (
	dsIDF            = 0
	dsTitleF         = 1
	dsDescF          = 2
	dsWebURLF        = 4
	dsDataURLF       = 5
	dsUpdatedAtF     = 11
	dsIsCuratedF     = 12
	dsIsAutoCuratedF = 13
	dsRecordCountF   = 14
)

// DataSourceInf provides fields associated with a DataSource
type DataSourceInf struct {
	Title          string
	TitleShort     string
	Description    string
	UUID           string
	HomeURL        string
	DataURL        string
	IsOutlinkReady bool
	OutlinkURL     string
	OutlinkID      func(n NameInf) string
}

// NameInf provides fields associated with a name-string in a particular
// data source.
type NameInf struct {
	RecordID         string
	AcceptedRecordID string
	LocalID          string
	GlobalID         string
	Canonical        string
	CanonicalFull    string
}

// DataSourcesInf provides missing data for data_sources table.
var DataSourcesInf = map[int]DataSourceInf{
	1: {
		Title:          "Catalogue of Life",
		TitleShort:     "Catalogue of Life",
		UUID:           "d4df2968-4257-4ad9-ab81-bedbbfb25e2a",
		HomeURL:        "https://www.catalogueoflife.org/",
		DataURL:        "http://www.catalogueoflife.org/DCA_Export/archive.php",
		IsOutlinkReady: true,
		OutlinkURL:     "http://www.catalogueoflife.org/annual-checklist/2019/details/species/id/{}",
		OutlinkID: func(n NameInf) string {
			return n.LocalID
		},
	},
	2: {
		TitleShort:     "Wikispecies",
		UUID:           "68923690-0727-473c-b7c5-2ae9e601e3fd",
		HomeURL:        "https://species.wikimedia.org/wiki/Main_Page",
		IsOutlinkReady: true,
		DataURL: "http://dumps.wikimedia.org/specieswiki/latest/" +
			"specieswiki-latest-pages-articles.xml.bz2",
		OutlinkURL: "http://species.wikimedia.org/wiki/{}",
		OutlinkID: func(n NameInf) string {
			return strings.ReplaceAll(n.CanonicalFull, " ", "_")
		},
	},
	3: {
		Title:          "Integrated Taxonomic Information System",
		TitleShort:     "ITIS",
		UUID:           "5d066e84-e512-4a2f-875c-0a605d3d9f35",
		HomeURL:        "https://www.itis.gov/",
		DataURL:        "https://www.itis.gov/downloads/itisMySQLTables.tar.gz",
		IsOutlinkReady: true,
		OutlinkURL:     "https://www.itis.gov/servlet/SingleRpt/SingleRpt?search_topic=TSN&search_value={}#null",
		OutlinkID: func(n NameInf) string {
			return n.RecordID
		},
	},
	4: {
		Title:          "National Center for Biotechnology Information",
		TitleShort:     "NCBI",
		UUID:           "97d7633b-5f79-4307-a397-3c29402d9311",
		HomeURL:        "https://www.ncbi.nlm.nih.gov/",
		DataURL:        "ftp://ftp.ncbi.nih.gov/pub/taxonomy/taxdump.tar.gz",
		IsOutlinkReady: true,
		OutlinkURL: "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?" +
			"mode=Undef&name={}&lvl=0&srchmode=1&keep=1&unlock",
		OutlinkID: func(n NameInf) string {
			return url.PathEscape(n.Canonical)
		},
	},
	5: {
		Title:          "Index Fungorum: Species Fungorum",
		TitleShort:     "Index Fungorum",
		UUID:           "af06816a-0b28-4a09-8219-bd1d63289858",
		HomeURL:        "http://www.speciesfungorum.org",
		IsOutlinkReady: true,
		OutlinkURL:     "http://www.indexfungorum.org/Names/NamesRecord.asp?RecordID={}",
		OutlinkID: func(n NameInf) string {
			return n.RecordID
		},
	},
	8: {
		TitleShort: "IRMNG (old)",
		UUID:       "f8e586aa-876e-4b0a-ab89-da0b4a64c19a",
		HomeURL:    "https://irmng.org/",
	},
	9: {
		TitleShort:     "WoRMS",
		UUID:           "bf077d91-673a-4be4-8af9-76db45d07e98",
		IsOutlinkReady: true,
		HomeURL:        "https://marinespecies.org",
	},
	10: {
		TitleShort: "Freebase",
		UUID:       "bacd21f0-44e0-43e2-914c-70929916f257",
	},
	11: {
		Title:          "Global Biodiversity Information Facility Backbone Taxonomy",
		TitleShort:     "GBIF Backbone Taxonomy",
		UUID:           "eebb6f49-e1a1-4f42-b9d5-050844c893cd",
		IsOutlinkReady: true,
		HomeURL:        "https://www.gbif.org/dataset/d7dddbf4-2cf0-4f39-9b2a-bb099caae36c",
	},
	12: {
		TitleShort:     "EOL",
		UUID:           "dba5f880-a40d-479b-a1ad-a646835edde4",
		HomeURL:        "https://eol.org",
		DataURL:        "https://eol.org/data/provider_ids.csv.gz",
		IsOutlinkReady: true,
		OutlinkURL:     "https://eol.org/pages/{}",
		OutlinkID: func(n NameInf) string {
			return n.RecordID
		},
	},
	113: {
		Title:      "Zoological names",
		TitleShort: "Zoological names",
	},
	117: {
		Title:      "Birds of Tansania",
		TitleShort: "Birds of Tansania",
	},
	119: {
		Title:      "Tansania Plant Specimens",
		TitleShort: "Tansania Plant Specimens",
	},
	142: {
		Title:      "The Clements Checklist of Birds of the World",
		TitleShort: "The Clements Checklist of Birds",
	},
	147: {
		TitleShort: "VASCAN",
	},
	149: {
		Title: "Ocean Biodiversity Information System",
	},
	155: {
		TitleShort:     "FishBase",
		UUID:           "bacd21f0-44e0-43e2-914c-70929916f257",
		IsOutlinkReady: true,
		HomeURL:        "https://www.fishbase.in/home.htm",
	},
	165: {
		Description: "The Tropicos database links over 1.33M scientific names " +
			"with over 4.87M specimens and over 685K digital images. The data " +
			"includes over 150K references from over 52.6K publications offered " +
			"as a free service to the world’s scientific community.",
	},
	167: {
		TitleShort:     "IPNI",
		UUID:           "6b3905ce-5025-49f3-9697-ddd5bdfb4ff0",
		HomeURL:        "https://www.ipni.org/",
		IsOutlinkReady: true,
		OutlinkURL:     "https://www.ipni.org/n/{}",
		OutlinkID: func(n NameInf) string {
			return n.RecordID
		},
	},
	168: {
		TitleShort:     "ION",
		UUID:           "1137dfa3-5b8c-487d-b497-dc0938605864",
		HomeURL:        "http://organismnames.com/",
		IsOutlinkReady: true,
		OutlinkURL:     "http://www.organismnames.com/details.htm?lsid={}",
		OutlinkID: func(n NameInf) string {
			return n.RecordID
		},
	},
	170: {
		TitleShort:     "Arctos",
		UUID:           "eea8315d-a244-4625-859a-226675622312",
		HomeURL:        "https://arctosdb.org/",
		IsOutlinkReady: true,
		OutlinkURL:     "https://arctos.database.museum/name/{}",
		OutlinkID: func(n NameInf) string {
			return url.QueryEscape(n.Canonical)
		},
	},
	172: {
		TitleShort:     "PaleoBioDB",
		UUID:           "fad9970e-c358-4e1b-8cc3-f9ad2582751f",
		HomeURL:        "https://paleobiodb.org/#/",
		IsOutlinkReady: true,
	},
	173: {
		TitleShort:     "The Reptile DataBase",
		UUID:           "c24e0905-4980-4e1d-aff2-ee0ef54ea1f8",
		HomeURL:        "http://reptile-database.org/",
		IsOutlinkReady: true,
	},
	174: {
		TitleShort:     "Mammal Species of the World",
		UUID:           "464dafec-1037-432d-8449-c0b309e0a030",
		HomeURL:        "https://www.departments.bucknell.edu/biology/resources/msw3/",
		DataURL:        "https://www.departments.bucknell.edu/biology/resources/msw3/export.asp",
		IsOutlinkReady: true,
		OutlinkURL:     "https://www.departments.bucknell.edu/biology/resources/msw3/browse.asp?s=y&id={}",
		OutlinkID: func(n NameInf) string {
			return n.LocalID
		},
	},
	175: {
		TitleShort:     "BirdLife International",
		UUID:           "b1d8de7a-ab96-455f-acd8-f3fff2d7d169",
		HomeURL:        "http://www.birdlife.org/",
		DataURL:        "http://datazone.birdlife.org/species/taxonomy",
		IsOutlinkReady: true,
		OutlinkURL:     "http://datazone.birdlife.org/species/results?thrlev1=&thrlev2=&kw={}",
		OutlinkID: func(n NameInf) string {
			return url.PathEscape(n.Canonical)
		},
	},
	179: {
		TitleShort:     "Open Tree of Life",
		UUID:           "e10865e2-cdd9-4f97-912f-08f3d5ef49f7",
		IsOutlinkReady: true,
		HomeURL:        "https://tree.opentreeoflife.org/",
		DataURL:        "https://files.opentreeoflife.org/ott/",
	},
	181: {
		TitleShort:     "IRMNG",
		UUID:           "417454fa-a0a1-4b9c-814d-edc0f4f25ad8",
		IsOutlinkReady: true,
		HomeURL:        "https://irmng.org/",
		DataURL:        "https://irmng.org/export/",
	},
	183: {
		TitleShort:     "Sherborn Index Animalium",
		UUID:           "05ad6ca2-fc37-47f4-983a-72e535420e28",
		IsOutlinkReady: true,
		HomeURL:        "https://www.sil.si.edu/DigitalCollections/indexanimalium/taxonomicnames/",
		DataURL: "https://www.sil.si.edu/DigitalCollections/indexanimalium/" +
			"Datasets/2006.01.06.TaxonomicData.csv",
	},
	184: {
		TitleShort:     "ASM Mammal Diversity DB",
		UUID:           "94270cdd-5424-4bb1-8324-46ccc5386dc7",
		HomeURL:        "https://mammaldiversity.org/",
		DataURL:        "https://mammaldiversity.org/",
		IsOutlinkReady: true,
		OutlinkURL:     "https://mammaldiversity.org/species-account/species-id={}",
		OutlinkID: func(n NameInf) string {
			return n.AcceptedRecordID
		},
	},
	185: {
		TitleShort:     "IOC World Bird List",
		UUID:           "6421ffec-38e3-40fb-a6d9-af27238a47a1",
		IsOutlinkReady: true,
		HomeURL:        "https://www.worldbirdnames.org/",
		DataURL:        "https://www.worldbirdnames.org/ioc-lists/master-list-2/",
	},
	186: {
		TitleShort:     "MCZbase",
		UUID:           "c79d055b-211b-40de-8e27-618011656265",
		IsOutlinkReady: true,
		HomeURL:        "https://mczbase.mcz.harvard.edu/",
		OutlinkURL:     "https://mczbase.mcz.harvard.edu/name/{}",
		OutlinkID: func(n NameInf) string {
			return url.PathEscape(n.Canonical)
		},
	},
	187: {
		TitleShort:     "Clements' Birds of the World",
		UUID:           "577c0b56-4a3c-4314-8724-14b304f601de",
		IsOutlinkReady: true,
		HomeURL:        "https://www.birds.cornell.edu/clementschecklist/",
		DataURL:        "https://www.birds.cornell.edu/clementschecklist/download/",
	},
	188: {
		TitleShort:     "American Ornithological Society",
		UUID:           "91d38806-8435-479f-a18d-705e5cb0767c",
		HomeURL:        "https://americanornithology.org/",
		IsOutlinkReady: true,
		DataURL:        "https://checklist.americanornithology.org/taxa.csv",
		OutlinkURL:     "https://checklist.americanornithology.org/taxa/{}",
		OutlinkID: func(n NameInf) string {
			return n.RecordID
		},
	},
	189: {
		TitleShort:     "Howard & Moore Birds of the World",
		UUID:           "85023fe5-bf2a-486b-bdae-3e61cefd41fd",
		HomeURL:        "https://www.howardandmoore.org/",
		IsOutlinkReady: true,
		DataURL:        "https://www.howardandmoore.org/howard-and-moore-database/",
	},
	194: {
		TitleShort:     "Plazi",
		UUID:           "68938dc9-b93d-43bc-9d51-5c2a632f136f",
		HomeURL:        "https://www.plazi.org/",
		IsOutlinkReady: true,
		DataURL:        "http://tb.plazi.org/GgServer/xml.rss.xml",
		OutlinkURL:     "http://tb.plazi.org/GgServer/html/{}",
		OutlinkID: func(n NameInf) string {
			return n.LocalID
		},
	},
	195: {
		TitleShort:     "AlgaeBase",
		UUID:           "a5869bfb-7cbf-40f2-88d3-962922dac43f",
		HomeURL:        "https://www.algaebase.org/",
		IsOutlinkReady: true,
		OutlinkURL:     "https://www.algaebase.org/search/species/detail/?species_id={}",
		OutlinkID: func(n NameInf) string {
			return n.RecordID
		},
	},
}

// UploadDataSources populates data_sources table with data.
func (rb Rebuild) UploadDataSources() error {
	db := rb.NewDbGorm()
	defer db.Close()
	log.Println("Populating data_sources table")
	ds, err := rb.loadDataSources()
	if err != nil {
		return err
	}
	for _, v := range ds {
		db.Create(&v)
	}
	return nil
}

func (rb Rebuild) loadDataSources() ([]DataSource, error) {
	var ds []DataSource
	path := filepath.Join(rb.DumpDir, "data_sources.csv")
	f, err := os.Open(path)
	if err != nil {
		return ds, err
	}
	defer f.Close()

	r := csv.NewReader(f)

	// skip header
	_, err = r.Read()
	if err != nil {
		log.Printf("ERROR: %s", err.Error())
	}
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("ERROR: %s", err.Error())
		}
		d, err := rowToDataSource(row)
		if err != nil {
			return ds, err
		}
		ds = append(ds, d)

	}
	return ds, nil
}

func rowToDataSource(row []string) (DataSource, error) {
	res := DataSource{}
	id, err := strconv.Atoi(row[dsIDF])
	if err != nil {
		return res, err
	}
	recNum, _ := strconv.Atoi(row[dsRecordCountF])
	updateAt, err := time.Parse(time.RFC3339, row[dsUpdatedAtF])
	if err != nil {
		return res, err
	}

	title := row[dsTitleF]
	info := DataSourceInf{UUID: "00000000-0000-0000-0000-000000000000", TitleShort: str.ShortTitle(title)}
	if data, ok := DataSourcesInf[id]; ok {
		info = data
	}
	if info.Title != "" {
		title = info.Title
	}
	description := row[dsDescF]
	if info.Description != "" {
		description = info.Description
	}

	res = DataSource{
		ID:             id,
		UUID:           info.UUID,
		Title:          title,
		TitleShort:     info.TitleShort,
		Description:    description,
		WebsiteURL:     info.HomeURL,
		DataURL:        info.DataURL,
		IsOutlinkReady: info.IsOutlinkReady,
		OutlinkURL:     info.OutlinkURL,
		IsCurated:      row[dsIsCuratedF] == "t",
		IsAutoCurated:  row[dsIsAutoCuratedF] == "t",
		RecordCount:    recNum,
		UpdatedAt:      updateAt,
	}

	return res, nil
}
