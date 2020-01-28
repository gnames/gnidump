package rebuild

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

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

func (rb Rebuild) UploadDataSources() error {
	db := rb.NewDbGorm()
	defer db.Close()
	log.Println("Uploading data for name_strings table")
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
	res = DataSource{
		ID:            id,
		Title:         row[dsTitleF],
		Description:   row[dsDescF],
		WebsiteURL:    row[dsWebURLF],
		DataURL:       row[dsDataURLF],
		IsCurated:     row[dsIsCuratedF] == "t",
		IsAutoCurated: row[dsIsAutoCuratedF] == "t",
		RecordCount:   recNum,
		UpdatedAt:     updateAt,
	}
	return res, nil
}
