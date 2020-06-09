package rebuild

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/dustin/go-humanize"
	"github.com/gnames/gnidump/keyval"
	"github.com/lib/pq"
)

const (
	nsiDataSourceIDF        = 0
	nsiNameStringIDF        = 1
	nsiTaxonIDF             = 3
	nsiGlobalIDF            = 4
	nsiLocalIDF             = 5
	nsiCodeIDF              = 6
	nsiRankF                = 7
	nsiAcceptedTaxonIDF     = 8
	nsiClassificationF      = 9
	nsiClassificationIDsF   = 10
	nsiClassificationRanksF = 11
)

func (rb Rebuild) UploadNameStringIndices() {
	log.Println("Uploading data for name_string_indices table")
	kv := keyval.InitKeyVal(rb.KeyValDir)
	defer kv.Close()
	chIn := make(chan []string)
	chOut := make(chan []NameStringIndex)
	var wg sync.WaitGroup
	var wg2 sync.WaitGroup
	wg.Add(1)
	wg2.Add(1)
	go rb.loadNameStringIndices(chIn)
	go rb.workerNameStringIndex(kv, chIn, chOut, &wg)
	go rb.dbNameStringIndices(chOut, &wg2)
	wg.Wait()
	close(chOut)
	wg2.Wait()
}

func (rb Rebuild) dbNameStringIndices(chOut <-chan []NameStringIndex,
	wg *sync.WaitGroup) {
	defer wg.Done()
	db := rb.PgDB.NewDb()
	defer db.Close()
	var total int64
	timeStart := time.Now().UnixNano()
	for nsi := range chOut {
		total += rb.saveNameStringIndices(db, nsi)
		timeSpent := float64(time.Now().UnixNano()-timeStart) / 1_000_000_000
		speed := int64(float64(total) / timeSpent)
		fmt.Printf("\r%s", strings.Repeat(" ", 40))
		fmt.Printf("\rUploaded %s indices, %s names/sec",
			humanize.Comma(total), humanize.Comma(speed))
	}
	fmt.Println()
	log.Println("Uploaded name_string_indices table")
	rb.verificationView(db)
}

func (rb Rebuild) saveNameStringIndices(db *sql.DB, nsi []NameStringIndex) int64 {
	columns := []string{"data_source_id", "name_string_id", "record_id",
		"local_id", "global_id", "code_id", "rank", "accepted_record_id",
		"classification", "classification_ids", "classification_ranks"}
	transaction, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	stmt, err := transaction.Prepare(pq.CopyIn("name_string_indices", columns...))
	if err != nil {
		log.Fatal(err)
	}
	for _, v := range nsi {
		_, err = stmt.Exec(
			v.DataSourceID,
			v.NameStringID,
			v.RecordID,
			v.LocalID,
			v.GlobalID,
			v.CodeID,
			v.Rank,
			v.AcceptedRecordID,
			v.Classification,
			v.ClassificationIDs,
			v.ClassificationRanks,
		)
	}
	if err != nil {
		log.Fatal(err)
	}

	_, err = stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}

	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}
	if err = transaction.Commit(); err != nil {
		log.Fatal(err)
	}
	return int64(len(nsi))
}
func (rb Rebuild) workerNameStringIndex(kv *badger.DB, chIn <-chan []string,
	chOut chan<- []NameStringIndex, wg *sync.WaitGroup) {
	defer wg.Done()
	res := make([]NameStringIndex, rb.Batch)
	i := 0
	for row := range chIn {
		dsID, err := strconv.Atoi(row[nsiDataSourceIDF])
		if err != nil {
			fmt.Printf("Error: %s\n", err.Error())
		}
		codeID, err := strconv.Atoi(row[nsiCodeIDF])
		if err != nil {
			codeID = 0
		}
		dsi := NameStringIndex{
			DataSourceID:        dsID,
			NameStringID:        keyval.GetValue(kv, row[nsiNameStringIDF]),
			RecordID:            row[nsiTaxonIDF],
			LocalID:             row[nsiLocalIDF],
			GlobalID:            row[nsiGlobalIDF],
			CodeID:              codeID,
			Rank:                row[nsiRankF],
			AcceptedRecordID:    row[nsiAcceptedTaxonIDF],
			Classification:      row[nsiClassificationF],
			ClassificationIDs:   row[nsiClassificationIDsF],
			ClassificationRanks: row[nsiClassificationRanksF],
		}
		if i < rb.Batch {
			res[i] = dsi
		} else {
			chOut <- res
			i = 0
			res = make([]NameStringIndex, rb.Batch)
			res[i] = dsi
		}
		i++
	}
	chOut <- res[0:i]
}

func (rb Rebuild) loadNameStringIndices(chIn chan<- []string) {
	path := filepath.Join(rb.DumpDir, "name_string_indices.csv")
	f, err := os.Open(path)
	if err != nil {
		log.Printf("ERROR: %s", err.Error())
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
		chIn <- row
	}
	close(chIn)
}

func (rb Rebuild) verificationView(db *sql.DB) {
	log.Println("Building verification view, it will take some time...")
	viewQuery := `CREATE MATERIALIZED VIEW verification as
WITH taxon_names AS (
SELECT nsi.data_source_id, nsi.record_id, ns.id as name_string_id, ns.name
	FROM name_string_indices nsi JOIN name_strings ns
		ON nsi.name_string_id = ns.id
)
SELECT ns.id, ns.canonical_id, ns.canonical_full_id, ns.name, ns.cardinality,
  nsi.data_source_id, nsi.record_id, nsi.name_string_id, nsi.local_id,
  nsi.outlink_id, nsi.accepted_record_id, tn.name_string_id as accepted_name_id,
  tn.name as accepted_name, nsi.classification, nsi.classification_ranks
  FROM name_string_indices nsi
    JOIN name_strings ns ON ns.id = nsi.name_string_id
    JOIN taxon_names tn
      ON nsi.data_source_id = tn.data_source_id AND
         nsi.name_string_id = tn.name_string_id AND
         nsi.accepted_record_id = tn.record_id`
	_, err := db.Exec("DROP MATERIALIZED VIEW IF EXISTS verification")
	if err != nil {
		log.Printf("verificationView")
		log.Fatal(err)
	}
	_, err = db.Exec(viewQuery)
	if err != nil {
		log.Printf("verificationView")
		log.Fatal(err)
	}
	log.Println("Building indices for verification view, it will take some time...")
	_, err = db.Exec("CREATE INDEX ON verification (canonical_id)")
	if err != nil {
		log.Printf("verificationView")
		log.Fatal(err)
	}
	_, err = db.Exec("CREATE INDEX ON verification (id)")
	if err != nil {
		log.Printf("verificationView")
		log.Fatal(err)
	}
	log.Println("View verification is created")
}
