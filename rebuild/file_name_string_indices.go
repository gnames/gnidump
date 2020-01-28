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
}

func (rb Rebuild) saveNameStringIndices(db *sql.DB, nsi []NameStringIndex) int64 {
	columns := []string{"data_source_id", "name_string_id", "taxon_id",
		"global_id", "code_id", "rank", "accepted_taxon_id", "classification",
		"classification_ids", "classification_ranks"}
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
			v.TaxonID,
			v.GlobalID,
			v.CodeID,
			v.Rank,
			v.AcceptedTaxonID,
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
			TaxonID:             row[nsiTaxonIDF],
			GlobalID:            row[nsiGlobalIDF],
			CodeID:              codeID,
			Rank:                row[nsiRankF],
			AcceptedTaxonID:     row[nsiAcceptedTaxonIDF],
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