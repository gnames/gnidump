package rebuild

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/dustin/go-humanize"
	"github.com/gnames/gnfmt"
	"github.com/gnames/gnidump/keyval"
	"github.com/lib/pq"
)

// List of fields from the name-string indices CSV file. The value corresponds
// to the position of the field in a row.
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

// UploadNameStringIndices constracts data for name_string_indices table and
// aploads them to the database.
func (rb Rebuild) UploadNameStringIndices() {
	slog.Info("Uploading data for name_string_indices table")
	kv := keyval.InitKeyVal(rb.ParserKeyValDir)
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
	slog.Info("Uploaded name_string_indices table")
}

func (rb Rebuild) saveNameStringIndices(db *sql.DB, nsi []NameStringIndex) int64 {
	columns := []string{"data_source_id", "name_string_id", "record_id",
		"local_id", "global_id", "outlink_id", "code_id", "rank", "accepted_record_id",
		"classification", "classification_ids", "classification_ranks"}
	transaction, err := db.Begin()
	if err != nil {
		slog.Error("Cannot start postgres transaction", "error", err)
		os.Exit(1)
	}
	stmt, err := transaction.Prepare(pq.CopyIn("name_string_indices", columns...))
	if err != nil {
		slog.Error("Cannot prepare copy", "error", err)
		os.Exit(1)
	}
	for _, v := range nsi {
		_, err = stmt.Exec(
			v.DataSourceID,
			v.NameStringID,
			v.RecordID,
			v.LocalID,
			v.GlobalID,
			v.OutlinkID,
			v.CodeID,
			v.Rank,
			v.AcceptedRecordID,
			v.Classification,
			v.ClassificationIDs,
			v.ClassificationRanks,
		)
	}
	if err != nil {
		slog.Error("Cannot run copy statement", "error", err)
		os.Exit(1)
	}

	_, err = stmt.Exec()
	if err != nil {
		slog.Error("Cannot run last exec", "error", err)
		os.Exit(1)
	}

	err = stmt.Close()
	if err != nil {
		slog.Error("Cannot close copy", "error", err)
		os.Exit(1)
	}
	if err = transaction.Commit(); err != nil {
		slog.Error("Cannot commit transaction", "error", err)
		os.Exit(1)
	}
	return int64(len(nsi))
}

func (rb Rebuild) workerNameStringIndex(kv *badger.DB, chIn <-chan []string,
	chOut chan<- []NameStringIndex, wg *sync.WaitGroup) {
	defer wg.Done()
	enc := gnfmt.GNgob{}
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
		var parsed ParsedData
		parsedBytes := keyval.GetValue(kv, row[nsiNameStringIDF])
		err = enc.Decode(parsedBytes, &parsed)
		if err != nil {
			slog.Error("Cannot decode parsed data", "error", err)
			os.Exit(1)
		}
		dsi := NameStringIndex{
			DataSourceID:        dsID,
			NameStringID:        parsed.ID,
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
		nInf := NameInf{
			RecordID:         dsi.RecordID,
			AcceptedRecordID: dsi.AcceptedRecordID,
			LocalID:          dsi.LocalID,
			GlobalID:         dsi.GlobalID,
			Canonical:        parsed.CanonicalSimple,
			CanonicalFull:    parsed.CanonicalFull,
		}
		if dInf, ok := DataSourcesInf[dsID]; ok && dInf.OutlinkID != nil {
			dsi.OutlinkID = dInf.OutlinkID(nInf)
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
		slog.Error("Cannot open name_string_indices.csv", "error", err)
	}
	defer f.Close()
	r := csv.NewReader(f)

	// skip header
	_, err = r.Read()
	if err != nil {
		slog.Error("Cannot read csv header", "error", err)
	}
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			slog.Error("Cannot read csv line", "error", err)
		}
		chIn <- row
	}
	close(chIn)
}

func (rb Rebuild) RemoveOrphans() {
	db := rb.PgDB.NewDb()
	defer db.Close()
	slog.Info("Removing orphan name-strings")
	q := `DELETE FROM name_strings
  WHERE id IN (
    SELECT ns.id
      FROM name_strings ns
        LEFT OUTER JOIN name_string_indices nsi
          ON ns.id = nsi.name_string_id
      WHERE nsi.name_string_id IS NULL
    )`

	_, err := db.Exec(q)
	if err != nil {
		slog.Error("Cannot remove orphan name-strings", "error", err)
		os.Exit(1)
	}

	slog.Info("Removing orphan canonicals")
	q = `DELETE FROM canonicals
  WHERE id IN (
    SELECT c.id
      FROM canonicals  c
        LEFT OUTER JOIN name_strings ns
          ON c.id = ns.canonical_id
      WHERE ns.id IS NULL
    )`

	_, err = db.Exec(q)
	if err != nil {
		slog.Error("Cannot remove orphan canonicals", "error", err)
		os.Exit(1)
	}

	slog.Info("Removing orphan canonical_fulls")
	q = `DELETE FROM canonical_fulls
  WHERE id IN (
    SELECT cf.id
      FROM canonical_fulls  cf
        LEFT OUTER JOIN name_strings ns
          ON cf.id = ns.canonical_full_id
      WHERE ns.id IS NULL
    )`

	_, err = db.Exec(q)
	if err != nil {
		slog.Error("Cannot remove orphan canonical_fulls", "error", err)
		os.Exit(1)
	}

	slog.Info("Removing orphan canonical_stems")
	q = `DELETE FROM canonical_stems
    WHERE id IN (
      SELECT cs.id
        FROM canonical_stems  cs
          LEFT OUTER JOIN name_strings ns
            ON cs.id = ns.canonical_stem_id
        WHERE ns.id IS NULL
      )`
	_, err = db.Exec(q)
	if err != nil {
		slog.Error("Cannot remove orphan canonical_stems", "error", err)
		os.Exit(1)
	}

}

// verificationView creates data for a materialized view.
func (rb Rebuild) VerificationView() {
	db := rb.PgDB.NewDb()
	defer db.Close()
	slog.Info("Building verification view, it will take some time...")
	viewQuery := `CREATE MATERIALIZED VIEW verification AS
WITH taxon_names AS (
SELECT nsi.data_source_id, nsi.record_id, nsi.name_string_id, ns.name
  FROM name_string_indices nsi
    JOIN name_strings ns
      ON nsi.name_string_id = ns.id
)
SELECT nsi.data_source_id, nsi.record_id, nsi.name_string_id,
  ns.name, ns.year, ns.cardinality, ns.canonical_id, ns.virus, ns.bacteria,
  ns.parse_quality, nsi.local_id, nsi.outlink_id, nsi.accepted_record_id,
  tn.name_string_id as accepted_name_id,
  tn.name as accepted_name, nsi.classification, nsi.classification_ranks,
  nsi.classification_ids
  FROM name_string_indices nsi
    JOIN name_strings ns ON ns.id = nsi.name_string_id
    LEFT JOIN taxon_names tn
      ON nsi.data_source_id = tn.data_source_id AND
         nsi.accepted_record_id = tn.record_id
  WHERE
    (
      ns.canonical_id is not NULL AND
      surrogate != TRUE AND
      (bacteria != TRUE OR parse_quality < 3)
    ) OR ns.virus = TRUE`
	_, err := db.Exec("DROP MATERIALIZED VIEW IF EXISTS verification")
	if err != nil {
		slog.Error("Cannot drop verification view", "error", err)
		os.Exit(1)
	}
	_, err = db.Exec(viewQuery)
	if err != nil {
		slog.Error("Cannot run verification create", "error", err)
		os.Exit(1)
	}
	slog.Info("Building indices for verification view, it will take some time...")
	_, err = db.Exec("CREATE INDEX ON verification (canonical_id)")
	if err != nil {
		slog.Error("Cannot create verification index", "error", err)
		os.Exit(1)
	}
	_, err = db.Exec("CREATE INDEX ON verification (name_string_id)")
	if err != nil {
		slog.Error("Cannot create verification index2", "error", err)
		os.Exit(1)
	}
	_, err = db.Exec("CREATE INDEX ON verification (year)")
	if err != nil {
		slog.Error("Cannot create verification index3", "error", err)
		os.Exit(1)
	}
	slog.Info("View verification is created")
}
