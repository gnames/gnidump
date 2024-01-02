package buildio

import (
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

	"github.com/dustin/go-humanize"
	"github.com/gnames/gnfmt"
	"github.com/gnames/gnidump/internal/ent/model"
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

// importNameIndices import data into name_string_indices table.
func (b *buildio) importNameIndices() {
	slog.Info("Uploading data for name_string_indices table")

	err := b.kvSci.Open()
	if err != nil {
		slog.Error("cannot open key-value store", "error", err)
		os.Exit(1)
	}
	defer b.kvSci.Close()

	chIn := make(chan []string)
	chOut := make(chan []model.NameStringIndex)
	var wg sync.WaitGroup
	var wg2 sync.WaitGroup
	wg.Add(1)
	wg2.Add(1)
	go b.loadNameStringIndices(chIn)
	go b.workerNameStringIndex(chIn, chOut, &wg)
	go b.dbNameStringIndices(chOut, &wg2)
	wg.Wait()
	close(chOut)
	wg2.Wait()
}

func (b *buildio) dbNameStringIndices(chOut <-chan []model.NameStringIndex,
	wg *sync.WaitGroup) {
	defer wg.Done()
	var total int64
	timeStart := time.Now().UnixNano()
	for nsi := range chOut {
		total += b.saveNameStringIndices(nsi)
		timeSpent := float64(time.Now().UnixNano()-timeStart) / 1_000_000_000
		speed := int64(float64(total) / timeSpent)
		fmt.Printf("\r%s", strings.Repeat(" ", 40))
		fmt.Printf("\rUploaded %s indices, %s names/sec",
			humanize.Comma(total), humanize.Comma(speed))
	}
	slog.Info("Uploaded name_string_indices table")
}

func (b *buildio) saveNameStringIndices(nsi []model.NameStringIndex) int64 {
	db := pgConn(b.cfg)
	defer db.Close()

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

func (b *buildio) workerNameStringIndex(chIn <-chan []string,
	chOut chan<- []model.NameStringIndex, wg *sync.WaitGroup) {
	defer wg.Done()
	enc := gnfmt.GNgob{}
	res := make([]model.NameStringIndex, b.cfg.BatchSize)
	i := 0
	for row := range chIn {
		dsID, err := strconv.Atoi(row[nsiDataSourceIDF])
		if err != nil {
			slog.Error("Cannot convert to int", "error", err)
		}
		codeID, err := strconv.Atoi(row[nsiCodeIDF])
		if err != nil {
			codeID = 0
		}
		var parsed ParsedData
		parsedBytes, err := b.kvSci.GetValue([]byte(row[nsiNameStringIDF]))
		if err != nil {
			slog.Error("Cannot get Value", "error", err,
				"data-source", row[nsiDataSourceIDF],
				"record", row[nsIDF],
			)
			// os.Exit(1)
			continue
		}
		err = enc.Decode(parsedBytes, &parsed)
		if err != nil {
			slog.Error("Cannot decode parsed data", "error", err)
			os.Exit(1)
		}
		dsi := model.NameStringIndex{
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
		if i < b.cfg.BatchSize {
			res[i] = dsi
		} else {
			chOut <- res
			i = 0
			res = make([]model.NameStringIndex, b.cfg.BatchSize)
			res[i] = dsi
		}
		i++
	}
	chOut <- res[0:i]
}

func (b *buildio) loadNameStringIndices(chIn chan<- []string) {
	path := filepath.Join(b.cfg.DumpDir, "name_string_indices.csv")
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
