package buildio

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/gnames/gnfmt"
	"github.com/gnames/gnidump/internal/ent/model"
	"golang.org/x/sync/errgroup"
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
func (b *buildio) importNameIndices() error {
	slog.Info("Uploading data for name_string_indices table")

	err := b.kvSci.Open()
	if err != nil {
		slog.Error("cannot open key-value store", "error", err)
		return err
	}
	defer b.kvSci.Close()

	_ = b.truncateTable("name_string_indices")

	chIn := make(chan []string)
	chOut := make(chan []model.NameStringIndex)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		defer close(chIn)
		return b.loadNameStringIndices(ctx, chIn)
	})
	g.Go(func() error {
		defer close(chOut)
		return b.workerNameStringIndex(ctx, chIn, chOut)
	})
	g.Go(func() error {
		return b.dbNameStringIndices(ctx, chOut)
	})

	if err := g.Wait(); err != nil {
		slog.Error("error in goroutines", "error", err)
		return err
	}

	slog.Info("Uploaded name_string_indices table")
	return nil
}

func (b *buildio) dbNameStringIndices(
	ctx context.Context,
	chOut <-chan []model.NameStringIndex,
) error {
	var err error
	var saved, total int64

	timeStart := time.Now().UnixNano()
	for nsi := range chOut {
		saved, err = b.saveNameStringIndices(nsi)
		if err != nil {
			slog.Error("Cannot save name-string-indices", "error", err)
			return err
		}
		total += saved
		timeSpent := float64(time.Now().UnixNano()-timeStart) / 1_000_000_000
		speed := int64(float64(total) / timeSpent)
		fmt.Printf("\r%s", strings.Repeat(" ", 40))
		fmt.Printf("\rUploaded %s indices, %s names/sec",
			humanize.Comma(total), humanize.Comma(speed))
	}
	fmt.Println()
	return nil
}

func (b *buildio) workerNameStringIndex(
	ctx context.Context,
	chIn <-chan []string,
	chOut chan<- []model.NameStringIndex,
) error {
	enc := gnfmt.GNgob{}
	res := make([]model.NameStringIndex, b.cfg.BatchSize)
	i := 0
loop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case row, ok := <-chIn:
			if !ok {
				break loop
			}
			dsi, err := b.processSciIdxRow(row, enc)
			if err != nil {
				return err
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
	}
	chOut <- res[0:i]
	return nil
}

func (b *buildio) processSciIdxRow(row []string, enc gnfmt.GNgob) (model.NameStringIndex, error) {
	var dsi model.NameStringIndex
	dsID, err := strconv.Atoi(row[nsiDataSourceIDF])
	if err != nil {
		slog.Error("Cannot convert to int", "error", err)
		return dsi, err
	}
	codeID, err := strconv.Atoi(row[nsiCodeIDF])
	if err != nil {
		codeID = 0
	}
	var parsed parsedData
	parsedBytes, err := b.kvSci.GetValue([]byte(row[nsiNameStringIDF]))
	if err != nil {
		slog.Error("Cannot get Value", "error", err,
			"data-source", row[nsiDataSourceIDF],
			"record", row[nsIDF],
		)
		// was continue
		return dsi, err
	}
	err = enc.Decode(parsedBytes, &parsed)
	if err != nil {
		slog.Error("Cannot decode parsed data", "error", err)
		return dsi, err
	}
	dsi = model.NameStringIndex{
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
	return dsi, nil
}

func (b *buildio) loadNameStringIndices(
	ctx context.Context,
	chIn chan<- []string,
) error {
	path := filepath.Join(b.cfg.DumpDir, "name_string_indices.csv")
	f, err := os.Open(path)
	if err != nil {
		slog.Error("Cannot open name_string_indices.csv", "error", err)
		return err
	}
	defer f.Close()
	r := csv.NewReader(f)

	// skip header
	_, err = r.Read()
	if err != nil {
		slog.Error("Cannot read csv header", "error", err)
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			row, err := r.Read()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				slog.Error("Cannot read csv line", "error", err)
				return err
			}
			chIn <- row
		}
	}
}
