package buildio

import (
	"context"
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

	"github.com/dgraph-io/badger/v2"
	"github.com/dustin/go-humanize"
	"github.com/gnames/gnfmt"
	"github.com/gnames/gnidump/pkg/ent/model"
	"github.com/gnames/gnparser"
	"github.com/gnames/gnparser/ent/parsed"
	"github.com/gnames/gnuuid"
	"golang.org/x/sync/errgroup"
)

const (
	nsIDF   = 0
	nsNameF = 1
)

// canonical Data provides data about various canonical forms of a name-string.
type canonicalData struct {
	ID          string
	Value       string
	FullID      string
	FullValue   string
	StemID      string
	StemValue   string
	Cardinality int
}

// parsedData provides data about parsed name-strings.
type parsedData struct {
	ID              string
	CanonicalSimple string
	CanonicalFull   string
}

func (b *buildio) importNameStrings() error {
	slog.Info("Importing name-strings")

	err := b.kvSci.Open()
	if err != nil {
		slog.Error("cannot open key-value store", "error", err)
		return err
	}
	defer b.kvSci.Close()

	_ = b.truncateTable("name_strings", "canonicals", "canonical_fulls", "canonical_stems")

	chIn := make(chan []string)
	chCan := make(chan []canonicalData)
	chOut := make(chan []model.NameString)
	var wg sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		defer close(chIn)
		return b.loadNameStrings(ctx, chIn)
	})
	for i := 0; i < b.cfg.JobsNum; i++ {
		wg.Add(1)
		g.Go(func() error {
			defer wg.Done()
			return b.workerNameString(ctx, chIn, chCan, chOut)
		})
	}
	g.Go(func() error {
		return b.dbNameString(ctx, chOut, chCan)
	})

	go func() {
		wg.Wait()
		close(chOut)
		close(chCan)
	}()

	if err := g.Wait(); err != nil {
		slog.Error("error in goroutines", "error", err)
		return err
	}

	slog.Info("Uploaded name_strings table")
	return nil
}

func (b *buildio) loadNameStrings(ctx context.Context, chIn chan<- []string) error {
	r, f, err := b.openCSV("name_strings.csv")
	if err != nil {
		return err
	}
	defer f.Close()

	// skip header
	_, err = r.Read()
	if err != nil {
		slog.Error("cannot read the header name_strings", "error", err)
		return err
	}
loop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			row, err := r.Read()
			if err == io.EOF {
				break loop
			}
			if err != nil {
				slog.Error("cannot read name_strings.csv", "error", err)
				return err
			}
			chIn <- row
		}
	}
	return nil
}

// workerNameString parses name-strings and prepares for the database.
func (b *buildio) workerNameString(
	ctx context.Context,
	chIn <-chan []string,
	chCan chan<- []canonicalData,
	chOut chan<- []model.NameString,
) error {
	var err error

	kvTxn, err := b.kvSci.GetTransaction()
	if err != nil {
		slog.Error("cannot make key-val transaction", "error", err)
		return err
	}

	enc := gnfmt.GNgob{}
	gnp := gnparser.New(gnparser.NewConfig())

	res := make([]model.NameString, b.cfg.BatchSize)
	cans := make([]canonicalData, 0, b.cfg.BatchSize)
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
			var p parsed.Parsed
			p, kvTxn, err = b.saveNameKV(gnp, enc, row, kvTxn)
			if err != nil {
				return err
			}
			var n model.NameString
			cans, n = b.prepareCansAndName(p, cans)

			if i < b.cfg.BatchSize {
				res[i] = n
			} else {
				chOut <- res
				chCan <- cans
				i = 0
				res = make([]model.NameString, b.cfg.BatchSize)
				cans = make([]canonicalData, 0, b.cfg.BatchSize)
				res[i] = n
			}
			i++
		}
	}
	err = kvTxn.Commit()
	if err != nil {
		slog.Error("cannot commit key/value transaction", "error", err)
		return err
	}

	chOut <- res[0:i]
	chCan <- cans
	return nil
}

func (*buildio) prepareCansAndName(
	p parsed.Parsed,
	cans []canonicalData,
) ([]canonicalData, model.NameString) {
	var canonicalID, canonicalFullID, canonicalStemID sql.NullString
	var cardinality sql.NullInt32
	var year sql.NullInt16
	if p.Parsed {
		cardinality = sql.NullInt32{
			Int32: int32(p.Cardinality),
			Valid: true,
		}
		year = parseYear(p)
		val := p.Canonical.Simple
		canonicalID = sql.NullString{
			String: gnuuid.New(val).String(),
			Valid:  true,
		}
		canData := canonicalData{
			ID:          canonicalID.String,
			Value:       val,
			Cardinality: int(p.Cardinality),
		}

		if p.Canonical.Simple != p.Canonical.Full {
			val = p.Canonical.Full
			canonicalFullID = sql.NullString{
				String: gnuuid.New(val).String(),
				Valid:  true,
			}
			canData.FullID = canonicalFullID.String
			canData.FullValue = val
		}

		if p.Cardinality > 0 && !strings.Contains(canData.Value, ".") {
			val = p.Canonical.Stemmed
			canonicalStemID = sql.NullString{
				String: gnuuid.New(val).String(),
				Valid:  true,
			}
			canData.StemID = canonicalStemID.String
			canData.StemValue = val
		}
		cans = append(cans, canData)
	}

	var bacteria, virus bool
	if p.Virus {
		virus = true
	}

	if p.Bacteria != nil && p.Bacteria.String() == "yes" {
		bacteria = true
	}

	var surrogate bool
	if p.Surrogate != nil {
		surrogate = true
	}
	n := model.NameString{
		ID:              p.VerbatimID,
		Name:            p.Verbatim,
		Cardinality:     cardinality,
		Year:            year,
		CanonicalID:     canonicalID,
		CanonicalFullID: canonicalFullID,
		CanonicalStemID: canonicalStemID,
		Virus:           virus,
		Bacteria:        bacteria,
		Surrogate:       surrogate,
		ParseQuality:    int(p.ParseQuality),
	}
	return cans, n
}

func (b *buildio) saveNameKV(
	gnp gnparser.GNparser,
	enc gnfmt.Encoder,
	row []string,
	kvTxn *badger.Txn,
) (parsed.Parsed, *badger.Txn, error) {
	var err error
	var valBytes []byte
	id := row[nsIDF]
	p := gnp.ParseName(row[nsNameF])
	key := id

	var can, canf string
	if p.Parsed {
		can = p.Canonical.Simple
		canf = p.Canonical.Full
	}
	val := parsedData{
		ID:              p.VerbatimID,
		CanonicalSimple: can,
		CanonicalFull:   canf,
	}

	valBytes, err = enc.Encode(val)
	if err != nil {
		slog.Error("cannot encode parsed data", "error", err)
		return p, nil, err
	}
	if err = kvTxn.Set([]byte(key), []byte(valBytes)); err == badger.ErrTxnTooBig {
		err = kvTxn.Commit()
		if err != nil {
			slog.Error("cannot commit key/value transaction", "error", err)
			return p, nil, err
		}
		kvTxn, err = b.kvSci.GetTransaction()
		if err != nil {
			slog.Error("cannot recreate key-val transaction", "error", err)
			return p, nil, err
		}
		err = kvTxn.Set([]byte(key), []byte(valBytes))
		if err != nil {
			slog.Error("cannot set key/value", "error", err)
			return p, nil, err
		}

	}
	return p, kvTxn, nil
}

func (b *buildio) dbNameString(
	ctx context.Context,
	chOut <-chan []model.NameString,
	chCan <-chan []canonicalData,
) error {
	var err error
	var saved, total int64
	timeStart := time.Now().UnixNano()
loop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ns, ok := <-chOut:
			if !ok {
				chOut = nil
			}
			saved, err = b.saveNameStrings(ns)
			if err != nil {
				return err
			}
			total += saved
			timeSpent := float64(time.Now().UnixNano()-timeStart) / 1_000_000_000
			speed := int64(float64(total) / timeSpent)
			fmt.Printf("\r%s", strings.Repeat(" ", 40))
			fmt.Printf("\rUploaded %s names, %s names/sec",
				humanize.Comma(total), humanize.Comma(speed))
		case cs, ok := <-chCan:
			if len(cs) > 0 {
				err = b.saveCanonicals(cs)
				if err != nil {
					return err
				}
			}
			if !ok {
				chCan = nil
			}
		}
		if chOut == nil && chCan == nil {
			break loop
		}
	}
	fmt.Println()
	return nil
}

func parseYear(p parsed.Parsed) sql.NullInt16 {
	res := sql.NullInt16{}
	if p.Authorship == nil || p.Authorship.Year == "" {
		return res
	}
	yr := strings.Trim(p.Authorship.Year, "()")
	yrInt, err := strconv.Atoi(yr[0:4])
	if err != nil {
		return res
	}
	return sql.NullInt16{Int16: int16(yrInt), Valid: true}
}

func (b *buildio) openCSV(fileName string) (*csv.Reader, *os.File, error) {
	path := filepath.Join(b.cfg.DumpDir, fileName)
	f, err := os.Open(path)
	if err != nil {
		slog.Error("Cannot open csv file", "error", err)
		return nil, nil, err
	}
	r := csv.NewReader(f)
	return r, f, nil
}
