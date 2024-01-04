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
	"github.com/gnames/gnidump/internal/ent/model"
	"github.com/gnames/gnidump/internal/str"
	"github.com/gnames/gnparser"
	"github.com/gnames/gnparser/ent/parsed"
	"github.com/gnames/gnuuid"
	"github.com/lib/pq"
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

	_ = b.truncateTable("name_strings")

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
	var n model.NameString
	var c canonicalData

	gnpCfg := gnparser.NewConfig()
	gnp := gnparser.New(gnpCfg)

	kvTxn, err := b.kvSci.GetTransaction()
	if err != nil {
		slog.Error("cannot make key-val transaction", "error", err)
		return err
	}

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
			n, c, kvTxn, err = b.processSciRow(gnp, kvTxn, row)
			if err != nil {
				return err
			}

			if c.Cardinality > 0 {
				cans = append(cans, c)
			}

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

func (b *buildio) processSciRow(
	gnp gnparser.GNparser,
	kvTxn *badger.Txn,
	row []string,
) (model.NameString, canonicalData, *badger.Txn, error) {
	var err error
	var valBytes []byte
	var n model.NameString
	var c canonicalData

	enc := gnfmt.GNgob{}
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
		return n, c, kvTxn, err
	}
	if err = kvTxn.Set([]byte(key), []byte(valBytes)); err == badger.ErrTxnTooBig {
		err = kvTxn.Commit()
		if err != nil {
			slog.Error("cannot commit key/value transaction", "error", err)
		}
		kvTxn, err = b.kvSci.GetTransaction()
		if err != nil {
			slog.Error("cannot recreate key-val transaction", "error", err)
			return n, c, kvTxn, err
		}
		err = kvTxn.Set([]byte(key), []byte(valBytes))
		if err != nil {
			slog.Error("cannot set key/value", "error", err)
			return n, c, kvTxn, err
		}

	}
	// Save stems of uninomials as well, we will use them for
	// exact matching to remove false positives from bloom filters.
	n, c = b.getNameString(p)
	return n, c, kvTxn, nil
}

func (*buildio) getNameString(p parsed.Parsed) (model.NameString, canonicalData) {
	var canData canonicalData
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
		canData := canonicalData{ID: canonicalID.String, Value: val, Cardinality: int(p.Cardinality)}

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
	return n, canData
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
			if !ok {
				chOut = nil
			}
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

func (b *buildio) saveNameStrings(ns []model.NameString) (int64, error) {
	db := pgConn(b.cfg)
	defer db.Close()

	columns := []string{"id", "name", "year", "cardinality", "canonical_id",
		"canonical_full_id", "canonical_stem_id", "virus", "bacteria", "surrogate",
		"parse_quality"}
	transaction, err := db.Begin()
	if err != nil {
		slog.Error("cannot start transaction", "error", err)
		return 0, err
	}
	stmt, err := transaction.Prepare(pq.CopyIn("name_strings", columns...))
	if err != nil {
		slog.Error("cannot prepare copy", "error", err)
		return 0, err
	}
	for _, v := range ns {
		_, err = stmt.Exec(v.ID, v.Name, v.Year, v.Cardinality, v.CanonicalID,
			v.CanonicalFullID, v.CanonicalStemID, v.Virus, v.Bacteria, v.Surrogate,
			v.ParseQuality)
		if err != nil {
			slog.Error("cannot insert rows", "error", err)
			return 0, err
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		slog.Error("cannot run final exec for db", "error", err)
		os.Exit(1)
	}

	err = stmt.Close()
	if err != nil {
		slog.Error("cannot close exec", "error", err)
		os.Exit(1)
	}
	if err = transaction.Commit(); err != nil {
		slog.Error("cannot close postgres transaction", "error", err)
		os.Exit(1)
	}
	return int64(len(ns)), nil
}

func (b *buildio) saveCanonicals(cs []canonicalData) error {
	db := pgConn(b.cfg)
	defer db.Close()

	var err error
	cal := make([]string, len(cs))
	calFull := make([]string, 0, len(cs))
	calStem := make([]string, 0, len(cs))
	for i, v := range cs {
		cal[i] = fmt.Sprintf("('%s', %s)", v.ID, str.QuoteString(v.Value))

		if v.FullID != "" {
			calFull = append(calFull,
				fmt.Sprintf("('%s', %s)", v.FullID, str.QuoteString(v.FullValue)))
		}
		if v.StemID != "" {
			calStem = append(calStem,
				fmt.Sprintf("('%s', %s)", v.StemID, str.QuoteString(v.StemValue)))
		}
	}

	q0 := `INSERT INTO %s (id, name) VALUES %s ON CONFLICT DO NOTHING`
	q := fmt.Sprintf(q0, "canonicals", strings.Join(cal, ","))
	if _, err = db.Query(q); err != nil {
		slog.Error("save canonicals failed", "error", err)
		return err
	}
	if len(calFull) > 0 {
		q = fmt.Sprintf(q0, "canonical_fulls", strings.Join(calFull, ","))
		if _, err = db.Query(q); err != nil {
			slog.Error("save canonical_fulls failed", "error", err)
			return err
		}
	}
	if len(calStem) > 0 {
		q = fmt.Sprintf(q0, "canonical_stems", strings.Join(calStem, ","))
		if _, err = db.Query(q); err != nil {
			slog.Error("save canonical_stems failed", "error", err)
			return err
		}
	}
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
