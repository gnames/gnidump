package buildio

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

	"github.com/dgraph-io/badger/v2"
	"github.com/dustin/go-humanize"
	"github.com/gnames/gnfmt"
	"github.com/gnames/gnidump/internal/ent/model"
	"github.com/gnames/gnidump/internal/str"
	"github.com/gnames/gnparser"
	"github.com/gnames/gnparser/ent/parsed"
	"github.com/gnames/gnuuid"
	"github.com/lib/pq"
)

const (
	nsIDF   = 0
	nsNameF = 1
)

// Canonical Data provides data about various canonical forms of a name-string.
type CanonicalData struct {
	ID          string
	Value       string
	FullID      string
	FullValue   string
	StemID      string
	StemValue   string
	Cardinality int
}

type ParsedData struct {
	ID              string
	CanonicalSimple string
	CanonicalFull   string
}

func (b *buildio) importNameStrings() {
	slog.Info("Importing name-strings")

	err := b.kvSci.Open()
	if err != nil {
		slog.Error("cannot open key-value store", "error", err)
		os.Exit(1)
	}
	defer b.kvSci.Close()

	chIn := make(chan []string)
	chCan := make(chan []CanonicalData)
	chOut := make(chan []model.NameString)
	var wg sync.WaitGroup
	var wg2 sync.WaitGroup
	wg2.Add(1)

	go b.loadNameStrings(chIn)
	for i := 0; i < b.cfg.JobsNum; i++ {
		wg.Add(1)
		go b.workerNameString(chIn, chCan, chOut, &wg)
	}
	go b.dbNameString(chOut, chCan, &wg2)

	wg.Wait()
	close(chOut)
	close(chCan)
	wg2.Wait()
}

func (b *buildio) loadNameStrings(chIn chan<- []string) {
	path := filepath.Join(b.cfg.DumpDir, "name_strings.csv")
	f, err := os.Open(path)
	if err != nil {
		slog.Error("cannot open name_strings.csv", "error", err)
		os.Exit(1)
	}
	defer f.Close()
	r := csv.NewReader(f)

	// skip header
	_, err = r.Read()
	if err != nil {
		slog.Error("cannot read the header name_strings", "error", err)
		os.Exit(1)
	}
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			slog.Error("cannot read name_strings.csv", "error", err)
			os.Exit(1)
		}
		chIn <- row
	}
	close(chIn)
}

// workerNameString parses name-strings and prepares for the database.
func (b *buildio) workerNameString(
	chIn <-chan []string,
	chCan chan<- []CanonicalData,
	chOut chan<- []model.NameString,
	wg *sync.WaitGroup,
) {
	var err error
	var valBytes []byte
	defer wg.Done()
	enc := gnfmt.GNgob{}
	kvTxn, err := b.kvSci.GetTransaction()
	if err != nil {
		slog.Error("cannot make key-val transaction", "error", err)
		os.Exit(1)
	}

	cfg := gnparser.NewConfig()
	gnp := gnparser.New(cfg)
	res := make([]model.NameString, b.cfg.BatchSize)
	cans := make([]CanonicalData, 0, b.cfg.BatchSize)
	i := 0
	for row := range chIn {
		id := row[nsIDF]
		p := gnp.ParseName(row[nsNameF])
		key := id

		var can, canf string
		if p.Parsed {
			can = p.Canonical.Simple
			canf = p.Canonical.Full
		}
		val := ParsedData{
			ID:              p.VerbatimID,
			CanonicalSimple: can,
			CanonicalFull:   canf,
		}

		valBytes, err = enc.Encode(val)
		if err != nil {
			slog.Error("cannot encode parsed data", "error", err)
			os.Exit(1)
		}
		if err = kvTxn.Set([]byte(key), []byte(valBytes)); err == badger.ErrTxnTooBig {
			err = kvTxn.Commit()
			if err != nil {
				slog.Error("cannot commit key/value transaction", "error", err)
			}
			kvTxn, err = b.kvSci.GetTransaction()
			if err != nil {
				slog.Error("cannot make key-val transaction", "error", err)
				os.Exit(1)
			}
			err = kvTxn.Set([]byte(key), []byte(valBytes))
			if err != nil {
				slog.Error("cannot set key/value", "error", err)
				os.Exit(1)
			}

		}
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
			canData := CanonicalData{ID: canonicalID.String, Value: val, Cardinality: int(p.Cardinality)}

			if p.Canonical.Simple != p.Canonical.Full {
				val = p.Canonical.Full
				canonicalFullID = sql.NullString{
					String: gnuuid.New(val).String(),
					Valid:  true,
				}
				canData.FullID = canonicalFullID.String
				canData.FullValue = val
			}
			// Save stems of uninomials as well, we will use them for
			// exact matching to remove false positives from bloom filters.
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

		if i < b.cfg.BatchSize {
			res[i] = n
		} else {
			chOut <- res
			chCan <- cans
			i = 0
			res = make([]model.NameString, b.cfg.BatchSize)
			cans = make([]CanonicalData, 0, b.cfg.BatchSize)
			res[i] = n
		}
		i++
	}
	err = kvTxn.Commit()
	if err != nil {
		slog.Error("cannot commit key/value transaction", "error", err)
		os.Exit(1)
	}

	chOut <- res[0:i]
	chCan <- cans
}

func (b *buildio) dbNameString(
	chOut <-chan []model.NameString,
	chCan <-chan []CanonicalData,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	var total int64
	timeStart := time.Now().UnixNano()
	for {
		select {
		case ns, ok := <-chOut:
			total += b.saveNameStrings(ns)
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
				b.saveCanonicals(cs)
			}
			if !ok {
				chCan = nil
			}
		}
		if chOut == nil && chCan == nil {
			break
		}
	}
	slog.Info("Uploaded name_strings table")
}

func (b *buildio) saveNameStrings(ns []model.NameString) int64 {
	db := pgConn(b.cfg)
	defer db.Close()

	columns := []string{"id", "name", "year", "cardinality", "canonical_id",
		"canonical_full_id", "canonical_stem_id", "virus", "bacteria", "surrogate",
		"parse_quality"}
	transaction, err := db.Begin()
	if err != nil {
		slog.Error("cannot start transaction", "error", err)
		os.Exit(1)
	}
	stmt, err := transaction.Prepare(pq.CopyIn("name_strings", columns...))
	if err != nil {
		slog.Error("cannot prepare copy", "error", err)
		os.Exit(1)
	}
	for _, v := range ns {
		_, err = stmt.Exec(v.ID, v.Name, v.Year, v.Cardinality, v.CanonicalID,
			v.CanonicalFullID, v.CanonicalStemID, v.Virus, v.Bacteria, v.Surrogate,
			v.ParseQuality)
		if err != nil {
			slog.Error("cannot insert rows", "error", err)
			os.Exit(1)
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
	return int64(len(ns))
}

func (b *buildio) saveCanonicals(cs []CanonicalData) {
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
		os.Exit(1)
	}
	if len(calFull) > 0 {
		q = fmt.Sprintf(q0, "canonical_fulls", strings.Join(calFull, ","))
		if _, err = db.Query(q); err != nil {
			slog.Error("save canonical_fulls failed", "error", err)
			os.Exit(1)
		}
	}
	if len(calStem) > 0 {
		q = fmt.Sprintf(q0, "canonical_stems", strings.Join(calStem, ","))
		if _, err = db.Query(q); err != nil {
			slog.Error("save canonical_stems failed", "error", err)
			os.Exit(1)
		}
	}
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
