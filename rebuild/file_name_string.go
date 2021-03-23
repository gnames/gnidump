package rebuild

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/dustin/go-humanize"
	"github.com/gnames/gnfmt"
	"github.com/gnames/gnidump/keyval"
	"github.com/gnames/gnparser"
	"github.com/gnames/gnuuid"
	"github.com/lib/pq"
)

// List of fields from name-strings CSV file. The value correspondes to the
// position of a field in a row.
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

// UploadNameString constructs data for name_strings, canonicals,
// canonical_fulls, canonical_stems tables and uploads these data to the
// database.
func (rb Rebuild) UploadNameString() error {
	log.Println("Uploading data for name_strings table")
	chIn := make(chan []string)
	chCan := make(chan []CanonicalData)
	chOut := make(chan []NameString)
	err := keyval.ResetKeyVal(rb.ParserKeyValDir)
	if err != nil {
		return err
	}
	kv := keyval.InitKeyVal(rb.ParserKeyValDir)
	defer kv.Close()
	var wg sync.WaitGroup
	var wg2 sync.WaitGroup
	wg2.Add(1)
	go rb.loadNameStrings(chIn)
	for i := 0; i < rb.JobsNum; i++ {
		wg.Add(1)
		go rb.workerNameString(kv, chIn, chCan, chOut, &wg)
	}
	go rb.dbNameString(chOut, chCan, &wg2)
	wg.Wait()
	close(chOut)
	close(chCan)
	wg2.Wait()
	return nil
}

func (rb Rebuild) dbNameString(chOut <-chan []NameString,
	chCan <-chan []CanonicalData, wg *sync.WaitGroup) {
	defer wg.Done()
	db := rb.PgDB.NewDb()
	defer db.Close()
	var total int64
	timeStart := time.Now().UnixNano()
	for {
		select {
		case ns, ok := <-chOut:
			total += rb.saveNameStrings(db, ns)
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
				rb.saveCanonicals(cs)
			}
			if !ok {
				chCan = nil
			}
		}
		if chOut == nil && chCan == nil {
			break
		}
	}
	fmt.Println()
	log.Println("Uploaded name_strings table")
}

func (rb Rebuild) saveCanonicals(cs []CanonicalData) {
	db := rb.PgDB.NewDb()
	defer db.Close()
	var err error
	cal := make([]string, len(cs))
	calFull := make([]string, 0, len(cs))
	calStem := make([]string, 0, len(cs))
	for i, v := range cs {
		cal[i] = fmt.Sprintf("('%s', %s)", v.ID, QuoteString(v.Value))

		if v.FullID != "" {
			calFull = append(calFull,
				fmt.Sprintf("('%s', %s)", v.FullID, QuoteString(v.FullValue)))
		}
		if v.StemID != "" {
			calStem = append(calStem,
				fmt.Sprintf("('%s', %s)", v.StemID, QuoteString(v.StemValue)))
		}
	}

	q0 := `INSERT INTO %s (id, name) VALUES %s ON CONFLICT DO NOTHING`
	q := fmt.Sprintf(q0, "canonicals", strings.Join(cal, ","))
	if _, err = db.Query(q); err != nil {
		err = fmt.Errorf("Failed to populate canonicals table: %w", err)
		fmt.Println(q)
		log.Fatal(err)
	}
	if len(calFull) > 0 {
		q = fmt.Sprintf(q0, "canonical_fulls", strings.Join(calFull, ","))
		if _, err = db.Query(q); err != nil {
			log.Println("saveCanonicals canonical_fulls")
			log.Fatal(err)
		}
	}
	if len(calStem) > 0 {
		q = fmt.Sprintf(q0, "canonical_stems", strings.Join(calStem, ","))
		if _, err = db.Query(q); err != nil {
			log.Println("saveCanonicals canonical_stems")
			log.Fatal(err)
		}
	}
}

func (rb Rebuild) saveNameStrings(db *sql.DB, ns []NameString) int64 {
	columns := []string{"id", "name", "cardinality", "canonical_id",
		"canonical_full_id", "canonical_stem_id", "virus", "bacteria", "surrogate",
		"parse_quality"}
	transaction, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	stmt, err := transaction.Prepare(pq.CopyIn("name_strings", columns...))
	if err != nil {
		log.Fatal(err)
	}
	for _, v := range ns {
		_, err = stmt.Exec(v.ID, v.Name, v.Cardinality, v.CanonicalID,
			v.CanonicalFullID, v.CanonicalStemID, v.Virus, v.Bacteria, v.Surrogate,
			v.ParseQuality)
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
	return int64(len(ns))
}

type ParsedData struct {
	ID              string
	CanonicalSimple string
	CanonicalFull   string
}

func (rb Rebuild) workerNameString(kv *badger.DB, chIn <-chan []string,
	chCan chan<- []CanonicalData, chOut chan<- []NameString, wg *sync.WaitGroup) {
	var err error
	defer wg.Done()
	enc := gnfmt.GNgob{}
	kvTxn := kv.NewTransaction(true)

	cfg := gnparser.NewConfig()
	gnp := gnparser.New(cfg)
	res := make([]NameString, rb.Batch)
	cans := make([]CanonicalData, 0, rb.Batch)
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

		valBytes, err := enc.Encode(val)
		if err != nil {
			log.Fatal(err)
		}
		if err = kvTxn.Set([]byte(key), []byte(valBytes)); err == badger.ErrTxnTooBig {
			err = kvTxn.Commit()
			if err != nil {
				log.Fatal(err)
			}
			kvTxn = kv.NewTransaction(true)
			err = kvTxn.Set([]byte(key), []byte(valBytes))
			if err != nil {
				log.Fatal(err)
			}

		}
		var canonicalID, canonicalFullID, canonicalStemID sql.NullString
		var cardinality sql.NullInt32
		if p.Parsed {
			cardinality = sql.NullInt32{
				Int32: int32(p.Cardinality),
				Valid: true,
			}
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
		n := NameString{
			ID:              p.VerbatimID,
			Name:            p.Verbatim,
			Cardinality:     cardinality,
			CanonicalID:     canonicalID,
			CanonicalFullID: canonicalFullID,
			CanonicalStemID: canonicalStemID,
			Virus:           virus,
			Bacteria:        bacteria,
			Surrogate:       surrogate,
			ParseQuality:    int(p.ParseQuality),
		}
		if i < rb.Batch {
			res[i] = n
		} else {
			chOut <- res
			chCan <- cans
			i = 0
			res = make([]NameString, rb.Batch)
			cans = make([]CanonicalData, 0, rb.Batch)
			res[i] = n
		}
		i++
	}
	err = kvTxn.Commit()
	if err != nil {
		log.Fatal(err)
	}

	chOut <- res[0:i]
	chCan <- cans
}

func (rb Rebuild) loadNameStrings(chIn chan<- []string) {
	path := filepath.Join(rb.DumpDir, "name_strings.csv")
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
