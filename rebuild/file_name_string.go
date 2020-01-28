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
	"github.com/gnames/gnidump/keyval"
	"github.com/lib/pq"
	uuid "github.com/satori/go.uuid"
	"gitlab.com/gogna/gnparser"
)

const (
	nsIDF   = 0
	nsNameF = 1
)

type CanonicalData struct {
	ID        string
	Value     string
	FullID    string
	FullValue string
	StemID    string
	StemValue string
}

func (rb Rebuild) UploadNameString() error {
	log.Println("Uploading data for name_strings table")
	chIn := make(chan []string)
	chCan := make(chan []CanonicalData)
	chOut := make(chan []NameString)
	err := keyval.ResetKeyVal(rb.KeyValDir)
	if err != nil {
		return err
	}
	kv := keyval.InitKeyVal(rb.KeyValDir)
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
		log.Fatal(err)
	}
	q = fmt.Sprintf(q0, "canonical_fulls", strings.Join(calFull, ","))
	if _, err = db.Query(q); err != nil {
		log.Fatal(err)
	}
	q = fmt.Sprintf(q0, "canonical_stems", strings.Join(calStem, ","))
	if _, err = db.Query(q); err != nil {
		log.Fatal(err)
	}
}

func (rb Rebuild) saveNameStrings(db *sql.DB, ns []NameString) int64 {
	columns := []string{"id", "name", "cardinality", "canonical_id",
		"canonical_full_id", "canonical_stem_id"}
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
			v.CanonicalFullID, v.CanonicalStemID)
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

func (rb Rebuild) workerNameString(kv *badger.DB, chIn <-chan []string,
	chCan chan<- []CanonicalData, chOut chan<- []NameString, wg *sync.WaitGroup) {
	var err error
	defer wg.Done()
	kvTxn := kv.NewTransaction(true)

	gnp := gnparser.NewGNparser()
	res := make([]NameString, rb.Batch)
	cans := make([]CanonicalData, 0, rb.Batch)
	i := 0
	for row := range chIn {
		id := row[nsIDF]
		p := gnp.ParseToObject(row[nsNameF])
		key := id
		val := p.Id
		if err = kvTxn.Set([]byte(key), []byte(val)); err == badger.ErrTxnTooBig {
			err = kvTxn.Commit()
			if err != nil {
				log.Fatal(err)
			}
			kvTxn = kv.NewTransaction(true)
			err = kvTxn.Set([]byte(key), []byte(val))
			if err != nil {
				log.Fatal(err)
			}

		}
		var canonicalID, canonicalFullID, canonicalStemID sql.NullString
		var cardinality sql.NullInt32
		if p.Parsed {
			cardinality = sql.NullInt32{
				Int32: p.Cardinality,
				Valid: true,
			}
			val := p.Canonical.GetSimple()
			canonicalID = sql.NullString{
				String: uuid.NewV5(gnNameSpace, val).String(),
				Valid:  true,
			}
			can := CanonicalData{ID: canonicalID.String, Value: val}

			if p.Canonical.GetSimple() != p.Canonical.GetFull() {
				val = p.Canonical.GetFull()
				canonicalFullID = sql.NullString{
					String: uuid.NewV5(gnNameSpace, val).String(),
					Valid:  true,
				}
				can.FullID = canonicalFullID.String
				can.FullValue = val
			}
			// Stems used for fuzzy matching, and we do not fuzzy match uninomials.
			if p.Cardinality > 1 {
				val = p.Canonical.GetStem()
				canonicalStemID = sql.NullString{
					String: uuid.NewV5(gnNameSpace, val).String(),
					Valid:  true,
				}
				can.StemID = canonicalStemID.String
				can.StemValue = val
			}
			cans = append(cans, can)
		}
		_ = canonicalStemID
		n := NameString{
			ID:              p.Id,
			Name:            p.Verbatim,
			Cardinality:     cardinality,
			CanonicalID:     canonicalID,
			CanonicalFullID: canonicalFullID,
			CanonicalStemID: canonicalStemID,
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
