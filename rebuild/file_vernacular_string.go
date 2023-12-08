package rebuild

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/dustin/go-humanize"
	"github.com/gnames/gnfmt"
	"github.com/gnames/gnidump/keyval"
	"github.com/gnames/gnuuid"
	"github.com/lib/pq"
)

// List of fields from name-strings CSV file. The value correspondes to the
// position of a field in a row.
const (
	vsIDF   = 0
	vsNameF = 1
)

func (rb Rebuild) UploadVernacularStrings() error {
	var err error
	slog.Info("Uploading data for vernacular_strings table")
	// truncate vernacular_strings table
	db := rb.NewDb()
	defer db.Close()

	_, err = db.Exec("TRUNCATE TABLE vernacular_strings")
	if err != nil {
		return err
	}
	chIn := make(chan []string)
	chOut := make(chan []VernacularString)
	err = keyval.ResetKeyVal(rb.VernKeyValDir)
	if err != nil {
		return err
	}
	kv := keyval.InitKeyVal(rb.VernKeyValDir)
	defer kv.Close()

	var wg sync.WaitGroup
	var wg2 sync.WaitGroup
	wg.Add(1)
	wg2.Add(1)

	go rb.loadVernStrings(chIn)
	go rb.workerVernString(kv, chIn, chOut, &wg)
	go rb.dbVernString(db, chOut, &wg2)

	wg.Wait()
	close(chOut)
	wg2.Wait()
	return nil
}

func (rb Rebuild) dbVernString(
	db *sql.DB,
	chOut <-chan []VernacularString,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	var total int64
	timeStart := time.Now().UnixNano()
	for vrn := range chOut {
		total += rb.saveVernStrings(db, vrn)
		timeSpent := float64(time.Now().UnixNano()-timeStart) / 1_000_000_000
		speed := int64(float64(total) / timeSpent)
		fmt.Printf("\r%s", strings.Repeat(" ", 40))
		fmt.Printf(
			"\rUploaded %s verns, %s verns/sec",
			humanize.Comma(total), humanize.Comma(speed),
		)
	}
	fmt.Println()
	slog.Info("Uploaded vernacular_strings table")
}

func (rb Rebuild) saveVernStrings(db *sql.DB, vs []VernacularString) int64 {
	columns := []string{"id", "name"}
	transaction, err := db.Begin()
	if err != nil {
		slog.Error("cannot start transaction", "error", err)
		os.Exit(1)
	}
	stmt, err := transaction.Prepare(pq.CopyIn("vernacular_strings", columns...))
	if err != nil {
		slog.Error("cannot start copy", "error", err)
		os.Exit(1)
	}
	for _, v := range vs {
		_, err = stmt.Exec(v.ID, v.Name)
		if err != nil {
			slog.Error(
				"cannot insert rows",
				"error", err, "id", v.ID, "name", v.Name,
			)
			os.Exit(1)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		slog.Error("cannot ran last Exec", "error", err)
		os.Exit(1)
	}

	err = stmt.Close()
	if err != nil {
		slog.Error("cannot close statement", "errror", err)
		os.Exit(1)
	}
	if err = transaction.Commit(); err != nil {
		slog.Error("cannot commit transaction", "errror", err)
		os.Exit(1)
	}
	return int64(len(vs))
}

func (rb Rebuild) workerVernString(
	kv *badger.DB,
	chIn <-chan []string,
	chOut chan<- []VernacularString,
	wg *sync.WaitGroup,
) {
	var err error
	var valBytes []byte
	dupl := make(map[string]struct{})
	defer wg.Done()
	enc := gnfmt.GNgob{}
	kvTxn := kv.NewTransaction(true)
	res := make([]VernacularString, rb.Batch)
	var i int
	for row := range chIn {
		id := row[vsIDF]
		name := row[vsNameF]
		key := id
		val := gnuuid.New(name).String()
		if _, ok := dupl[val]; ok {
			fmt.Println(val, name)
			continue
		}
		dupl[val] = struct{}{}

		valBytes, err = enc.Encode(val)
		if err != nil {
			slog.Error("Cannot encode value", "error", err)
			os.Exit(1)
		}

		if err = kvTxn.Set([]byte(key), []byte(valBytes)); err == badger.ErrTxnTooBig {
			err = kvTxn.Commit()
			if err != nil {
				slog.Error("Cannot commit key/value transaction", "error", err)
				os.Exit(1)
			}
			kvTxn = kv.NewTransaction(true)
			err = kvTxn.Set([]byte(key), []byte(valBytes))
			if err != nil {
				slog.Error("Cannot set key/value", "error", err)
				os.Exit(1)
			}
		}

		vrn := VernacularString{
			ID:   val,
			Name: name,
		}

		if i < rb.Batch {
			res[i] = vrn
		} else {
			chOut <- res
			i = 0
			res = make([]VernacularString, rb.Batch)
			res[i] = vrn
		}
		i++
	}

	err = kvTxn.Commit()
	if err != nil {
		slog.Error("Cannot commit key/value transaction", "error", err)
		os.Exit(1)
	}

	chOut <- res[0:i]
}

func (rb Rebuild) loadVernStrings(chIn chan<- []string) {
	dupl := make(map[string]struct{})
	path := filepath.Join(rb.DumpDir, "vernacular_strings.csv")
	f, err := os.Open(path)
	if err != nil {
		slog.Error("Cannot open csv file", "error", err)
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
		if _, ok := dupl[row[vsNameF]]; ok {
			fmt.Println(row)
			continue
		}
		dupl[row[vsNameF]] = struct{}{}
		chIn <- row
	}
	close(chIn)
}
