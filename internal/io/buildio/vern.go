package buildio

import (
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
	"github.com/gnames/gnidump/internal/ent/model"
	"github.com/gnames/gnuuid"
	"github.com/lib/pq"
)

// List of fields from name-strings CSV file. The value correspondes to the
// position of a field in a row.
const (
	vsIDF   = 0
	vsNameF = 1
)

func (b *buildio) importVern() {
	var err error
	slog.Info("Uploading data for vernacular_strings table")
	chIn := make(chan []string)
	chOut := make(chan []model.VernacularString)
	var wg sync.WaitGroup
	var wg2 sync.WaitGroup
	wg.Add(1)
	wg2.Add(1)

	err = b.kvVern.Open()
	if err != nil {
		slog.Error("cannot open key-value", "error", err)
		os.Exit(1)
	}
	defer b.kvVern.Close()

	b.truncateTable("vernacular_strings")

	go b.loadVernStrings(chIn)
	go b.workerVernString(chIn, chOut, &wg)
	go b.dbVernString(chOut, &wg2)

	wg.Wait()
	close(chOut)
	wg2.Wait()
}

func (b *buildio) dbVernString(
	chOut <-chan []model.VernacularString,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	var total int64
	timeStart := time.Now().UnixNano()
	for vrn := range chOut {
		total += b.saveVernStrings(vrn)
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

func (b *buildio) saveVernStrings(vs []model.VernacularString) int64 {
	db := pgConn(b.cfg)
	defer db.Close()

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

func (b *buildio) workerVernString(
	chIn <-chan []string,
	chOut chan<- []model.VernacularString,
	wg *sync.WaitGroup,
) {
	var err error
	var kvTxn *badger.Txn
	var valBytes []byte
	dupl := make(map[string]struct{})
	defer wg.Done()
	enc := gnfmt.GNgob{}
	kvTxn, err = b.kvVern.GetTransaction()
	if err != nil {
		slog.Error("Cannot make transaction", "error", err)
		os.Exit(1)
	}
	res := make([]model.VernacularString, b.cfg.BatchSize)
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

			kvTxn, err = b.kvVern.GetTransaction()
			if err != nil {
				slog.Error("Cannot make transaction", "error", err)
				os.Exit(1)
			}

			err = kvTxn.Set([]byte(key), []byte(valBytes))
			if err != nil {
				slog.Error("Cannot set key/value", "error", err)
				os.Exit(1)
			}
		}

		vrn := model.VernacularString{
			ID:   val,
			Name: name,
		}

		if i < b.cfg.BatchSize {
			res[i] = vrn
		} else {
			chOut <- res
			i = 0
			res = make([]model.VernacularString, b.cfg.BatchSize)
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

func (b *buildio) loadVernStrings(chIn chan<- []string) {
	dupl := make(map[string]struct{})
	path := filepath.Join(b.cfg.DumpDir, "vernacular_strings.csv")
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
