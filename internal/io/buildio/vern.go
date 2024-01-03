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
	"strings"
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/dustin/go-humanize"
	"github.com/gnames/gnfmt"
	"github.com/gnames/gnidump/internal/ent/model"
	"github.com/gnames/gnuuid"
	"github.com/lib/pq"
	"golang.org/x/sync/errgroup"
)

// List of fields from name-strings CSV file. The value correspondes to the
// position of a field in a row.
const (
	vsIDF   = 0
	vsNameF = 1
)

// importVern imports takes data from vernacular_strings.csv file and
// uploads it to the database.
func (b *buildio) importVern() error {
	slog.Info("Uploading data for vernacular_strings table")

	err := b.kvVern.Open()
	if err != nil {
		slog.Error("cannot open key-value", "error", err)
		return err
	}
	defer b.kvVern.Close()

	_ = b.truncateTable("vernacular_strings")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	chIn := make(chan []string)
	chOut := make(chan []model.VernacularString)

	g.Go(func() error {
		defer close(chIn)
		return b.loadVernStrings(ctx, chIn)
	})

	g.Go(func() error {
		defer close(chOut)
		return b.workerVernString(ctx, chIn, chOut)
	})

	g.Go(func() error {
		return b.dbVernString(ctx, chOut)
	})

	if err := g.Wait(); err != nil {
		slog.Error("error in goroutines", "error", err)
		return err
	}

	return nil
}

func (b *buildio) loadVernStrings(ctx context.Context, chIn chan<- []string) error {
	dupl := make(map[string]struct{})

	path := filepath.Join(b.cfg.DumpDir, "vernacular_strings.csv")

	f, err := os.Open(path)
	if err != nil {
		slog.Error("Cannot open csv file", "error", err)
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
				slog.Error("Cannot read CSV line", "error", err)
				return err
			}
			if _, ok := dupl[row[vsNameF]]; ok {
				fmt.Println(row)
				continue
			}
			dupl[row[vsNameF]] = struct{}{}
			chIn <- row
		}
	}
}

func (b *buildio) workerVernString(
	ctx context.Context,
	chIn <-chan []string,
	chOut chan<- []model.VernacularString,
) error {
	var err error
	var kvTxn *badger.Txn
	var vrn model.VernacularString

	kvTxn, err = b.kvVern.GetTransaction()
	if err != nil {
		slog.Error("Cannot make kvVern transaction", "error", err)
		return err
	}
	res := make([]model.VernacularString, b.cfg.BatchSize)
	var i int

	for row := range chIn {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			vrn, kvTxn, err = b.processRow(kvTxn, row)
			if err != nil {
				return err
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
	}

	err = kvTxn.Commit()
	if err != nil {
		slog.Error("Cannot commit key/value transaction", "error", err)
		os.Exit(1)
	}

	chOut <- res[0:i]
	return nil
}

func (b *buildio) dbVernString(
	ctx context.Context,
	chOut <-chan []model.VernacularString,
) error {
	var err error
	var saved, total int64
	timeStart := time.Now().UnixNano()

	for vrn := range chOut {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			saved, err = b.saveVernStrings(vrn)
			if err != nil {
				slog.Error("Cannot save vernacular_strings", "error", err)
				os.Exit(1)
			}
			total += saved
			timeSpent := float64(time.Now().UnixNano()-timeStart) / 1_000_000_000
			speed := int64(float64(total) / timeSpent)
			fmt.Printf("\r%s", strings.Repeat(" ", 40))
			fmt.Printf(
				"\rUploaded %s verns, %s verns/sec",
				humanize.Comma(total), humanize.Comma(speed),
			)
		}
	}
	fmt.Println()
	slog.Info("Uploaded vernacular_strings table")
	return nil
}

func (b *buildio) processRow(
	kvTxn *badger.Txn,
	row []string,
) (model.VernacularString, *badger.Txn, error) {
	var err error
	var vrn model.VernacularString
	var valBytes []byte
	enc := gnfmt.GNgob{}
	id := row[vsIDF]
	name := row[vsNameF]
	key := id
	val := gnuuid.New(name).String()

	valBytes, err = enc.Encode(val)
	if err != nil {
		slog.Error("Cannot encode value", "error", err)
		return vrn, kvTxn, err
	}

	if err = kvTxn.Set([]byte(key), []byte(valBytes)); err == badger.ErrTxnTooBig {
		err = kvTxn.Commit()
		if err != nil {
			slog.Error("Cannot commit key/value transaction", "error", err)
			return vrn, kvTxn, err
		}

		kvTxn, err = b.kvVern.GetTransaction()
		if err != nil {
			slog.Error("Cannot make transaction", "error", err)
			return vrn, kvTxn, err
		}

		err = kvTxn.Set([]byte(key), []byte(valBytes))
		if err != nil {
			slog.Error("Cannot set key/value", "error", err)
			return vrn, kvTxn, err
		}
	}

	vrn = model.VernacularString{
		ID:   val,
		Name: name,
	}
	return vrn, kvTxn, nil
}

func (b *buildio) saveVernStrings(vs []model.VernacularString) (int64, error) {
	db := pgConn(b.cfg)
	defer db.Close()

	columns := []string{"id", "name"}
	transaction, err := db.Begin()
	if err != nil {
		return 0, fmt.Errorf("cannot start transaction: %w", err)
	}

	stmt, err := prepareStatement(transaction, "vernacular_strings", columns)
	if err != nil {
		return 0, err
	}

	if err := insertRows(stmt, vs); err != nil {
		return 0, err
	}

	if err := finalizeStatement(stmt); err != nil {
		return 0, err
	}

	if err := transaction.Commit(); err != nil {
		return 0, fmt.Errorf("cannot commit transaction: %w", err)
	}

	return int64(len(vs)), nil
}

func prepareStatement(transaction *sql.Tx, tableName string, columns []string) (*sql.Stmt, error) {
	stmt, err := transaction.Prepare(pq.CopyIn(tableName, columns...))
	if err != nil {
		return nil, fmt.Errorf("cannot prepare statement: %w", err)
	}
	return stmt, nil
}

func insertRows(stmt *sql.Stmt, vs []model.VernacularString) error {
	for _, v := range vs {
		_, err := stmt.Exec(v.ID, v.Name)
		if err != nil {
			slog.Error(
				"Cannot insert rows",
				"error", err, "id", v.ID, "name", v.Name,
			)
			return fmt.Errorf("cannot insert rows: %w", err)
		}
	}
	return nil
}

func finalizeStatement(stmt *sql.Stmt) error {
	_, err := stmt.Exec()
	if err != nil {
		return fmt.Errorf("cannot run last Exec: %w", err)
	}
	err = stmt.Close()
	if err != nil {
		return fmt.Errorf("cannot close statement: %w", err)
	}
	return nil
}
