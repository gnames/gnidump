package buildio

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/gnames/gnparser"
	"github.com/gnames/gnuuid"
	"golang.org/x/sync/errgroup"
)

type reparsed struct {
	nameStringID, name                            string
	canonicalID, canonicalFullID, canonicalStemID sql.NullString
	canonical, canonicalFull, canonicalStem       string
}

func (b *buildio) reparse() error {
	slog.Info("Reparsing name-strings")

	chIn := make(chan reparsed)
	chOut := make(chan reparsed)
	var wg sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		defer close(chIn)
		return b.loadReparse(ctx, chIn)
	})

	for i := 0; i < 50; i++ {
		wg.Add(1)
		g.Go(func() error {
			defer wg.Done()
			return b.workerReparse(ctx, chIn, chOut)
		})
	}

	g.Go(func() error {
		return b.saveReparse(ctx, chOut)
	})

	go func() {
		wg.Wait()
		close(chOut)
	}()

	if err := g.Wait(); err != nil {
		slog.Error("error in goroutines", "error", err)
		return err
	}

	slog.Info("Reparsed name_strings table")
	return nil
}

func (b *buildio) loadReparse(
	ctx context.Context,
	chIn chan<- reparsed,
) error {
	q := `
SELECT id, name, canonical_id, canonical_full_id, canonical_stem_id
FROM name_strings
`
	rows, err := b.db.Query(ctx, q)
	if err != nil {
		return err
	}
	defer rows.Close()

	var count int
	timeStart := time.Now().UnixNano()
	for rows.Next() {
		count++
		res := reparsed{}
		err = rows.Scan(
			&res.nameStringID, &res.name, &res.canonicalID,
			&res.canonicalFullID, &res.canonicalStemID,
		)
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			chIn <- res
		}

		if count%100_000 == 0 {
			timeSpent := float64(time.Now().UnixNano()-timeStart) / 1_000_000_000
			speed := int64(float64(count) / timeSpent)
			fmt.Fprintf(os.Stderr, "\r%s", strings.Repeat(" ", 40))
			fmt.Fprintf(os.Stderr, "\rParsed %s names, %s names/sec",
				humanize.Comma(int64(count)), humanize.Comma(speed))
		}
	}

	fmt.Fprintf(os.Stderr, "\r%s\r", strings.Repeat(" ", 40))
	slog.Info("Finished names reparsing")
	return nil
}

func (b *buildio) workerReparse(
	ctx context.Context,
	chIn <-chan reparsed,
	chOut chan<- reparsed,
) error {
	prsCfg := gnparser.NewConfig()
	prs := gnparser.New(prsCfg)
	for r := range chIn {
		select {
		case <-ctx.Done():
			for range chIn {
			}
			return ctx.Err()
		default:
		}

		var canonicalID, canonicalFullID, canonicalStemID string
		parsed := prs.ParseName(r.name)
		if !parsed.Parsed {
			continue
		}
		canonicalID = gnuuid.New(parsed.Canonical.Simple).String()
		if canonicalID == r.canonicalID.String {
			continue
		}
		if parsed.Canonical.Simple != parsed.Canonical.Full {
			canonicalFullID = gnuuid.New(parsed.Canonical.Full).String()
		} else {
			parsed.Canonical.Full = ""
		}
		canonicalStemID = gnuuid.New(parsed.Canonical.Stemmed).String()

		chOut <- reparsed{
			nameStringID:    r.nameStringID,
			name:            r.name,
			canonicalID:     newNullStr(canonicalID),
			canonicalFullID: newNullStr(canonicalFullID),
			canonicalStemID: newNullStr(canonicalStemID),
			canonical:       parsed.Canonical.Simple,
			canonicalFull:   parsed.Canonical.Full,
			canonicalStem:   parsed.Canonical.Stemmed,
		}
	}
	return nil
}

func newNullStr(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

func (b *buildio) saveReparse(
	ctx context.Context,
	chOut <-chan reparsed,
) error {
	// Open the log file in current directory.
	file, err := os.OpenFile("reparse.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}
	defer file.Close()

	// Create a logger that writes to the file.
	logger := log.New(file, "", log.LstdFlags)

	for {
		select {
		case <-ctx.Done():
			logger.Println("Context cancelled, stopping reparse logging.")
			return ctx.Err()
		case r, ok := <-chOut:
			if !ok {
				return nil // Channel closed, we're done
			}
			b.updateNameString(ctx, r)
			// Use the logger to write to the file instead of fmt.Printf.
			logger.Printf("Name: %s, Can: %s", r.name, r.canonical)
		}
	}
}

func (b *buildio) updateNameString(ctx context.Context, r reparsed) error {
	tx, err := b.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) // Rollback in case of any error

	_, err = tx.Exec(ctx, `
		UPDATE name_strings
		SET canonical_id = $1, canonical_full_id = $2, canonical_stem_id = $3
		WHERE id = $4`,
		r.canonicalID, r.canonicalFullID, r.canonicalStemID, r.nameStringID)
	if err != nil {
		return fmt.Errorf("update name_strings: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO canonicals (id, name) 
		VALUES ($1, $2) 
		ON CONFLICT (id) DO NOTHING`,
		r.canonicalID, r.canonical)
	if err != nil {
		return fmt.Errorf("insert into canonicals: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO canonical_stems (id, name) 
		VALUES ($1, $2) 
		ON CONFLICT (id) DO NOTHING`,
		r.canonicalStemID, r.canonicalStem)
	if err != nil {
		return fmt.Errorf("insert into canonical_stems: %w", err)
	}

	if r.canonicalFull != "" {
		_, err = tx.Exec(ctx, `
		INSERT INTO canonical_fulls (id, name) 
		VALUES ($1, $2) 
		ON CONFLICT (id) DO NOTHING`,
			r.canonicalFullID, r.canonicalFull)
		if err != nil {
			return fmt.Errorf("insert into canonical_fulls: %w", err)
		}
	}

	// Commit the transaction if all operations were successful
	return tx.Commit(ctx)
}
