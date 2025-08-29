package buildio

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/gnames/gnfmt/gnlang"
	"golang.org/x/sync/errgroup"
)

type vern struct {
	ctID         string
	languageOrig sql.NullString
	language     sql.NullString
	langCode     sql.NullString
}

func (b *buildio) fixVernLang() error {
	slog.Info("Moving new language data to language_orig")
	b.langOrig(context.Background())
	slog.Info("Normalizing vernacular language")
	chIn := make(chan vern)
	g, ctx := errgroup.WithContext(context.Background())

	g.Go(func() error {
		defer close(chIn)
		return b.loadVern(ctx, chIn)
	})

	g.Go(func() error {
		return b.normVernLang(ctx, chIn)
	})

	if err := g.Wait(); err != nil {
		return err
	}

	slog.Info("Making sure all language codes are low case")
	err := b.langCodeLowCase()
	if err != nil {
		slog.Error("Could not set all langage codes to low case", "error", err)
		return err
	}
	return nil
}

func (b *buildio) langCodeLowCase() error {
	q := `
UPDATE vernacular_string_indices
	SET lang_code = LOWER(lang_code)
`
	_, err := b.db.Exec(context.Background(), q)
	if err != nil {
		return err
	}
	return nil
}

func (b *buildio) langOrig(ctx context.Context) error {
	q := `
UPDATE vernacular_string_indices
	SET language_orig = language
	WHERE language_orig IS NULL
`
	_, err := b.db.Exec(ctx, q)
	if err != nil {
		return err
	}
	return nil
}

func (b *buildio) loadVern(ctx context.Context, ch chan<- vern) error {
	timeStart := time.Now().UnixNano()
	q := `
SELECT ctid, language, lang_code
	FROM vernacular_string_indices
`
	rows, err := b.db.Query(ctx, q)
	if err != nil {
		return err
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		count++
		var vern vern
		if err := rows.Scan(&vern.ctID, &vern.language, &vern.langCode); err != nil {
			return err
		}
		ch <- vern

		if count%50_000 == 0 {
			timeSpent := float64(time.Now().UnixNano()-timeStart) / 1_000_000_000
			speed := int64(float64(count) / timeSpent)
			fmt.Fprintf(os.Stderr, "\r%s", strings.Repeat(" ", 60))
			fmt.Fprintf(os.Stderr, "\rParsed %s names, %s names/sec",
				humanize.Comma(int64(count)), humanize.Comma(speed))
		}
	}
	fmt.Fprintf(os.Stderr, "\r%s\r", strings.Repeat(" ", 60))
	slog.Info("Finished normalization of vernacular languages")
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

func (b *buildio) normVernLang(ctx context.Context, ch <-chan vern) error {
	for vern := range ch {
		switch {
		case len(vern.language.String) == 2:
			lang3, err := gnlang.LangCode2To3Letters(vern.language.String)
			if err != nil {
				continue
			}
			if len(vern.langCode.String) != 3 {
				vern.langCode = sql.NullString{String: lang3, Valid: true}
			}
			lang := gnlang.Lang(lang3)
			if lang != "" {
				vern.language = sql.NullString{String: lang, Valid: true}
			}
		case len(vern.language.String) == 3:
			_, err := gnlang.LangCode3To2Letters(vern.language.String)
			if err != nil {
				continue
			}
			if len(vern.langCode.String) != 3 {
				vern.langCode = vern.language
			}
			lang := gnlang.Lang(vern.language.String)
			if lang != "" {
				vern.language = sql.NullString{String: lang, Valid: true}
			}
		case len(vern.langCode.String) != 3:
			lang3 := gnlang.LangCode(vern.language.String)
			if lang3 != "" {
				vern.langCode = sql.NullString{String: lang3, Valid: true}
			}
		default:
			continue
		}
		if err := b.updateVernRecord(ctx, vern); err != nil {
			slog.Error("Failed to update vernacular record",
				"ctid", vern.ctID, "error", err)
			return err
		}
	}
	return nil
}

func (b *buildio) updateVernRecord(ctx context.Context, v vern) error {
	q := `
UPDATE vernacular_string_indices
  SET language = $1, lang_code = $2
  WHERE ctid = $3
`
	_, err := b.db.Exec(ctx, q, v.language, v.langCode, v.ctID)
	return err
}
