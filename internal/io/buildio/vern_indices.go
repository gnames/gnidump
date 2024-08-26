package buildio

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/gnames/gnfmt"
	"github.com/gnames/gnidump/pkg/ent/model"
	"golang.org/x/sync/errgroup"
	"golang.org/x/text/language"
)

const (
	vsiDataSourceIDF  = 0
	vsiTaxonIDF       = 1
	vsiVernStringIDF  = 2
	vsiLangIDF        = 3
	vsiLocalityIDF    = 4
	vsiCountryCodeIDF = 5
)

var langMap = map[string]string{
	"Afrikaans":  "afr",
	"Arabic":     "ara",
	"Chinese":    "zho",
	"Danish":     "dan",
	"English":    "eng",
	"French":     "fra",
	"German":     "deu",
	"Greek":      "ell",
	"Hausa":      "hau",
	"Hawaiian":   "haw",
	"Indonesian": "ind",
	"Italian":    "ita",
	"Japanese":   "jpn",
	"Korean":     "kor",
	"Malagasy":   "mlg",
	"Portuguese": "por",
	"Romanian":   "ron",
	"Slovenian":  "slv",
	"Spanish":    "spa",
	"Swedish":    "swe",
	"Thai":       "tha",
	"Zulu":       "zul",
}

func (b *buildio) importVernIndices() error {
	err := b.kvVern.Open()
	if err != nil {
		slog.Error("cannot open key-value store", "error", err)
		return err
	}
	defer b.kvVern.Close()

	_ = b.truncateTable("vernacular_string_indices")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)

	slog.Info("Uploading data for vernacular_string_indices table")

	chIn := make(chan []string)
	chOut := make(chan []model.VernacularStringIndex)

	g.Go(func() error {
		defer close(chIn)
		return b.loadVernStringIndices(ctx, chIn)
	})

	g.Go(func() error {
		defer close(chOut)
		return b.workerVernStringIndex(ctx, chIn, chOut)
	})

	g.Go(func() error {
		return b.dbVernStringIndices(ctx, chOut)
	})

	if err := g.Wait(); err != nil {
		slog.Error("error in goroutines", "error", err)
		return err
	}

	slog.Info("Uploaded data for vernacular_string_indices table")
	return nil
}

func (b *buildio) dbVernStringIndices(
	ctx context.Context,
	chOut <-chan []model.VernacularStringIndex,
) error {
	var err error
	var saved, total int64
	timeStart := time.Now().UnixNano()

	for vsi := range chOut {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			saved, err = b.saveVernStringIndices(vsi)
			if err != nil {
				slog.Error("cannot save vernacular_string_indices", "error", err)
				return err
			}
			total += saved
			timeSpent := float64(time.Now().UnixNano()-timeStart) / 1_000_000_000
			speed := int64(float64(total) / timeSpent)
			fmt.Printf("\r%s", strings.Repeat(" ", 40))
			fmt.Printf("\rUploaded %s indices, %s names/sec",
				humanize.Comma(total), humanize.Comma(speed))
		}
	}
	fmt.Println()
	return nil
}

func (b *buildio) workerVernStringIndex(
	ctx context.Context,
	chIn <-chan []string,
	chOut chan<- []model.VernacularStringIndex,
) error {
	enc := gnfmt.GNgob{}
	res := make([]model.VernacularStringIndex, b.cfg.BatchSize)
	i := 0
	for row := range chIn {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			dsID, err := strconv.Atoi(row[vsiDataSourceIDF])
			if err != nil {
				slog.Error("cannot convert data_source_id to int", "error", err)
				return err
			}
			var uuid string
			uuidBytes, err := b.kvVern.GetValue([]byte(row[vsiVernStringIDF]))
			if err != nil {
				slog.Error("Cannot get Value", "error", err,
					"data-source", row[nsiDataSourceIDF],
					"record", row[nsIDF],
				)
				return err
			}

			err = enc.Decode(uuidBytes, &uuid)
			if err != nil {
				slog.Error("cannot decode uuid", "error", err)
				return err
			}

			vsi := model.VernacularStringIndex{
				DataSourceID:       dsID,
				VernacularStringID: uuid,
				RecordID:           row[vsiTaxonIDF],
				Language:           row[vsiLangIDF],
				Locality:           row[vsiLocalityIDF],
				CountryCode:        row[vsiCountryCodeIDF],
			}

			// normalize to ISO 639-3  (3-letter code) where possible
			tag, err := language.Parse(strings.ToLower(vsi.Language))
			if err == nil {
				base, _ := tag.Base()
				vsi.LangCode = base.ISO3()
			} else {
				if iso, ok := langMap[vsi.Language]; ok {
					vsi.LangCode = iso
				}
			}

			if i < b.cfg.BatchSize {
				res[i] = vsi
			} else {
				chOut <- res
				i = 0
				res = make([]model.VernacularStringIndex, b.cfg.BatchSize)
				res[i] = vsi
			}
			i++
		}
	}
	chOut <- res[0:i]
	return nil
}

func (b *buildio) loadVernStringIndices(ctx context.Context, chIn chan<- []string) error {
	path := filepath.Join(b.cfg.DumpDir, "vernacular_string_indices.csv")
	f, err := os.Open(path)
	if err != nil {
		slog.Error("cannot open vernacular_string_indices.csv", "error", err)
	}
	defer f.Close()
	r := csv.NewReader(f)

	// skip header
	_, err = r.Read()
	if err != nil {
		slog.Error("cannot read the header vernacular_string_indices", "error", err)
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
				slog.Error("cannot read vernacular_string_indices.csv", "error", err)
				return err
			}
			chIn <- row
		}
	}
}
