package buildio

import (
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

	"github.com/dustin/go-humanize"
	"github.com/gnames/gnfmt"
	"github.com/gnames/gnidump/internal/ent/model"
	"github.com/lib/pq"
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

func (b *buildio) importVernIndices() {
	err := b.kvVern.Open()
	if err != nil {
		slog.Error("cannot open key-value store", "error", err)
		os.Exit(1)
	}
	defer b.kvVern.Close()

	slog.Info("Uploading data for vernacular_string_indices table")

	chIn := make(chan []string)
	chOut := make(chan []model.VernacularStringIndex)
	var wg sync.WaitGroup
	var wg2 sync.WaitGroup
	wg.Add(1)
	wg2.Add(1)

	go b.loadVernStringIndices(chIn)
	go b.workerVernStringIndex(chIn, chOut, &wg)
	go b.dbVernStringIndices(chOut, &wg2)
	wg.Wait()
	close(chOut)
	wg2.Wait()
	slog.Info("Uploaded name_string_indices table")
}

func (b *buildio) dbVernStringIndices(
	chOut <-chan []model.VernacularStringIndex,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	var total int64
	timeStart := time.Now().UnixNano()

	for vsi := range chOut {
		total += b.saveVernStringIndices(vsi)
		timeSpent := float64(time.Now().UnixNano()-timeStart) / 1_000_000_000
		speed := int64(float64(total) / timeSpent)
		fmt.Printf("\r%s", strings.Repeat(" ", 40))
		fmt.Printf("\rUploaded %s indices, %s names/sec",
			humanize.Comma(total), humanize.Comma(speed))
	}
	fmt.Println()
}

func (b *buildio) saveVernStringIndices(nsi []model.VernacularStringIndex) int64 {
	db := pgConn(b.cfg)
	defer db.Close()

	columns := []string{"data_source_id", "vernacular_string_id", "record_id",
		"language", "lang_code", "locality", "country_code"}
	transaction, err := db.Begin()
	if err != nil {
		slog.Error("cannot start transaction", "error", err)
		os.Exit(1)
	}
	stmt, err := transaction.Prepare(pq.CopyIn("vernacular_string_indices", columns...))
	if err != nil {
		slog.Error("cannot prepare copy", "error", err)
		os.Exit(1)
	}
	for _, v := range nsi {
		_, err = stmt.Exec(
			v.DataSourceID,
			v.VernacularStringID,
			v.RecordID,
			v.Language,
			v.LangCode,
			v.Locality,
			v.CountryCode,
		)
	}
	if err != nil {
		slog.Error("cannot insert rows", "error", err)
		os.Exit(1)
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
	return int64(len(nsi))
}

func (b *buildio) workerVernStringIndex(
	chIn <-chan []string,
	chOut chan<- []model.VernacularStringIndex,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	enc := gnfmt.GNgob{}
	res := make([]model.VernacularStringIndex, b.cfg.BatchSize)
	i := 0
	for row := range chIn {
		dsID, err := strconv.Atoi(row[vsiDataSourceIDF])
		if err != nil {
			slog.Error("cannot convert data_source_id to int", "error", err)
		}
		var uuid string
		uuidBytes, err := b.kvVern.GetValue([]byte(row[vsiVernStringIDF]))
		if err != nil {
			slog.Error("Cannot get Value", "error", err,
				"data-source", row[nsiDataSourceIDF],
				"record", row[nsIDF],
			)
			// os.Exit(1)
			continue
		}

		err = enc.Decode(uuidBytes, &uuid)
		if err != nil {
			slog.Error("cannot decode uuid", "error", err)
			os.Exit(1)
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
	chOut <- res[0:i]
}

func (b *buildio) loadVernStringIndices(chIn chan<- []string) {
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
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			slog.Error("cannot read vernacular_string_indices.csv", "error", err)
		}
		chIn <- row
	}
	close(chIn)
}
