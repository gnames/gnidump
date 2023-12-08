package rebuild

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/gnames/gnparser"
	"github.com/gnames/gnparser/ent/parsed"
	"github.com/gnames/gnuuid"
	"github.com/lib/pq"
)

func (rb Rebuild) CreateWords() {
	db := rb.NewDb()
	defer db.Close()
	q := `SELECT name
					FROM name_strings`
	rows, err := db.Query(q)
	if err != nil {
		slog.Error("Cannot get names from db", "error", err)
		os.Exit(1)
	}
	slog.Info("Processing names for words tables")
	rb.processWords(rows)
}

var batch int64 = 50_000

func (rb Rebuild) processWords(rows *sql.Rows) {
	cfg := gnparser.NewConfig(gnparser.OptWithDetails(true), gnparser.OptJobsNum(100))
	gnp := gnparser.New(cfg)

	var name string
	var wordNames []WordNameString
	var words []Word
	var names []string
	wordsMap := make(map[string]Word)

	var count int64
	for rows.Next() {
		if count != 0 && count%batch == 0 {
			fmt.Printf("\r%s", strings.Repeat(" ", 35))
			fmt.Printf("\rProcessed %s names for `word_name_strings`", humanize.Comma(count))
			words, wordNames = processParsedWords(gnp, names)
			for i := range words {
				wordsMap[words[i].ID+"|"+words[i].Normalized] = words[i]
			}
			rb.saveNameWords(wordNames)
			names = names[:0]
		}
		if err := rows.Scan(&name); err != nil {
			slog.Error("Cannot scan", "error", err)
		} else {
			names = append(names, name)
		}
		count++
	}
	words, wordNames = processParsedWords(gnp, names)
	for i := range words {
		wordsMap[words[i].ID+"|"+words[i].Normalized] = words[i]
	}
	fmt.Printf("\r%s", strings.Repeat(" ", 35))
	fmt.Printf("\rProcessed %s names for `word_name_strings`", humanize.Comma(count))
	fmt.Println()
	rb.saveNameWords(wordNames)
	rb.prepWords(wordsMap)
}

func processParsedWords(gnp gnparser.GNparser, names []string) ([]Word, []WordNameString) {
	wordNames := make([]WordNameString, 0, len(names)*5)
	words := make([]Word, 0, len(names)*5)
	ps := gnp.ParseNames(names)
	for i := range ps {
		if !ps[i].Parsed || ps[i].Surrogate != nil || ps[i].Hybrid != nil {
			continue
		}
		nsID := ps[i].VerbatimID
		cID := gnuuid.New(ps[i].Canonical.Simple).String()
		for _, v := range ps[i].Words {
			wt := v.Type
			mod := parsed.NormalizeByType(v.Normalized, wt)
			idstr := fmt.Sprintf("%s|%d", mod, int(wt))
			wordID := gnuuid.New(idstr).String()
			nw := WordNameString{NameStringID: nsID, CanonicalID: cID, WordID: wordID}
			switch wt {
			case
				parsed.SpEpithetType,
				parsed.InfraspEpithetType,
				parsed.AuthorWordType:
				word := Word{
					ID:         wordID,
					Normalized: v.Normalized,
					Modified:   mod,
					TypeID:     int(wt),
				}
				words = append(words, word)
				wordNames = append(wordNames, nw)
			}
		}
	}
	return words, wordNames
}

func (rb Rebuild) saveNameWords(wns []WordNameString) {
	db := rb.NewDb()
	defer db.Close()
	wns = uniqWordNameString(wns)
	columns := []string{"word_id", "name_string_id", "canonical_id"}
	transaction, err := db.Begin()
	if err != nil {
		slog.Error("Cannot start postgres transaction", "error", err)
		os.Exit(1)
	}
	stmt, err := transaction.Prepare(pq.CopyIn("word_name_strings", columns...))
	if err != nil {
		slog.Error("Cannot prepare copy statement", "error", err)
		os.Exit(1)
	}
	for _, v := range wns {
		_, err = stmt.Exec(v.WordID, v.NameStringID, v.CanonicalID)
	}
	if err != nil {
		slog.Error("Cannot save words to db", "error", err)
		os.Exit(1)
	}

	_, err = stmt.Exec()
	if err != nil {
		slog.Error("Cannot run final exec for db", "error", err)
		os.Exit(1)
	}

	err = stmt.Close()
	if err != nil {
		slog.Error("Cannot close exec", "error", err)
		os.Exit(1)
	}
	if err = transaction.Commit(); err != nil {
		slog.Error("Cannot close postgres transaction", "error", err)
		os.Exit(1)
	}
}

func (rb Rebuild) prepWords(nws map[string]Word) {
	slog.Info("Saving words", "wordsNum", len(nws))
	words := make([]Word, 0, batch)
	var count int64
	for _, v := range nws {
		if count != 0 && count%batch == 0 {
			fmt.Printf("\r%s", strings.Repeat(" ", 35))
			fmt.Printf("\rProcessed %s names for `words` table", humanize.Comma(count))
			rb.saveWords(words)
			words = make([]Word, 0, batch)
		}
		words = append(words, v)
		count++
	}
	rb.saveWords(words)
	fmt.Printf("\r%s", strings.Repeat(" ", 35))
	fmt.Printf("\rProcessed %s words for `words` table", humanize.Comma(count))
	fmt.Println()
}

func (rb Rebuild) saveWords(ws []Word) {
	db := rb.NewDb()
	defer db.Close()
	columns := []string{"id", "normalized", "modified", "type_id"}
	transaction, err := db.Begin()
	if err != nil {
		slog.Error("Cannot start postgres transaction", "error", err)
		os.Exit(1)
	}
	stmt, err := transaction.Prepare(pq.CopyIn("words", columns...))
	if err != nil {
		slog.Error("Cannot prepare copy", "error", err)
		os.Exit(1)
	}
	for _, v := range ws {
		_, err = stmt.Exec(v.ID, v.Normalized, v.Modified, v.TypeID)
	}
	if err != nil {
		slog.Error("Cannot save words", "error", err)
		os.Exit(1)
	}

	_, err = stmt.Exec()
	if err != nil {
		slog.Error("Cannot run final exec", "error", err)
		os.Exit(1)
	}

	err = stmt.Close()
	if err != nil {
		slog.Error("Cannot close copy", "error", err)
		os.Exit(1)
	}
	if err = transaction.Commit(); err != nil {
		slog.Error("Cannot commit transaction", "error", err)
		os.Exit(1)
	}
}

func uniqWordNameString(wns []WordNameString) []WordNameString {
	wnsMap := make(map[string]WordNameString)
	for _, v := range wns {
		wnsMap[v.WordID+"|"+v.NameStringID] = v
	}
	res := make([]WordNameString, len(wnsMap))
	var count int
	for _, v := range wnsMap {
		res[count] = v
		count++
	}
	return res
}
