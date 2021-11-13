package rebuild

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/gnames/gnparser"
	"github.com/gnames/gnparser/ent/parsed"
	"github.com/gnames/gnuuid"
	"github.com/lib/pq"
)

func (rb Rebuild) CreateWords() {
	db := rb.PgDB.NewDb()
	defer db.Close()
	q := `SELECT name
					FROM name_strings`
	rows, err := db.Query(q)
	if err != nil {
		log.Println(err)
	}
	log.Print("Processing names for words tables.")
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
			log.Println(err)
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
		nsID := ps[i].VerbatimID
		for _, v := range ps[i].Words {
			wt := v.Type
			mod := parsed.NormalizeByType(v.Normalized, wt)
			idstr := fmt.Sprintf("%s|%d", mod, int(wt))
			wordID := gnuuid.New(idstr).String()
			nw := WordNameString{NameStringID: nsID, WordID: wordID}
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
	return words, wordNames
}

func (rb Rebuild) saveNameWords(wns []WordNameString) {
	db := rb.NewDb()
	defer db.Close()
	wns = uniqWordNameString(wns)
	columns := []string{"word_id", "name_string_id"}
	transaction, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	stmt, err := transaction.Prepare(pq.CopyIn("word_name_strings", columns...))
	if err != nil {
		log.Fatal(err)
	}
	for _, v := range wns {
		_, err = stmt.Exec(v.WordID, v.NameStringID)
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
}

func (rb Rebuild) prepWords(nws map[string]Word) {
	log.Printf("Saving %d words.\n", len(nws))
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
		log.Fatal(err)
	}
	stmt, err := transaction.Prepare(pq.CopyIn("words", columns...))
	if err != nil {
		log.Fatal(err)
	}
	for _, v := range ws {
		_, err = stmt.Exec(v.ID, v.Normalized, v.Modified, v.TypeID)
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
