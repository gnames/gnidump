package dump

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
)

// CreateCSV creates all the CSV file needed for migration of the data.
func (dmp Dump) CreateCSV() error {
	err := dmp.updateDataSourcesDate()
	if err != nil {
		return err
	}
	err = dmp.dumpTableDataSources()
	if err != nil {
		return err
	}
	err = dmp.dumpTableNameStrings()
	if err != nil {
		return err
	}
	err = dmp.dumpTableNameStringIndices()
	if err != nil {
		return err
	}
	err = dmp.dumpTableVernacularStrings()
	if err != nil {
		return err
	}
	err = dmp.dumpTableVernacularStringIndices()
	if err != nil {
		return err
	}

	log.Println("CSV dump is created")
	return dmp.DB.Close()
}

func (dmp Dump) csvFile(f string) (*os.File, error) {
	path := filepath.Join(dmp.DumpDir, f+".csv")
	return os.Create(path)
}

func (dmp Dump) updateDataSourcesDate() error {
	var id int
	update := `UPDATE data_sources
							SET updated_at = (
								SELECT updated_at
								  FROM name_string_indices
									  WHERE data_source_id = %d LIMIT 1
								)
							WHERE id = %d`
	q := `SELECT DISTINCT id
	        FROM data_sources ds
					  JOIN name_string_indices nsi
						  ON nsi.data_source_id = ds.id`
	rows, err := dmp.DB.Query(q)
	if err != nil {
		log.Println("updateDataSourcesDate")
		return err
	}
	for rows.Next() {
		err := rows.Scan(&id)
		if err != nil {
			log.Println("updateDataSourcesDate")
			return err
		}
		uq := fmt.Sprintf(update, id, id)
		_, err = dmp.DB.Query(uq)
		if err != nil {
			log.Println("updateDataSourcesDate")
			return err
		}
	}
	return rows.Close()
}

func (dmp Dump) dumpTableDataSources() error {
	log.Print("Create data_sources.csv")
	q1 := `SELECT data_source_id, count(*)
	          FROM name_string_indices
						  GROUP BY data_source_id`
	q2 := `SELECT id, title, description,
	 	  		logo_url, web_site_url, data_url,
	 	  		refresh_period_days, name_strings_count,
	 	  		data_hash, unique_names_count, created_at, updated_at
	 	  	FROM data_sources`
	rows, err := dmp.DB.Query(q1)
	if err != nil {
		return err
	}
	recNum, err := collectDataSourceRecords(rows)
	if err != nil {
		return err
	}
	rows, err = dmp.DB.Query(q2)
	if err != nil {
		return err
	}
	dmp.handleDataSource(rows, recNum)
	return nil
}

func collectDataSourceRecords(rows *sql.Rows) (map[int]int, error) {
	res := make(map[int]int)
	var id, recNum int
	defer func() {
		err := rows.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	for rows.Next() {
		err := rows.Scan(&id, &recNum)
		if err != nil {
			return res, err
		}
		res[id] = recNum
	}
	return res, nil
}

func (dmp Dump) handleDataSource(rows *sql.Rows, recNum map[int]int) error {
	var id int
	var title string
	var refreshPeriodDays, nameStringsCount sql.NullInt64
	var uniqueNamesCount sql.NullInt64
	var description, logoURL, webSiteURL sql.NullString
	var dataURL, dataHash sql.NullString
	var createdAt, updatedAt time.Time
	curated, autoCurated := qualityMaps()
	file, err := dmp.csvFile("data_sources")
	w := csv.NewWriter(file)

	err = w.Write([]string{"id", "title", "description",
		"logo_url", "web_site_url", "data_url",
		"refresh_period_days", "name_strings_count",
		"data_hash", "unique_names_count", "created_at",
		"updated_at", "is_curated", "is_auto_curated", "record_count"})
	if err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&id, &title, &description, &logoURL, &webSiteURL,
			&dataURL, &refreshPeriodDays, &nameStringsCount, &dataHash,
			&uniqueNamesCount, &createdAt, &updatedAt)
		if err != nil {
			return err
		}
		created := createdAt.Format(time.RFC3339)
		updated := updatedAt.Format(time.RFC3339)
		isCurated := "f"
		isAutoCurated := "f"
		if _, ok := curated[id]; ok {
			isCurated = "t"
		}
		if _, ok := autoCurated[id]; ok {
			isAutoCurated = "t"
		}
		csvRow := []string{strconv.Itoa(id), title, description.String,
			logoURL.String, webSiteURL.String, dataURL.String,
			strconv.Itoa(int(refreshPeriodDays.Int64)),
			strconv.Itoa(int(nameStringsCount.Int64)), dataHash.String,
			strconv.Itoa(int(uniqueNamesCount.Int64)),
			created, updated, isCurated, isAutoCurated,
			strconv.Itoa(recNum[id])}

		err = w.Write(csvRow)
		if err != nil {
			return err
		}
	}
	w.Flush()
	file.Sync()
	return file.Close()
}

func qualityMaps() (map[int]byte, map[int]byte) {
	curatedAry := []int{1, 2, 3, 5, 6, 9, 105, 132, 151, 155,
		163, 165, 167, 172, 173, 174, 175, 176, 177, 181, 183, 184, 185,
		187, 188, 189, 193, 195, 197, 201}
	autoCuratedAry := []int{11, 12, 158, 170, 179, 186, 194, 196}

	curated := make(map[int]byte)
	autoCurated := make(map[int]byte)

	for _, v := range curatedAry {
		curated[v] = '\x00'
	}

	for _, v := range autoCuratedAry {
		autoCurated[v] = '\x00'
	}
	return curated, autoCurated
}

func (dmp Dump) dumpTableNameStrings() error {
	log.Print("Create name_strings.csv")
	q := `SELECT id, name
					FROM name_strings`
	rows, err := dmp.DB.Query(q)
	if err != nil {
		return err
	}
	return dmp.handleNameStrings(rows)
}

func (dmp Dump) handleNameStrings(rows *sql.Rows) error {
	var id string
	var name string
	file, err := dmp.csvFile("name_strings")
	if err != nil {
		return err
	}
	defer file.Close()
	w := csv.NewWriter(file)
	err = w.Write([]string{"id", "name"})
	if err != nil {
		return err
	}

	defer rows.Close()
	var count int64
	for rows.Next() {
		count++
		if count%1_000_000 == 0 {
			fmt.Printf("\r%s", strings.Repeat(" ", 35))
			fmt.Printf("\rDownloaded %s names to a CSV file", humanize.Comma(count))
		}
		if err := rows.Scan(&id, &name); err != nil {
			return err
		}
		name := strings.Replace(name, "\u0000", "", -1)
		csvRow := []string{id, name}

		err = w.Write(csvRow)
		if err != nil {
			return err
		}
	}

	w.Flush()
	file.Sync()
	fmt.Printf("\r%s", strings.Repeat(" ", 35))
	fmt.Printf("\rDownloaded %s names to a CSV file\n", humanize.Comma(count))
	return nil
}

func (dmp Dump) dumpTableNameStringIndices() error {
	log.Print("Create name_string_indices.csv")
	q := `SELECT data_source_id, name_string_id,
					url, taxon_id, global_id, local_id,
					nomenclatural_code_id, rank,
					accepted_taxon_id, classification_path,
					classification_path_ids,
					classification_path_ranks
					FROM name_string_indices`
	rows, err := dmp.DB.Query(q)
	if err != nil {
		return err
	}
	return dmp.handleNameStringIndices(rows)
}

func (dmp Dump) handleNameStringIndices(rows *sql.Rows) error {
	var dataSourceID, nameStringID, taxonID string
	var url, globalID, localID, nomenclaturalCodeID, rank sql.NullString
	var acceptedTaxonID sql.NullString
	var classificationPath, classificationPathIDs sql.NullString
	var classificationPathRanks sql.NullString
	file, err := dmp.csvFile("name_string_indices")
	if err != nil {
		return err
	}
	defer file.Close()
	w := csv.NewWriter(file)
	err = w.Write([]string{"data_source_id", "name_string_id", "url",
		"taxon_id", "global_id", "local_id", "nomenclatural_code_id", "rank",
		"accepted_taxon_id", "classification_path", "classification_path_ids",
		"classification_path_ranks"})
	if err != nil {
		return err
	}

	defer rows.Close()
	var count int64
	for rows.Next() {
		count++
		if count%100_000 == 0 {
			fmt.Printf("\r%s", strings.Repeat(" ", 35))
			fmt.Printf("\rDownloaded %s name indices to a CSV file", humanize.Comma(count))
		}
		err := rows.Scan(&dataSourceID, &nameStringID, &url, &taxonID,
			&globalID, &localID, &nomenclaturalCodeID, &rank, &acceptedTaxonID,
			&classificationPath, &classificationPathIDs,
			&classificationPathRanks)
		if err != nil {
			return err
		}
		urlString := removeNewLines(url)
		csvRow := []string{dataSourceID, nameStringID, urlString, taxonID,
			globalID.String, localID.String, nomenclaturalCodeID.String,
			rank.String, acceptedTaxonID.String, classificationPath.String,
			classificationPathIDs.String, classificationPathRanks.String}

		if err := w.Write(csvRow); err != nil {
			return err
		}
	}
	w.Flush()
	file.Sync()
	fmt.Printf("\r%s", strings.Repeat(" ", 35))
	fmt.Printf("\rDownloaded %s name indices to a CSV file\n", humanize.Comma(count))
	return nil
}

func removeNewLines(data sql.NullString) string {
	str := data.String
	return strings.Replace(str, "\n", "", -1)
}

func (dmp Dump) dumpTableVernacularStrings() error {
	log.Print("Create vernacular_strings.csv")
	q := "SELECT id, name FROM vernacular_strings"
	rows, err := dmp.DB.Query(q)
	if err != nil {
		return err
	}
	return dmp.handleVernacularStrings(rows)
}

func (dmp Dump) handleVernacularStrings(rows *sql.Rows) error {
	var id string
	var name string
	file, err := dmp.csvFile("vernacular_strings")
	if err != nil {
		return err
	}
	defer file.Close()
	w := csv.NewWriter(file)
	if err = w.Write([]string{"id", "name"}); err != nil {
		return err
	}

	defer rows.Close()
	var count int64
	for rows.Next() {
		count++

		if err = rows.Scan(&id, &name); err != nil {
			return err
		}
		csvRow := []string{id, name}

		if err := w.Write(csvRow); err != nil {
			return err
		}
	}
	fmt.Printf("Downloaded %s vernaculars to a CSV file\n", humanize.Comma(count))
	w.Flush()
	return file.Sync()
}

func (dmp Dump) dumpTableVernacularStringIndices() error {
	log.Print("Create vernacular_string_indices.csv")
	q := `SELECT data_source_id, taxon_id,
					vernacular_string_id, language, locality,
					country_code
					FROM vernacular_string_indices`

	rows, err := dmp.DB.Query(q)
	if err != nil {
		return err
	}
	return dmp.handleVernacularStringIndices(rows)
}
func (dmp Dump) handleVernacularStringIndices(rows *sql.Rows) error {
	var dataSourceID, taxonID, vernacularStringID string
	var language, locality, countryCode sql.NullString
	file, err := dmp.csvFile("vernacular_string_indices")
	if err != nil {
		return err
	}
	defer file.Close()

	w := csv.NewWriter(file)
	err = w.Write([]string{"data_source_id", "taxon_id", "vernacular_string_id",
		"language", "locality", "country_code"})
	if err != nil {
		return err
	}

	defer rows.Close()

	var count int64
	for rows.Next() {
		count++
		if count%100_000 == 0 {
			fmt.Printf("\r%s", strings.Repeat(" ", 35))
			fmt.Printf("\rDownloaded %s verncular indices to a CSV file", humanize.Comma(count))
		}
		err := rows.Scan(&dataSourceID, &taxonID, &vernacularStringID,
			&language, &locality, &countryCode)
		if err != nil {
			return err
		}

		csvRow := []string{dataSourceID, taxonID, vernacularStringID,
			language.String, locality.String, countryCode.String}

		if err := w.Write(csvRow); err != nil {
			log.Fatal(err)
		}
	}
	w.Flush()
	fmt.Printf("\r%s", strings.Repeat(" ", 35))
	fmt.Printf("\rDownloaded %s verncular indices to a CSV file\n", humanize.Comma(count))
	return file.Sync()
}
