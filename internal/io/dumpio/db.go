package dumpio

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
)

// NewDb creates a handler for interaction with MySQL database.
func (d *dumpio) initDb() (*sql.DB, error) {
	db, err := sql.Open("mysql", d.dbURI())
	if err != nil {
		slog.Error("Cannot connect to database", "error", err)
		return nil, err
	}
	return db, nil
}

func (d *dumpio) dbURI() string {
	url := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		d.cfg.MyUser, d.cfg.MyPass, d.cfg.MyHost, 3306, d.cfg.MyDB)
	return url
}

func (d *dumpio) updateDataSourcesDate() error {
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
	rows, err := d.db.Query(q)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&id)
		if err != nil {
			return err
		}
		uq := fmt.Sprintf(update, id, id)
		rows, err = d.db.Query(uq)
		if err != nil {
			return err
		}
		defer rows.Close()
	}
	return nil
}

func (d *dumpio) dumpTableDataSources() error {
	slog.Info("Create data_sources.csv")
	q1 := `SELECT data_source_id, count(*)
	          FROM name_string_indices
						  GROUP BY data_source_id`
	q2 := `SELECT id, title, description,
	 	  		logo_url, web_site_url, data_url,
	 	  		refresh_period_days, name_strings_count,
	 	  		data_hash, unique_names_count, created_at, updated_at
	 	  	FROM data_sources`
	rows, err := d.db.Query(q1)
	if err != nil {
		return err
	}
	defer rows.Close()
	recNum, err := collectDataSourceRecords(rows)
	if err != nil {
		return err
	}
	rows, err = d.db.Query(q2)
	if err != nil {
		return err
	}
	defer rows.Close()
	d.handleDataSource(rows, recNum)
	return nil
}

func collectDataSourceRecords(rows *sql.Rows) (map[int]int, error) {
	res := make(map[int]int)
	var id, recNum int
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&id, &recNum)
		if err != nil {
			return res, err
		}
		res[id] = recNum
	}
	return res, nil
}

func (d *dumpio) handleDataSource(rows *sql.Rows, recNum map[int]int) error {
	var id int
	var title string
	var refreshPeriodDays, nameStringsCount sql.NullInt64
	var uniqueNamesCount sql.NullInt64
	var description, logoURL, webSiteURL sql.NullString
	var dataURL, dataHash sql.NullString
	var createdAt, updatedAt time.Time
	curated, autoCurated := d.qualityMaps()
	file, err := d.csvFile("data_sources")
	if err != nil {
		return err
	}
	defer file.Close()
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
	return nil
}

func (d *dumpio) qualityMaps() (curated, autoCurated map[int]struct{}) {
	curated = make(map[int]struct{})
	autoCurated = make(map[int]struct{})

	for _, v := range d.cfg.Curated {
		curated[v] = struct{}{}
	}

	for _, v := range d.cfg.AutoCurated {
		autoCurated[v] = struct{}{}
	}
	return curated, autoCurated
}

func (d dumpio) dumpTableNameStrings() error {
	slog.Info("Create name_strings.csv")
	q := `SELECT id, name
					FROM name_strings`
	rows, err := d.db.Query(q)
	if err != nil {
		return err
	}
	defer rows.Close()

	err = d.handleNameStrings(rows)
	if err != nil {
		return err
	}
	return nil
}

func (d dumpio) handleNameStrings(rows *sql.Rows) error {
	var id string
	var name string
	file, err := d.csvFile("name_strings")
	if err != nil {
		return err
	}
	defer file.Close()

	w := csv.NewWriter(file)
	err = w.Write([]string{"id", "name"})
	if err != nil {
		return err
	}

	var count int64
	for rows.Next() {
		count++
		if count%1_000_000 == 0 {
			fmt.Printf("\r%s", strings.Repeat(" ", 35))
			fmt.Printf("\rDownloaded %s names to a CSV file", humanize.Comma(count))
		}
		if err = rows.Scan(&id, &name); err != nil {
			return err
		}
		name = strings.ReplaceAll(name, "\u0000", "")
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

func (d *dumpio) dumpTableNameStringIndices() error {
	slog.Info("Create name_string_indices.csv")
	q := `SELECT data_source_id, name_string_id,
					url, taxon_id, global_id, local_id,
					nomenclatural_code_id, rank,
					accepted_taxon_id, classification_path,
					classification_path_ids,
					classification_path_ranks
					FROM name_string_indices`
	rows, err := d.db.Query(q)
	if err != nil {
		return err
	}
	defer rows.Close()
	err = d.handleNameStringIndices(rows)
	if err != nil {
		return err
	}

	return nil
}

func (d *dumpio) handleNameStringIndices(rows *sql.Rows) error {
	var dataSourceID, nameStringID, taxonID string
	var url, globalID, localID, nomenclaturalCodeID, rank sql.NullString
	var acceptedTaxonID sql.NullString
	var classificationPath, classificationPathIDs sql.NullString
	var classificationPathRanks sql.NullString
	file, err := d.csvFile("name_string_indices")
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
	fmt.Printf(
		"\rDownloaded %s name indices to a CSV file\n",
		humanize.Comma(count),
	)
	return nil
}

func removeNewLines(data sql.NullString) string {
	str := data.String
	return strings.ReplaceAll(str, "\n", "")
}

func (d *dumpio) dumpTableVernacularStrings() error {
	slog.Info("Create vernacular_strings.csv")
	q := "SELECT id, name FROM vernacular_strings"
	rows, err := d.db.Query(q)
	if err != nil {
		return err
	}
	defer rows.Close()
	err = d.handleVernacularStrings(rows)
	if err != nil {
		return err
	}
	return nil
}

func (d *dumpio) handleVernacularStrings(rows *sql.Rows) error {
	var id string
	var name string
	file, err := d.csvFile("vernacular_strings")
	if err != nil {
		return err
	}
	defer file.Close()
	w := csv.NewWriter(file)
	if err = w.Write([]string{"id", "name"}); err != nil {
		return err
	}

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

func (d *dumpio) dumpTableVernacularStringIndices() error {
	slog.Info("Create vernacular_string_indices.csv")
	q := `SELECT data_source_id, taxon_id,
					vernacular_string_id, language, locality,
					country_code
					FROM vernacular_string_indices`

	rows, err := d.db.Query(q)
	if err != nil {
		return err
	}
	defer rows.Close()
	err = d.handleVernacularStringIndices(rows)
	if err != nil {
		return err
	}

	return nil
}

func (d *dumpio) handleVernacularStringIndices(rows *sql.Rows) error {
	var dataSourceID, taxonID, vernacularStringID string
	var language, locality, countryCode sql.NullString
	file, err := d.csvFile("vernacular_string_indices")
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
			slog.Error("Cannot write to CSV file", "error", err)
			return err
		}
	}
	w.Flush()
	fmt.Printf("\r%s", strings.Repeat(" ", 35))
	fmt.Printf("\rDownloaded %s verncular indices to a CSV file\n", humanize.Comma(count))
	return file.Sync()
}
