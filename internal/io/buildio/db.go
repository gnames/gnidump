package buildio

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/gnames/gnidump/internal/ent/model"
	"github.com/gnames/gnidump/internal/str"
	"github.com/jackc/pgx/v5"
)

// resetDB resets the database to a clean state.
func (b *buildio) resetDB() error {
	var err error
	var rows pgx.Rows
	slog.Info("Resetting database")
	qs := []string{
		"DROP SCHEMA IF EXISTS public CASCADE",
		"CREATE SCHEMA public",
		"GRANT ALL ON SCHEMA public TO postgres",
		fmt.Sprintf("GRANT ALL ON SCHEMA public TO %s", b.cfg.PgUser),
		"COMMENT ON SCHEMA public IS 'standard public schema'",
	}
	for i := range qs {
		rows, err = b.db.Query(context.Background(), qs[i])
		if err != nil {
			slog.Error("Cannot reset database", "error", err, "query", qs[i])
			return err
		}
		rows.Close()
	}

	slog.Info("Database did reset successfully")
	return nil
}

func (b *buildio) truncateTable(tbls ...string) error {
	var err error
	for _, tbl := range tbls {
		_, err = b.db.Exec(context.Background(), "TRUNCATE TABLE "+tbl)
		if err != nil {
			slog.Error("cannot truncate table", "table", tbl, "error", err)
			return err
		}
	}
	return nil
}

func (b *buildio) insertRows(tbl string, columns []string, rows [][]any) (int64, error) {
	copyCount, err := b.db.CopyFrom(
		context.Background(),
		pgx.Identifier{tbl},
		columns,
		pgx.CopyFromRows(rows),
	)

	return int64(copyCount), err
}

func (b *buildio) saveNameStrings(ns []model.NameString) (int64, error) {
	columns := []string{
		"id", "name", "year", "cardinality", "canonical_id",
		"canonical_full_id", "canonical_stem_id", "virus",
		"bacteria", "surrogate", "parse_quality"}
	rows := make([][]any, len(ns))
	for i, n := range ns {
		rows[i] = []any{
			n.ID, n.Name, n.Year, n.Cardinality,
			n.CanonicalID, n.CanonicalFullID, n.CanonicalStemID,
			n.Virus, n.Bacteria, n.Surrogate, n.ParseQuality,
		}
	}
	return b.insertRows("name_strings", columns, rows)
}

func (b *buildio) saveCanonicals(cs []canonicalData) error {
	var err error
	var rows pgx.Rows
	cal := make([]string, len(cs))
	calFull := make([]string, 0, len(cs))
	calStem := make([]string, 0, len(cs))
	for i, v := range cs {
		cal[i] = fmt.Sprintf("('%s', %s)", v.ID, str.QuoteString(v.Value))

		if v.FullID != "" {
			calFull = append(calFull,
				fmt.Sprintf("('%s', %s)", v.FullID, str.QuoteString(v.FullValue)))
		}
		if v.StemID != "" {
			calStem = append(calStem,
				fmt.Sprintf("('%s', %s)", v.StemID, str.QuoteString(v.StemValue)))
		}
	}

	q0 := `INSERT INTO %s (id, name) VALUES %s ON CONFLICT DO NOTHING`
	q := fmt.Sprintf(q0, "canonicals", strings.Join(cal, ","))
	if rows, err = b.db.Query(context.Background(), q); err != nil {
		slog.Error("save canonicals failed", "error", err)
		return err
	}
	rows.Close()

	if len(calFull) > 0 {
		q = fmt.Sprintf(q0, "canonical_fulls", strings.Join(calFull, ","))
		if rows, err = b.db.Query(context.Background(), q); err != nil {
			slog.Error("save canonical_fulls failed", "error", err)
			return err
		}
		rows.Close()
	}
	if len(calStem) > 0 {
		q = fmt.Sprintf(q0, "canonical_stems", strings.Join(calStem, ","))
		if rows, err = b.db.Query(context.Background(), q); err != nil {
			slog.Error("save canonical_stems failed", "error", err)
			return err
		}
		rows.Close()
	}
	return nil
}

func (b *buildio) saveNameStringIndices(
	nsi []model.NameStringIndex,
) (int64, error) {
	columns := []string{
		"data_source_id", "name_string_id", "record_id",
		"local_id", "global_id", "outlink_id", "code_id", "rank",
		"accepted_record_id", "classification", "classification_ids",
		"classification_ranks"}
	rows := make([][]any, len(nsi))
	for i := range nsi {
		row := []any{
			nsi[i].DataSourceID, nsi[i].NameStringID, nsi[i].RecordID,
			nsi[i].LocalID, nsi[i].GlobalID, nsi[i].OutlinkID,
			nsi[i].CodeID, nsi[i].Rank, nsi[i].AcceptedRecordID,
			nsi[i].Classification, nsi[i].ClassificationIDs,
			nsi[i].ClassificationRanks,
		}
		rows[i] = row
	}

	return b.insertRows("name_string_indices", columns, rows)
}

func (b *buildio) saveVernStrings(vs []model.VernacularString) (int64, error) {
	columns := []string{"id", "name"}
	rows := make([][]any, len(vs))
	for i, v := range vs {
		rows[i] = []any{v.ID, v.Name}
	}

	return b.insertRows("vernacular_strings", columns, rows)
}

func (b *buildio) saveVernStringIndices(nsi []model.VernacularStringIndex) (int64, error) {
	columns := []string{"data_source_id", "vernacular_string_id", "record_id",
		"language", "lang_code", "locality", "country_code"}
	rows := make([][]any, len(nsi))
	for i, v := range nsi {
		row := []any{
			v.DataSourceID, v.VernacularStringID, v.RecordID, v.Language,
			v.LangCode, v.Locality, v.CountryCode,
		}
		rows[i] = row
	}
	return b.insertRows("vernacular_string_indices", columns, rows)
}

func (b *buildio) getWordNames() (pgx.Rows, error) {
	var err error
	ctx := context.Background()
	q := "SELECT name FROM name_strings"
	rows, err := b.db.Query(ctx, q)
	if err != nil {
		slog.Error("Cannot get names from db", "error", err)
		return rows, err
	}
	return rows, nil
}

func (b *buildio) saveNameWords(wns []model.WordNameString) error {
	var err error
	wns = uniqWordNameString(wns)
	columns := []string{"word_id", "name_string_id", "canonical_id"}
	rows := make([][]any, len(wns))
	for i, v := range wns {
		row := []any{v.WordID, v.NameStringID, v.CanonicalID}
		rows[i] = row
	}
	_, err = b.insertRows("word_name_strings", columns, rows)
	return err
}

func (b *buildio) saveWords(ws []model.Word) error {
	columns := []string{"id", "normalized", "modified", "type_id"}
	rows := make([][]any, len(ws))
	for i, v := range ws {
		row := []any{v.ID, v.Normalized, v.Modified, v.TypeID}
		rows[i] = row
	}

	_, err := b.insertRows("words", columns, rows)
	return err
}
