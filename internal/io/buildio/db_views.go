package buildio

import (
	"context"
	"log/slog"
)

func (b *buildio) removeOrphans() error {
	var err error
	ctx := context.Background()
	slog.Info("Removing orphan name-strings")
	q := `DELETE FROM name_strings
  WHERE id IN (
    SELECT ns.id
      FROM name_strings ns
        LEFT OUTER JOIN name_string_indices nsi
          ON ns.id = nsi.name_string_id
      WHERE nsi.name_string_id IS NULL
    )`

	_, err = b.db.Exec(ctx, q)
	if err != nil {
		slog.Error("Cannot remove orphan name-strings", "error", err)
		return err
	}

	slog.Info("Removing orphan canonicals")
	q = `DELETE FROM canonicals
  WHERE id IN (
    SELECT c.id
      FROM canonicals  c
        LEFT OUTER JOIN name_strings ns
          ON c.id = ns.canonical_id
      WHERE ns.id IS NULL
    )`

	_, err = b.db.Exec(ctx, q)
	if err != nil {
		slog.Error("Cannot remove orphan canonicals", "error", err)
		return err
	}

	slog.Info("Removing orphan canonical_fulls")
	q = `DELETE FROM canonical_fulls
  WHERE id IN (
    SELECT cf.id
      FROM canonical_fulls  cf
        LEFT OUTER JOIN name_strings ns
          ON cf.id = ns.canonical_full_id
      WHERE ns.id IS NULL
    )`

	_, err = b.db.Exec(ctx, q)
	if err != nil {
		slog.Error("Cannot remove orphan canonical_fulls", "error", err)
		return err
	}

	slog.Info("Removing orphan canonical_stems")
	q = `DELETE FROM canonical_stems
    WHERE id IN (
      SELECT cs.id
        FROM canonical_stems  cs
          LEFT OUTER JOIN name_strings ns
            ON cs.id = ns.canonical_stem_id
        WHERE ns.id IS NULL
      )`
	_, err = b.db.Exec(ctx, q)
	if err != nil {
		slog.Error("Cannot remove orphan canonical_stems", "error", err)
		return err
	}
	return nil
}

// verificationView creates data for a materialized view.
func (b *buildio) createVerification() error {
	var err error
	ctx := context.Background()

	// Drop the view if it exists
	_, err = b.db.Exec(ctx, "DROP MATERIALIZED VIEW IF EXISTS verification")
	if err != nil {
		slog.Error("Cannot drop view", "error", err)
		return err
	}

	slog.Info("Building verification view, it will take some time...")
	viewQuery := `CREATE MATERIALIZED VIEW verification AS
WITH taxon_names AS (
SELECT nsi.data_source_id, nsi.record_id, nsi.name_string_id, ns.name
  FROM name_string_indices nsi
    JOIN name_strings ns
      ON nsi.name_string_id = ns.id
)
SELECT nsi.data_source_id, nsi.record_id, nsi.name_string_id,
  ns.name, ns.year, ns.cardinality, ns.canonical_id, ns.virus, ns.bacteria,
  ns.parse_quality, nsi.local_id, nsi.outlink_id, nsi.accepted_record_id,
  tn.name_string_id as accepted_name_id,
  tn.name as accepted_name, nsi.classification, nsi.classification_ranks,
  nsi.classification_ids
  FROM name_string_indices nsi
    JOIN name_strings ns ON ns.id = nsi.name_string_id
    LEFT JOIN taxon_names tn
      ON nsi.data_source_id = tn.data_source_id AND
         nsi.accepted_record_id = tn.record_id
  WHERE
    (
      ns.canonical_id is not NULL AND
      surrogate != TRUE AND
      (bacteria != TRUE OR parse_quality < 3)
    ) OR ns.virus = TRUE`
	_, err = b.db.Exec(ctx, "DROP MATERIALIZED VIEW IF EXISTS verification")
	if err != nil {
		slog.Error("Cannot drop verification view", "error", err)
		return err
	}
	_, err = b.db.Exec(ctx, viewQuery)
	if err != nil {
		slog.Error("Cannot run verification create", "error", err)
		return err
	}
	slog.Info("Building indices for verification view, it will take some time...")
	_, err = b.db.Exec(ctx, "CREATE INDEX ON verification (canonical_id)")
	if err != nil {
		slog.Error("Cannot create verification index", "error", err)
		return err
	}
	_, err = b.db.Exec(ctx, "CREATE INDEX ON verification (name_string_id)")
	if err != nil {
		slog.Error("Cannot create verification index2", "error", err)
		return err
	}
	_, err = b.db.Exec(ctx, "CREATE INDEX ON verification (year)")
	if err != nil {
		slog.Error("Cannot create verification index3", "error", err)
		return err
	}
	slog.Info("View verification is created")
	return nil
}
