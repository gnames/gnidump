package gnidump

import (
	"fmt"
	"log/slog"

	"github.com/gnames/gnidump/rebuild"
	"github.com/gnames/gnsys"
)

func (gnd GNIdump) PopulatePG() error {
	var err error
	slog.Info("Rebuilding database", "database", gnd.PgDB.PgDB)
	if err = gnd.ResetDB(); err != nil {
		return fmt.Errorf("reset of DB did not work: %w", err)
	}
	if err = gnd.Migrate(); err != nil {
		return fmt.Errorf("cannot rebuild DB schema: %w", err)
	}
	rb := rebuild.NewRebuild(gnd.PgDB, gnd.InputDir, gnd.JobsNum)
	if err = gnsys.MakeDir(rb.ParserKeyValDir); err != nil {
		return err
	}
	if err = gnsys.MakeDir(rb.VernKeyValDir); err != nil {
		return err
	}
	if err = rb.UploadNameStrings(); err != nil {
		return fmt.Errorf("unable to populate name_strings table: %w", err)
	}
	if err = rb.UploadDataSources(); err != nil {
		return fmt.Errorf("unable to populate data_sources table: %w", err)
	}

	rb.UploadNameStringIndices()

	if err = rb.UploadVernacularStrings(); err != nil {
		return fmt.Errorf("unable to populate vernacular_strings table: %w", err)
	}

	rb.UploadVernStringIndices()

	rb.RemoveOrphans()
	rb.CreateWords()
	rb.VerificationView()
	return nil
}
