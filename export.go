package gnidump

import (
	"fmt"
	"github.com/gnames/gnidump/rebuild"
	"github.com/gnames/gnidump/sys"
	"log"
)

func (gnd GNIdump) PopulatePG() error {
	var err error
	log.Printf("Rebuilding '%s' database.\n", gnd.PgDB.PgDB)
	if err = gnd.PgDB.ResetDB(); err != nil {
		return fmt.Errorf("Reset of DB did not work: %w", err)
	}
	if err = gnd.PgDB.Migrate(); err != nil {
		return fmt.Errorf("Cannot rebuild DB schema: %w", err)
	}
	rb := rebuild.NewRebuild(gnd.PgDB, gnd.InputDir, gnd.JobsNum)
	if err = sys.MakeDir(rb.ParserKeyValDir); err != nil {
		return err
	}
	if err = rb.UploadNameString(); err != nil {
		return fmt.Errorf("Unable to populate name_strings table: %w", err)
	}
	if err = rb.UploadDataSources(); err != nil {
		return fmt.Errorf("Unable to populate data_sources table: %w", err)
	}
	rb.UploadNameStringIndices()
	return nil
}
