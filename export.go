package gnidump

import (
	"log"

	"github.com/gnames/gnidump/rebuild"
	"github.com/gnames/gnidump/sys"
)

func (gnd GNIdump) PopulatePG() error {
	var err error
	log.Printf("Rebuilding '%s' database.\n", gnd.PgDB.PgDB)
	if err = gnd.PgDB.ResetDB(); err != nil {
		return err
	}
	if err = gnd.PgDB.Migrate(); err != nil {
		return err
	}
	rb := rebuild.NewRebuild(gnd.PgDB, gnd.InputDir, gnd.JobsNum)
	if err = sys.MakeDir(rb.KeyValDir); err != nil {
		return err
	}
	if err = rb.UploadNameString(); err != nil {
		return err
	}
	if err = rb.UploadDataSources(); err != nil {
		return err
	}
	rb.UploadNameStringIndices()
	return nil
}
