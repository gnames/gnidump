package gnidump

import (
	"github.com/gnames/gnidump/rebuild"
)

func (gnd GNIdump) PopulatePG() error {
	// var err error
	// log.Printf("Rebuilding '%s' database.\n", gnd.PgDB.PgDB)
	// if err = gnd.ResetDB(); err != nil {
	// 	return fmt.Errorf("reset of DB did not work: %w", err)
	// }
	// if err = gnd.Migrate(); err != nil {
	// 	return fmt.Errorf("cannot rebuild DB schema: %w", err)
	// }
	rb := rebuild.NewRebuild(gnd.PgDB, gnd.InputDir, gnd.JobsNum)
	// if err = gnsys.MakeDir(rb.ParserKeyValDir); err != nil {
	// 	return err
	// }
	// if err = rb.UploadNameString(); err != nil {
	// 	return fmt.Errorf("unable to populate name_strings table: %w", err)
	// }
	// if err = rb.UploadDataSources(); err != nil {
	// 	return fmt.Errorf("unable to populate data_sources table: %w", err)
	// }
	//
	// rb.UploadNameStringIndices()
	// rb.RemoveOrphans()
	// rb.CreateWords()
	rb.VerificationView()
	return nil
}
