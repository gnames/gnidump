package gnidump

import (
	"log"

	"github.com/gnames/gnidump/dump"
	"github.com/gnames/gnlib/sys"
)

func (gnd GNIdump) CSVdump() error {
	var err error
	log.Println("Dumping data from GNI to CSV files.")
	dmp := dump.NewDump(gnd.MyDB, gnd.InputDir, gnd.JobsNum)
	err = sys.MakeDir(dmp.DumpDir)
	if err != nil {
		return err
	}
	return dmp.CreateCSV()
}
