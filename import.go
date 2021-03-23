package gnidump

import (
	"log"

	"github.com/gnames/gnidump/dump"
	"github.com/gnames/gnsys"
)

func (gnd GNIdump) CSVdump() error {
	var err error
	log.Println("Dumping data from GNI to CSV files.")
	dmp := dump.NewDump(gnd.MyDB, gnd.InputDir, gnd.JobsNum)
	err = gnsys.MakeDir(dmp.DumpDir)
	if err != nil {
		return err
	}
	return dmp.CreateCSV()
}
