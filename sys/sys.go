// sys package contains helper functions related ot underlying OS
package sys

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// MakeDir creates a directory with subdirectories recursively.
func MakeDir(dir string) error {
	path, err := os.Stat(dir)
	if os.IsNotExist(err) {
		return os.MkdirAll(dir, 0755)
	}
	if path.Mode().IsRegular() {
		return fmt.Errorf("'%s' is a file, not a directory", dir)
	}
	return nil
}

// FileExists checks if a file exists in a filesystem and that it is really
// a file.
func FileExists(f string) bool {
	path, err := os.Stat(f)
	if os.IsNotExist(err) {
		return false
	}
	if !path.Mode().IsRegular() {
		log.Fatal(fmt.Errorf("'%s' is not a regular file", f))
	}
	return true
}

// CleanDir removes all files from a directory.
func CleanDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()

	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	return nil
}
