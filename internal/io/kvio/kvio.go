package kvio

import (
	"log/slog"
	"os"

	"github.com/dgraph-io/badger/v2"
	"github.com/gnames/gnidump/internal/ent/kv"
	"github.com/gnames/gnsys"
)

type kvio struct {
	dir string
	kv  *badger.DB
}

// New returns a new instance of kvio.
func New(dir string) kv.KeyVal {
	res := kvio{
		dir: dir,
	}
	err := gnsys.MakeDir(dir)
	if err != nil {
		slog.Error("Cannot create directory", "error", err, "dir", dir)
		os.Exit(1)
	}

	err = gnsys.CleanDir(dir)
	if err != nil {
		slog.Error("Cannot reset  KeyValue", "error", err, "dir", dir)
		os.Exit(1)
	}

	options := badger.DefaultOptions(dir)
	options.Logger = nil

	bdb, err := badger.Open(options)
	if err != nil {
		slog.Error("Cannot init the key/value store", "error", err)
		os.Exit(1)
	}
	res.kv = bdb

	return &res
}

func (k *kvio) DB() *badger.DB {
	return k.kv
}
