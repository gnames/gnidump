package keyval

import (
	"log/slog"
	"os"

	"github.com/dgraph-io/badger/v2"
	"github.com/gnames/gnsys"
)

// InitKeyVal --- InitBadger finds and initializes connection to a badger
// key-value store. If the store does not exist, InitBadger creates it.
func InitKeyVal(dir string) *badger.DB {
	options := badger.DefaultOptions(dir)
	options.Logger = nil
	bdb, err := badger.Open(options)
	if err != nil {
		slog.Error("Cannot init the key/value store", "error", err)
		os.Exit(1)
	}
	return bdb
}

// GetValue gets a value for a key in a key-value store.
func GetValue(kv *badger.DB, key string) []byte {
	txn := kv.NewTransaction(false)
	defer func() {
		err := txn.Commit()
		if err != nil {
			slog.Error("Cannot commit key/value transaction", "error", err)
			os.Exit(1)
		}
	}()
	val, err := txn.Get([]byte(key))
	if err == badger.ErrKeyNotFound {
		slog.Warn("Cannot find key", "key", key)
		return nil
	} else if err != nil {
		slog.Error("Cannot get value from key/value store", "error", err)
		os.Exit(1)
	}
	var res []byte
	res, err = val.ValueCopy(res)
	if err != nil {
		slog.Error("Cannot copy value", "error", err)
		os.Exit(1)
	}
	return res
}

// ResetKeyVal cleans key-value store from old data.
func ResetKeyVal(dir string) error {
	return gnsys.CleanDir(dir)
}
