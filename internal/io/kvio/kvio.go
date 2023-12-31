package kvio

import (
	"errors"
	"log/slog"

	"github.com/dgraph-io/badger/v2"
	"github.com/gnames/gnidump/internal/ent/kv"
	"github.com/gnames/gnsys"
)

type kvio struct {
	dir string
	kv  *badger.DB
}

// New returns a new instance of kvio.
func New(dir string) (kv.KeyVal, error) {
	res := kvio{
		dir: dir,
	}

	err := gnsys.MakeDir(dir)
	if err != nil {
		slog.Error("Cannot create directory", "error", err, "dir", dir)
		return nil, err
	}

	err = gnsys.CleanDir(dir)
	if err != nil {
		slog.Error("Cannot reset  KeyValue", "error", err, "dir", dir)
		return nil, err
	}

	return &res, err
}

// Open opens a key-value store.
func (k *kvio) Open() error {
	if k.kv != nil {
		slog.Warn("key-value store is not nil")
	}
	options := badger.DefaultOptions(k.dir)
	options.Logger = nil

	bdb, err := badger.Open(options)
	if err != nil {
		return err
	}
	k.kv = bdb
	return nil
}

// Close closes a key-value store.
func (k *kvio) Close() error {
	if k.kv == nil {
		slog.Warn("key-value store is nil")
		return nil
	}
	err := k.kv.Close()
	k.kv = nil
	return err
}

// GetTransaction returns a transaction object.
func (k *kvio) GetTransaction() (*badger.Txn, error) {
	if k.kv == nil {
		err := errors.New("key-value store is not open")
		return nil, err
	}
	trx := k.kv.NewTransaction(true)
	return trx, nil
}

// GetValue returns a value for a given key.
func (k *kvio) GetValue(key []byte) ([]byte, error) {
	txn := k.kv.NewTransaction(false)
	defer txn.Commit()
	val, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		slog.Warn("Cannot find key", "key", key)
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	var res []byte
	return val.ValueCopy(res)
}
