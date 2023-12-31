package kv

import "github.com/dgraph-io/badger/v2"

// KeyVal is a key-value store.
type KeyVal interface {
	// Open opens a key-value store.
	Open() error

	// Close closes a key-value store.
	Close() error

	// GetTransaction returns a transaction object.
	GetTransaction() (*badger.Txn, error)

	// Set sets a key-value pair.
	GetValue(key []byte) ([]byte, error)
}
