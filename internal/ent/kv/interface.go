package kv

import "github.com/dgraph-io/badger/v2"

// KeyVal is a key-value store.
type KeyVal interface {
	DB() *badger.DB
}
