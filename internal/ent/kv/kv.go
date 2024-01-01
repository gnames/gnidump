package kv

// Record is a key-value pair to be stored in KV store.
type Record struct {
	// Key is the key of the record.
	Key []byte

	// Value is the value of the record.
	Value []byte
}
