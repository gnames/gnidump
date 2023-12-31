package dump

// Dumper is the interface that wraps the Dump method.
type Dumper interface {
	// Dump dumps the data from MySQL to CSV
	Dump() error
}
