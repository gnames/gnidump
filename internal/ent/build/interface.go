package build

// Builder is the interface that wraps the Build method.
type Builder interface {
	// Build builds the data from CSV to PostgreSQL.
	Build() error
}
