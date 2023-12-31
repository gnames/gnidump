package buildio

import (
	"github.com/gnames/gnidump/internal/ent/build"
	"github.com/gnames/gnidump/pkg/config"
)

// buildio is a struct that implements build.Builder interface.
type buildio struct {
	cfg config.Config
}

// New returns a new instance of Builder
func New(cfg config.Config) build.Builder {
	res := buildio{cfg: cfg}
	return res
}

// Build creates a new PostgreSQL database from CSV dump files.
func (b buildio) Build() error {
	return nil
}
