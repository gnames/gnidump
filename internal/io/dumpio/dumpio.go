package dumpio

import (
	"github.com/gnames/gnidump/internal/ent/dump"
	"github.com/gnames/gnidump/pkg/config"
)

type dumpio struct {
	cfg config.Config
}

func New(cfg config.Config) dump.Dumper {
	res := dumpio{cfg: cfg}
	return &res
}

func (d *dumpio) Dump() error {
	return nil
}
