package model

type Model interface {
	Migrate() error
}
