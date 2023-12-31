package config

import (
	"os"
	"path/filepath"
)

var (
	curatedAry = []int{1, 2, 3, 5, 6, 9, 105, 132, 151, 155,
		163, 165, 167, 172, 173, 174, 175, 176, 177, 181, 183, 184, 185,
		187, 188, 189, 193, 195, 197, 201, 203, 204, 205, 208, 209}
	autoCuratedAry = []int{11, 12, 158, 170, 179, 186, 194, 196, 206, 207}
)

type Config struct {
	InputDir    string
	DumpDir     string
	JobsNum     int
	MyHost      string
	MyUser      string
	MyPass      string
	MyDB        string
	PgHost      string
	PgUser      string
	PgPass      string
	PgDB        string
	Curated     []int
	AutoCurated []int
}

// Option type allows to change settings for Config.
type Option func(*Config)

// OptInputDir sets a directory for temporary files and key-value stores.
func OptInputDir(d string) Option {
	return func(cfg *Config) {
		cfg.InputDir = d
	}
}

// OptJobsNum sets parallelism number for concurrent goroutines.
func OptJobsNum(j int) Option {
	return func(cfg *Config) {
		cfg.JobsNum = j
	}
}

// OptMyHost sets host for MySQL
func OptMyHost(h string) Option {
	return func(cfg *Config) {
		cfg.MyHost = h
	}
}

// OptMyUser sets user for MySQL
func OptMyUser(u string) Option {
	return func(cfg *Config) {
		cfg.MyUser = u
	}
}

// OptMyPass sets password for MySQL
func OptMyPass(p string) Option {
	return func(cfg *Config) {
		cfg.MyPass = p
	}
}

// OptMyDB sets database name for MySQL
func OptMyDB(d string) Option {
	return func(cfg *Config) {
		cfg.MyDB = d
	}
}

// OptPgHost sets host name for PostgreSQL
func OptPgHost(h string) Option {
	return func(cfg *Config) {
		cfg.PgHost = h
	}
}

// OptPgUser sets user for PostgreSQL
func OptPgUser(u string) Option {
	return func(cfg *Config) {
		cfg.PgUser = u
	}
}

// OptPgPass sets password for PostgreSQL
func OptPgPass(p string) Option {
	return func(cfg *Config) {
		cfg.PgPass = p
	}
}

// OptPgDB sets database name for PostgreSQL
func OptPgDB(d string) Option {
	return func(cfg *Config) {
		cfg.PgDB = d
	}
}

func New(opts ...Option) Config {
	inpDir, err := os.UserCacheDir()
	if err != nil {
		inpDir = os.TempDir()
	}
	inpDir = filepath.Join(inpDir, "gnidump")

	res := Config{
		InputDir:    inpDir,
		DumpDir:     filepath.Join(inpDir, "gni-dump"),
		JobsNum:     4,
		MyDB:        "gni",
		PgHost:      "0.0.0.0",
		PgUser:      "postgres",
		PgPass:      "postgres",
		PgDB:        "gnames",
		Curated:     curatedAry,
		AutoCurated: autoCuratedAry,
	}

	for _, opt := range opts {
		opt(&res)
	}

	return res
}
