package store

import (
	logpkg "log"
)

// Store data.
type Store struct {
	conf *Config
	log  *logpkg.Logger

	accum     map[uint16][]string
	accumSize int
	flushedOK chan bool
}

// Create a new store.
func New(conf *Config, log *logpkg.Logger) *Store {
	accum := make(map[uint16][]string)
	return &Store{conf: conf, log: log, accum: accum}
}
