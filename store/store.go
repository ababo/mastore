package store

import (
	"log"
)

type Store struct {
	conf *Config
	log_ *log.Logger

	accum     map[uint16][]string
	accumSize int
	flushedOK chan bool
}

func New(conf *Config, log_ *log.Logger) *Store {
	accum := make(map[uint16][]string)
	return &Store{conf: conf, log_: log_, accum: accum}
}
