package store

import (
	"log"
)

type Store struct {
	conf *Config
	log_ *log.Logger

	accum     []string
	accumSize int
	flushedOK chan bool
}

func New(conf *Config, log_ *log.Logger) *Store {
	return &Store{conf: conf, log_: log_}
}
