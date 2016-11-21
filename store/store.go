package store

import (
	"log"
	"os"
	"os/signal"
)

type Store struct {
	conf *Config
	log_ *log.Logger

	accum     []string
	accumSize int
	flushedOK chan bool
}

func New(conf *Config, log_ *log.Logger) *Store {
	st := &Store{conf: conf, log_: log_}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	go func() {
		<-ch
		if st.Flush(true) {
			os.Exit(0)
		} else {
			os.Exit(1)
		}
	}()

	return st
}
