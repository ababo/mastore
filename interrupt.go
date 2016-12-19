package main

import (
	"github.com/ababo/mastore/store"
	"os"
	"os/signal"
)

var interrupted = false

func setInterruptHandler() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	go func() {
		for {
			<-ch
			if interrupted {
				os.Exit(1)
			}
			interrupted = true
		}
	}()
}

func checkInterrupted(st *store.Store) {
	if interrupted {
		if st != nil {
			st.Flush(true)
		}
		os.Exit(1)
	}
}
