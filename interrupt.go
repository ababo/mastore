package main

import (
	"mastore/store"
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
		st.Flush(true)
		os.Exit(1)
	}
}
