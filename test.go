package main

import (
	"log"
	"mastore/store"
	"math/rand"
	"time"
)

const testKeys = 10000000

func randomString(strlen int) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	result := make([]byte, strlen)
	for i := 0; i < strlen; i++ {
		result[i] = chars[rand.Intn(len(chars))]
	}
	return string(result)
}

func normIndex(size int) int {
	i := size/2 + int(rand.NormFloat64()*float64(size/4))
	if i < 0 {
		i = 0
	} else if i >= size {
		i = size - 1
	}
	return i
}

func doTest(log_ *log.Logger, st *store.Store, keynum int, entries int) bool {
	rand.Seed(time.Now().UTC().UnixNano())

	log.Printf("started to generate %d random keys", keynum)
	var keys []string
	for i := 0; i < keynum; i++ {
		keys = append(keys, randomString(rand.Int()%32+1))
	}

	log.Printf("started to insert %d entries", entries)
	for i := 0; i < entries; i++ {
		checkInterrupted(st)

		key := keys[normIndex(len(keys))]
		entry := randomString(rand.Int()%64 + 1)
		if !st.AddEntry(key, entry) {
			return false
		}
	}

	return st.Flush(true)
}
