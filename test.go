package main

import (
	"log"
	"mastore/store"
	"math/rand"
	"time"
)

const testKeys = 10000000
const logValuesCount = 1000000

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

func doTest(log_ *log.Logger, st *store.Store, keys int, values int) bool {
	rand.Seed(time.Now().UTC().UnixNano())

	log.Printf("started to generate %d random keys", keys)
	var keys_ []string
	for i := 0; i < keys; i++ {
		keys_ = append(keys_, randomString(rand.Int()%32+1))
	}

	log.Printf("started to insert %d values", values)
	for i, size := 0, 0; i < values; i++ {
		checkInterrupted(st)

		key := keys_[normIndex(keys)]
		val := randomString(rand.Int()%64 + 1)
		if !st.AddValue(key, val) {
			return false
		}

		size += len(key) + len(val) + 2
		if i != 0 && i%logValuesCount == 0 {
			log_.Printf("added %d values (%d bytes)", i, size)
		}
	}

	return st.Flush(true)
}
