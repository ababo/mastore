package store

import (
	"bufio"
	"compress/gzip"
	"io/ioutil"
	"os"
	"path/filepath"
)

type scanIndexFunc func(s *Store, name string, a ...interface{}) (bool, bool)

func (s *Store) scanIndex(sectionPath string,
	cb scanIndexFunc, a ...interface{}) bool {
	info, err := os.Stat(sectionPath)
	if os.IsNotExist(err) || (err == nil && !info.IsDir()) {
		return true
	}

	files, err := ioutil.ReadDir(sectionPath)
	if err != nil {
		s.log.Printf("failed to read index directory: %s", err)
		return false
	}

	for _, v := range files {
		if v.IsDir() || v.Name()[0] != '_' {
			continue
		}

		name := filepath.Join(sectionPath, v.Name())
		cont, ok := cb(s, name, a...)
		if !ok {
			return false
		}
		if !cont {
			break
		}
	}

	return true
}

func (s *Store) readIndex(
	sectionPath string, dst *[]string) (map[string]int, bool) {
	var singulars map[string]int
	if !s.scanIndex(sectionPath, readIndexCb, dst, &singulars) {
		return nil, false
	}
	return singulars, true
}

func readIndexCb(s *Store, name string, a ...interface{}) (bool, bool) {
	dst, singulars := a[0].(*[]string), a[1].(*map[string]int)

	info, err := os.Stat(name)
	if err != nil {
		s.log.Printf("failed to check singularity: %s", err)
		return false, false
	}

	if info.Size() >= int64(s.conf.MinSingularSizeKiB*1024) {
		key, _, ok := parseIndexFileName(filepath.Base(name))
		if !ok {
			s.log.Printf("bad index file name: %s", name)
			return false, false
		}

		if (*singulars) == nil {
			(*singulars) = make(map[string]int)
		}
		(*singulars)[key]++
	} else if !s.readIndexFile(name, dst) {
		return false, false
	}

	return true, true
}

func (s *Store) readIndexFile(name string, dst *[]string) bool {
	in, err := os.Open(name)
	if err != nil {
		s.log.Printf("failed to open index file: %s", err)
		return false
	}
	defer in.Close()

	var gz *gzip.Reader
	if gz, err = gzip.NewReader(in); err != nil {
		s.log.Printf("failed to create gzip reader: %s", err)
		return false
	}

	scan := bufio.NewScanner(gz)
	for scan.Scan() {
		*dst = append(*dst, scan.Text())
	}

	if err := scan.Err(); err != nil {
		s.log.Printf("failed to read index file: %s", err)
		return false
	}

	return true
}

func (s *Store) readCache(cachePath string, dst *[]string) bool {
	in, err := os.Open(cachePath)
	if os.IsNotExist(err) {
		return true
	}
	if err != nil {
		s.log.Printf("failed to open cache file: %s", err)
		return false
	}
	defer in.Close()

	scan := bufio.NewScanner(in)
	for scan.Scan() {
		*dst = append(*dst, scan.Text())
	}

	if err := scan.Err(); err != nil {
		s.log.Printf("failed to read cache file: %s", err)
		return false
	}

	return true
}

// Callback function to read keys or values.
type ReadFunc func(st *Store, val string)

// Find all values for the given key.
func (s *Store) FindValues(key string, cb ReadFunc) bool {
	key = recordKey(key + "\t")
	hash := keyHash(key)
	spath := s.sectionPath(hash)

	var more bool
	var prev string
	if !s.scanIndex(spath, findValuesCb, key, cb, &prev, &more) {
		return false
	}

	if more {
		findValuesCb(s, prev, key, cb, &prev, &more)
	}

	cpath, ok := s.cachePath(hash)
	if !ok {
		return false
	}

	var recs []string
	if !s.readCache(cpath, &recs) {
		return false
	}

	for _, v := range recs {
		if recordKey(v) == key {
			cb(s, recordValue(v))
		}
	}

	return true
}

func findValuesCb(s *Store, name string, a ...interface{}) (bool, bool) {
	key, cb := a[0].(string), a[1].(ReadFunc)
	prev, more := a[2].(*string), a[3].(*bool)

	key2, _, ok := parseIndexFileName(filepath.Base(name))
	if !ok {
		return false, false
	}

	if key2 < key {
		*prev, *more = name, true
		return true, true
	}

	if len(*prev) != 0 {
		var recs []string
		if !s.readIndexFile(*prev, &recs) {
			return false, false
		}

		for _, v := range recs {
			if recordKey(v) == key {
				cb(s, recordValue(v))
			}
		}
	}

	*prev, *more = name, key2 == key
	return *more, true
}

// Find all keys in the store.
func (s *Store) FindKeys(cb ReadFunc) bool {
	for i := 0; i <= 0xffff; i++ {
		spath := s.sectionPath(uint16(i))
		cpath, ok := s.cachePath(uint16(i))
		if !ok {
			return false
		}

		var section []string
		singulars, ok := s.readIndex(spath, &section)
		if !ok || !s.readCache(cpath, &section) {
			return false
		}

		keys := make(map[string]bool)
		for k := range singulars {
			key, err := stripKey(k)
			if err != nil {
				s.log.Printf("failed to strip key: %s", k)
				return false
			}

			keys[key] = true
		}

		for _, v := range section {
			keys[recordStrippedKey(v)] = true
		}

		for k := range keys {
			cb(s, k)
		}
	}

	return true
}
