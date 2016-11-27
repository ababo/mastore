package store

import (
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
		s.log_.Printf("failed to read index directory: %s", err)
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

type ReadFunc func(st *Store, val string)

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
