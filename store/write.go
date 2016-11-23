package store

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
)

const dirMode = 0777
const fileMode = 0644

func (s *Store) AddValue(key, val string) bool {
	if s.flushedOK != nil {
		select {
		case ok := <-s.flushedOK:
			s.flushedOK = nil
			if !ok {
				return false
			}
		}
	}

	rec := stripString(key, "\t\n") + "\t" + stripString(val, "\n")

	max := s.conf.MaxAccumSizeMiB * 1024 * 1024
	if s.accumSize+len(rec) > max && !s.Flush(false) {
		return false
	}

	hash := keyHash(recordKey(rec))
	sec := s.accum[hash]
	sec = append(sec, rec)
	s.accum[hash] = sec
	s.accumSize += len(rec) + 1
	return true
}

func (s *Store) Flush(wait bool) bool {
	s.log_.Println("requested to flush")
	if s.flushedOK != nil {
		ok := <-s.flushedOK
		s.flushedOK = nil
		if !ok {
			return false
		}
	}

	s.flushedOK = make(chan bool)
	go s.flushAccum(s.accum, s.accumSize)
	s.accum, s.accumSize = make(map[uint16][]string), 0

	if wait {
		ok := <-s.flushedOK
		s.flushedOK = nil
		return ok
	}

	return true
}

func (s *Store) flushAccum(accum map[uint16][]string, accumSize int) {
	s.log_.Printf("started flushing (%d sections, %d bytes)",
		len(accum), accumSize)

	finish := make(chan bool)
	limiter := make(chan bool, s.conf.MaxGoroutines)
	for k, v := range accum {
		limiter <- true
		go s.flushSection(finish, limiter, k, v)
	}

	ok := true
	for i := 0; i < len(accum); i++ {
		if !<-finish {
			ok = false
		}
	}

	if ok {
		s.log_.Printf("finished flushing")
	} else {
		s.log_.Printf("flushing failed")
	}
	s.flushedOK <- ok
}

func (s *Store) sectionPath(key uint16) string {
	keys := fmt.Sprintf("%04x", key)
	return filepath.Join(os.ExpandEnv(s.conf.StorePath), keys)
}

func (s *Store) cachePath(key uint16) (string, bool) {
	spath := s.sectionPath(key)
	info, err := os.Stat(spath)
	if os.IsNotExist(err) {
		return spath, true
	}
	if err != nil {
		s.log_.Printf("failed to obtain cache path for section %x: %s",
			key, err)
		return "", false
	}
	if info.IsDir() {
		spath = filepath.Join(spath, "cache")
	}
	return spath, true
}

func (s *Store) flushSection(finish chan<- bool,
	limiter <-chan bool, key uint16, section []string) {
	path, ok := s.cachePath(key)
	if !ok {
		<-limiter
		finish <- false
		return
	}

	size := 0
	for _, v := range section {
		size += len(v) + 1
	}

	fsize := 0
	info, err := os.Stat(path)
	if err != nil && !os.IsNotExist(err) {
		s.log_.Printf("failed to obtain file size: %s", err)
		<-limiter
		finish <- false
		return
	}
	if err == nil {
		fsize = int(info.Size())
	}

	if fsize+size > s.conf.MaxCacheSizeKiB*1024 {
		ok = s.rebuildSectionIndex(key, path, section)
	} else {
		ok = s.appendCache(key, path, section)
	}
	<-limiter
	finish <- ok
}

func (s *Store) appendCache(key uint16, path string, section []string) bool {
	out, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		os.FileMode(fileMode))
	if err != nil {
		s.log_.Printf("failed to open cache: %s", err)
		return false
	}
	defer out.Close()

	for _, v := range section {
		if _, err := fmt.Fprintln(out, v); err != nil {
			s.log_.Printf("failed to append cache: %s", err)
			return false
		}
	}

	return true
}

func (s *Store) rebuildSectionIndex(key uint16,
	cachePath string, section []string) bool {
	spath := s.sectionPath(key)
	var singulars map[string]int
	singulars, ok := s.readIndex(spath, &section)
	if !ok || !s.readCache(cachePath, &section) {
		return false
	}

	sort.Sort(byKey(section))

	tpath := s.sectionPath(key) + ".tmp"
	if err := os.Mkdir(tpath, os.FileMode(dirMode)); err != nil {
		s.log_.Printf("failed to create index directory: %s", err)
		return false
	}

	for k, v := range singulars {
		for i := 0; i < v; i++ {
			name := indexFileName(k, i)
			if err := os.Link(filepath.Join(spath, name),
				filepath.Join(tpath, name)); err != nil {
				s.log_.Printf(
					"failed to link index file: %s", err)
				return false
			}
		}
	}

	if !s.writeIndex(tpath, section, singulars) {
		return false
	}

	if err := os.RemoveAll(spath); err != nil {
		s.log_.Printf("failed to remove index directory: %s", err)
		return false
	}

	if err := os.Rename(tpath, spath); err != nil {
		s.log_.Printf("failed to rename index directory: %s", err)
		return false
	}

	s.log_.Printf("finished rebuilding index for section %x", key)
	return true
}

func (s *Store) readIndex(
	sectionPath string, dst *[]string) (map[string]int, bool) {
	info, err := os.Stat(sectionPath)
	if os.IsNotExist(err) || (err == nil && !info.IsDir()) {
		return nil, true
	}

	files, err := ioutil.ReadDir(sectionPath)
	if err != nil {
		s.log_.Printf("failed to read index directory: %s", err)
		return nil, false
	}

	var singulars map[string]int
	for _, v := range files {
		if v.IsDir() || v.Name()[0] != '_' {
			continue
		}

		name := filepath.Join(sectionPath, v.Name())
		ignored, ok := s.readIndexFile(name, dst, true)
		if !ok {
			return nil, false
		}

		if ignored {
			key, _, ok := parseIndexFileName(v.Name())
			if !ok {
				s.log_.Printf("bad index file name: %s", err)
				return nil, false
			}

			if singulars == nil {
				singulars = make(map[string]int)
			}
			singulars[key]++
		}
	}

	return singulars, true
}

func (s *Store) readIndexFile(name string,
	dst *[]string, ignoreSingular bool) (bool, bool) {
	in, err := os.Open(name)
	if err != nil {
		s.log_.Printf("failed to open index file: %s", err)
		return false, false
	}
	defer in.Close()

	var gz *gzip.Reader
	if gz, err = gzip.NewReader(in); err != nil {
		s.log_.Printf("failed to create gzip reader: %s", err)
		return false, false
	}

	size, dlen := 0, len(*dst)
	scan := bufio.NewScanner(gz)
	for scan.Scan() {
		rec := scan.Text()
		size += len(rec) + 1
		*dst = append(*dst, rec)
	}

	if err := scan.Err(); err != nil {
		s.log_.Printf("failed to read index file: %s", err)
		return false, false
	}

	singular := size > s.conf.MaxIndexBlockSizeKiB*1024
	if singular && ignoreSingular {
		*dst = (*dst)[:dlen]
	}

	return singular, true
}

func (s *Store) readCache(cachePath string, dst *[]string) bool {
	in, err := os.Open(cachePath)
	if os.IsNotExist(err) {
		return true
	}
	if err != nil {
		s.log_.Printf("failed to open cache file: %s", err)
		return false
	}
	defer in.Close()

	scan := bufio.NewScanner(in)
	for scan.Scan() {
		*dst = append(*dst, scan.Text())
	}

	if err := scan.Err(); err != nil {
		s.log_.Printf("failed to read cache file: %s", err)
		return false
	}

	return true
}

func (s *Store) writeIndex(sectionPath string,
	section []string, singulars map[string]int) bool {
	prev, size := 0, 0
	max := s.conf.MaxIndexBlockSizeKiB * 1024
	for i, v := range section {
		size += len(v)
		if i < len(section)-1 &&
			(size+len(section[i+1]) <= max ||
				recordKey(section[i+1]) ==
					recordKey(section[prev])) {
			continue
		}

		key := recordKey(section[prev])
		name := indexFileName(key, singulars[key])
		name = filepath.Join(sectionPath, name)
		if !s.writeIndexFile(name, section[prev:i+1]) {
			return false
		}

		prev, size = i+1, 0
	}

	return true
}

func (s *Store) writeIndexFile(name string, recs []string) bool {
	out, err := os.OpenFile(name,
		os.O_CREATE|os.O_WRONLY, os.FileMode(fileMode))
	if err != nil {
		s.log_.Printf("failed to open index file: %s", err)
		return false
	}
	defer out.Close()

	var gz *gzip.Writer
	gz, err = gzip.NewWriterLevel(out, s.conf.CompressionLevel)
	if err != nil {
		s.log_.Printf("failed to create gzip writer: %s", err)
		return false
	}
	defer gz.Close()

	for _, v := range recs {
		if _, err := fmt.Fprintln(gz, v); err != nil {
			s.log_.Printf("failed to write index file: %s", err)
			return false
		}
	}

	return true
}
