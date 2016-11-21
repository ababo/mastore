package store

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
)

const dirMode = 0777
const fileMode = 0644

func (s *Store) AddEntry(key, entry string) bool {
	if s.flushedOK != nil {
		select {
		case ok := <-s.flushedOK:
			s.flushedOK = nil
			if !ok {
				return false
			}
		}
	}

	key = stripString(key, "\t\n")
	entry = stripString(entry, "\n")
	line := key + "\t" + entry

	max := s.conf.MaxAccumSizeMiB * 1024 * 1024
	if s.accumSize+len(line) >= max && !s.Flush(false) {
		return false
	}

	s.accum = append(s.accum, line)
	s.accumSize += len(line)
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
	s.accum, s.accumSize = nil, 0

	if wait {
		ok := <-s.flushedOK
		s.flushedOK = nil
		return ok
	}

	return true
}

func (s *Store) flushAccum(accum []string, accumSize int) {
	s.log_.Printf("started flushing (%d entries, %d bytes)",
		len(accum), accumSize)

	sort.Sort(newKeyHashCmp(accum))
	s.log_.Printf("finished sorting accumulator")

	prev, count := 0, 0
	finish := make(chan bool)
	limiter := make(chan bool, s.conf.MaxGoroutines)
	for i := range accum {
		if i < len(accum)-1 &&
			keyHash(key(accum[i+1])) == keyHash(key(accum[prev])) {
			continue
		}

		limiter <- true
		key := keyHash(key(accum[prev]))
		go s.flushSection(finish, limiter, key, accum[prev:i+1])
		prev = i + 1
		count++
	}

	ok := true
	for i := 0; i < count; i++ {
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

	if fsize+size >= s.conf.MaxCacheSizeKiB*1024 {
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
	s.log_.Printf("started to rebuild index for section %x", key)
	spath := s.sectionPath(key)
	if !s.readIndex(spath, &section) ||
		!s.readIndex(cachePath, &section) {
		return false
	}
	s.log_.Printf("finished reading data for section %x", key)

	sort.Sort(byKey(section))
	s.log_.Printf("finished sorting data for section %x", key)

	tpath := s.sectionPath(key) + ".tmp"
	if err := os.Mkdir(tpath, os.FileMode(dirMode)); err != nil {
		s.log_.Printf("failed to create index directory: %s", err)
		return false
	}

	if !s.writeIndex(tpath, section) {
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

func (s *Store) readIndex(sectionPath string, dst *[]string) bool {
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

		in, err := os.Open(filepath.Join(sectionPath, v.Name()))
		if err != nil {
			s.log_.Printf("failed to open index file: %s", err)
			return false
		}
		defer in.Close()

		var gz *gzip.Reader
		if gz, err = gzip.NewReader(in); err != nil {
			s.log_.Printf("failed to create gzip reader: %s", err)
			return false
		}

		var str string
		rd := bufio.NewReader(gz)
		for ; err == nil; str, err = rd.ReadString('\n') {
			if len(str) != 0 && str != "\n" {
				*dst = append(*dst, str)
			}
		}

		if err != io.EOF {
			s.log_.Printf("failed to read index file: %s", err)
			return false
		}
	}

	return true
}

func (s *Store) readCache(cachePath string, dst *[]string) bool {
	in, err := os.Open(cachePath)
	if err != nil {
		s.log_.Printf("failed to open cache file: %s", err)
		return false
	}
	defer in.Close()

	var str string
	rd := bufio.NewReader(in)
	for ; err == nil; str, err = rd.ReadString('\n') {
		if len(str) != 0 && str != "\n" {
			*dst = append(*dst, str)
		}
	}

	if err != io.EOF {
		s.log_.Printf("failed to read cache file: %s", err)
		return false
	}

	return true
}

func (s *Store) writeIndex(sectionPath string, section []string) bool {
	prev, size, dupls := 0, 0, 0
	max := s.conf.MaxIndexBlockSizeKiB * 1024
	for i, v := range section {
		size += len(v)
		if i < len(section)-1 && size+len(section[i+1]) < max {
			continue
		}

		name := fmt.Sprintf("_%s_%04x", key(section[prev]), dupls)
		if !s.writeIndexFile(
			filepath.Join(sectionPath, name), section[prev:i+1]) {
			return false
		}

		if i > 0 && i < len(section)-1 &&
			key(section[prev]) == key(section[i+1]) {
			dupls++
		} else {
			dupls = 0
		}
		prev, size = i+1, 0
	}

	return true
}

func (s *Store) writeIndexFile(name string, lines []string) bool {
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

	for _, v := range lines {
		if _, err := fmt.Fprintln(gz, v); err != nil {
			s.log_.Printf("failed to write index file: %s", err)
			return false
		}
	}

	return true
}
