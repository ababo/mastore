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
	"strconv"
)

const dirMode = 0777
const fileMode = 0644

func (s *Store) checkFlushed(wait bool) bool {
	if s.flushed == nil {
		if wait {
			s.flushed = make(chan bool)
		}
		return true
	}

	if wait {
		return <-s.flushed
	} else {
		select {
		case ok := <-s.flushed:
			if ok {
				s.flushed <- true
			} else {
				return false
			}
		}
	}

	return true
}

func (s *Store) AddEntry(key, entry string) bool {
	if !s.checkFlushed(false) {
		return false
	}

	key = stripString(key, "\t\n")
	entry = stripString(entry, "\n")
	line := key + "\t" + entry

	max := s.conf.AccumSizeMiB * 1024 * 1024
	if s.accumSize+len(line) >= max && !s.Flush() {
		return false
	}

	s.accum = append(s.accum, line)
	s.accumSize += len(line)
	return true
}

func (s *Store) Flush() bool {
	s.log_.Println("requested to flush")
	if !s.checkFlushed(true) {
		return false
	}

	go s.flushAccum(s.accum, s.accumSize)
	s.accum, s.accumSize = nil, 0

	return true
}

func (s *Store) flushAccum(accum []string, accumSize int) {
	s.log_.Printf("started flushing (%d entries, %d MiB)",
		len(s.accum), s.accumSize/(1024*1024))

	sort.Sort(byKeyHash(accum))
	s.log_.Printf("finished sorting accumulator")

	prev, count := 0, 0
	finish := make(chan bool)
	for i := range accum {
		if i < len(accum)-1 &&
			keyHash(key(accum[i+1])) == keyHash(key(accum[prev])) {
			continue
		}

		key := keyHash(key(accum[prev]))
		go s.flushSection(finish, key, accum[prev:i+1])
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
	s.flushed <- ok
}

func (s *Store) sectionPath(key uint16) string {
	keys := strconv.FormatInt(int64(key), 16)
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

func (s *Store) flushSection(finish chan<- bool, key uint16, section []string) {
	s.log_.Printf("started to flush section %x", key)
	path, ok := s.cachePath(key)
	if !ok {
		finish <- false
		return
	}

	size := 0
	for _, v := range section {
		size += len(v) + 1
	}

	info, err := os.Stat(path)
	max := int64(s.conf.MaxFileSizeKiB * 1024)
	if err != nil && info != nil &&
		!info.IsDir() && info.Size()+int64(size) >= max {
		finish <- s.rebuildSectionIndex(key, path, section)
	} else {
		finish <- s.appendCache(key, path, section)
	}
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

	sort.Sort(byKeyHash(section))
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

	return true
}

func (s *Store) readIndex(sectionPath string, dst *[]string) bool {
	files, err := ioutil.ReadDir(sectionPath)
	if err != nil {
		if os.IsNotExist(err) {
			return true
		}

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
	prev, size := 0, 0
	max := s.conf.MaxFileSizeKiB * 1024
	for i, v := range section {
		size += len(v)
		if i < len(section)-1 && size+len(section[i+1]) < max {
			continue
		}

		name := filepath.Join(sectionPath, "_"+key(section[prev]))
		out, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY,
			os.FileMode(fileMode))
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

		for j := prev; j <= i; j++ {
			if _, err := fmt.Fprintln(gz, section[j]); err != nil {
				s.log_.Printf(
					"failed to write index file: %s", err)
				return false
			}
		}
		gz.Close()
	}

	return true
}
