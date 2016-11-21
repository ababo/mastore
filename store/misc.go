package store

import (
	"github.com/howeyc/crc16"
	"net/url"
	"strings"
)

func stripString(str, chars string) string {
	if i := strings.IndexAny(str, chars); i != -1 {
		str = str[:i]
	}
	return str
}

func key(accumLine string) string {
	return url.QueryEscape(accumLine[:strings.Index(accumLine, "\t")])
}

func keyHash(key string) uint16 {
	return crc16.ChecksumIBM([]byte(key))
}

type keyHashCmp struct {
	strs  []string
	cache map[string]uint16
}

func newKeyHashCmp(strs []string) *keyHashCmp {
	cmp := keyHashCmp{
		strs:  strs,
		cache: make(map[string]uint16),
	}

	for _, v := range cmp.strs {
		cmp.cache[key(v)] = keyHash(key(v))
	}

	return &cmp
}

func (c keyHashCmp) Len() int {
	return len(c.strs)
}
func (c keyHashCmp) Swap(i, j int) {
	c.strs[i], c.strs[j] = c.strs[j], c.strs[i]
}
func (c keyHashCmp) Less(i, j int) bool {
	return c.cache[key(c.strs[i])] < c.cache[key(c.strs[j])]
}

type byKey []string

func (a byKey) Len() int           { return len(a) }
func (a byKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byKey) Less(i, j int) bool { return key(a[i]) < key(a[j]) }
