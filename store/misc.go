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

type byKeyHash []string

func (a byKeyHash) Len() int      { return len(a) }
func (a byKeyHash) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byKeyHash) Less(i, j int) bool {
	return keyHash(key(a[i])) < keyHash(key(a[j]))
}
