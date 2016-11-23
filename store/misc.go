package store

import (
	"fmt"
	"github.com/howeyc/crc16"
	"net/url"
	"strconv"
	"strings"
)

func stripString(str, chars string) string {
	if i := strings.IndexAny(str, chars); i != -1 {
		str = str[:i]
	}
	return str
}

func recordKey(rec string) string {
	return url.QueryEscape(rec[:strings.Index(rec, "\t")])
}

func keyHash(key string) uint16 {
	return crc16.ChecksumIBM([]byte(key))
}

type byKey []string

func (a byKey) Len() int           { return len(a) }
func (a byKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byKey) Less(i, j int) bool { return recordKey(a[i]) < recordKey(a[j]) }

func indexFileName(key string, part int) string {
	return fmt.Sprintf("_%s_%04x", key, part)
}

func parseIndexFileName(name string) (string, int, bool) {
	comps := strings.Split(name, "_")
	if len(comps) != 3 || len(comps[0]) != 0 {
		return "", 0, false
	}

	part, err := strconv.ParseInt(comps[2], 16, 16)
	if err != nil {
		return "", 0, false
	}

	return comps[1], int(part), true
}
