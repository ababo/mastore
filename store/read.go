package store

type ReadFunc func(st *Store, entry string)

func (s *Store) FindEntries(key string, cb ReadFunc) bool {

	return false
}
