package store

type ReadFunc func(st *Store, val string)

func (s *Store) FindValues(key string, cb ReadFunc) bool {

	return false
}
