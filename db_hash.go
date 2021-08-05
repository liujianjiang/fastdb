package fastdb

import (
	"bytes"
	"fastdb/ds/hash"
	"fastdb/storage"
	"sync"
)

type HashIdx struct {
	mu      sync.RWMutex
	indexes *hash.Hash
}

func newHashIdx() *HashIdx {
	return &HashIdx{indexes: hash.New()}
}

func (db *FastDB) HGet(key, field []byte) []byte {
	if err := db.checkKeyValue(key, nil); err != nil {
		return nil
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(key, Hash) {
		return nil
	}

	return db.hashIndex.indexes.HGet(string(key), string(field))
}

func (db *FastDB) HSet(key []byte, field []byte, value []byte) (res int, err error) {
	if err = db.checkKeyValue(key, value); err != nil {
		return
	}

	// If the existed value is the same as the set value, nothing will be done.
	oldVal := db.HGet(key, field)
	if bytes.Compare(oldVal, value) == 0 {
		return
	}

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	e := storage.NewEntry(key, value, field, Hash, HashHSet)
	if err = db.store(e); err != nil {
		return
	}

	res = db.hashIndex.indexes.HSet(string(key), string(field), value)
	return
}
