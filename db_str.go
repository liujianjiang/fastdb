package fastdb

import (
	"bytes"
	"fastdb/index"
	"fastdb/storage"
	"sync"
)

type StrIndex struct {
	mu      sync.RWMutex
	idxList *index.SkipList
}

func NewStrIdx() *StrIndex {
	return &StrIndex{idxList: index.NewSkipList()}
}

func (db *FastDB) Set(key, value []byte) error {
	return db.doSet(key, value)
}

func (db *FastDB) Get(key []byte) ([]byte, error) {
	if err := db.checkKeyValue(key, nil); err != nil {
		return nil, err
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	// Get index info from a skip list in memory.
	node := db.strIndex.idxList.Get(key)
	if node == nil {
		return nil, ErrKeyNotExist
	}

	idx := node.Value().(*index.Indexer)
	if idx == nil {
		return nil, ErrNilIndexer
	}

	// Check if the key is expired.
	if db.checkExpired(key, String) {
		return nil, ErrKeyExpired
	}

	// In KeyValueMemMode, the value will be stored in memory.
	// So get the value from the index info.
	if db.config.IdxMode == KeyValueMemMode {
		return idx.Meta.Value, nil
	}

	// In KeyOnlyMemMode, the value not in memory.
	// So get the value from the db file at the offset.
	if db.config.IdxMode == KeyOnlyMemMode {
		df := db.activeFile[String]

		if idx.FileId != db.activeFileIds[String] {
			df = db.archFiles[String][idx.FileId]
		}

		e, err := df.Read(idx.Offset)
		if err != nil {
			return nil, err
		}
		return e.Meta.Value, nil
	}

	return nil, ErrKeyNotExist
}

func (db *FastDB) doSet(key, value []byte) (err error) {
	if err = db.checkKeyValue(key, value); err != nil {
		return err
	}
	// If the existed value is the same as the set value, nothing will be done.
	if db.config.IdxMode == KeyValueMemMode {
		if existVal, _ := db.Get(key); existVal != nil && bytes.Compare(existVal, value) == 0 {
			return
		}
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(key, value, String, StringSet)
	if err := db.store(e); err != nil {
		return err
	}

	db.incrReclaimableSpace(key)
	// clear expire time.
	if _, ok := db.expires[String][string(key)]; ok {
		delete(db.expires[String], string(key))
	}

	// string indexes, stored in skiplist.
	idx := &index.Indexer{
		Meta: &storage.Meta{
			KeySize:   uint32(len(e.Meta.Key)),
			Key:       e.Meta.Key,
			ValueSize: uint32(len(e.Meta.Value)),
		},
		FileId:    db.activeFileIds[String],
		EntrySize: e.Size(),
		Offset:    db.activeFile[String].Offset - int64(e.Size()),
	}
	// in KeyValueMemMode, both key and value will store in memory.
	if db.config.IdxMode == KeyValueMemMode {
		idx.Meta.Value = e.Meta.Value
	}
	db.strIndex.idxList.Put(idx.Meta.Key, idx)
	return

}

func (db *FastDB) incrReclaimableSpace(key []byte) {
	oldIdx := db.strIndex.idxList.Get(key)
	if oldIdx != nil {
		indexer := oldIdx.Value().(*index.Indexer)

		if indexer != nil {
			space := int64(indexer.EntrySize)
			db.meta.ReclaimableSpace[indexer.FileId] += space
		}
	}
}

// StrLen returns the length of the string value stored at key.
func (db *FastDB) StrLen(key []byte) int {
	if err := db.checkKeyValue(key, nil); err != nil {
		return 0
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	e := db.strIndex.idxList.Get(key)
	if e != nil {
		if db.checkExpired(key, String) {
			return 0
		}
		idx := e.Value().(*index.Indexer)
		return int(idx.Meta.ValueSize)
	}
	return 0
}

// StrExists check whether the key exists.
func (db *FastDB) StrExists(key []byte) bool {
	if err := db.checkKeyValue(key, nil); err != nil {
		return false
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	exist := db.strIndex.idxList.Exist(key)
	if exist && !db.checkExpired(key, String) {
		return true
	}
	return false
}

// StrRem remove the value stored at key.
func (db *FastDB) StrRem(key []byte) error {
	if err := db.checkKeyValue(key, nil); err != nil {
		return err
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(key, nil, String, StringRem)
	if err := db.store(e); err != nil {
		return err
	}

	db.incrReclaimableSpace(key)
	db.strIndex.idxList.Remove(key)
	delete(db.expires[String], string(key))
	return nil
}
