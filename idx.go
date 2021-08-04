package fastdb

import (
	"io"
	"log"
	"sort"
	"sync"
	"time"

	"fastdb/index"
	"fastdb/storage"
)

// DataType Define the data structure type.
type DataType = uint16

// Five different data types, support String, List, Hash, Set, Sorted Set right now.
const (
	String DataType = iota
	List
	Hash
	Set
	ZSet
)

// The operations of a String Type, will be a part of Entry, the same for the other four types.
const (
	StringSet uint16 = iota
	StringRem
	StringExpire
	StringPersist
)

// The operations of List.
const (
	ListLPush uint16 = iota
	ListRPush
	ListLPop
	ListRPop
	ListLRem
	ListLInsert
	ListLSet
	ListLTrim
	ListLClear
	ListLExpire
)

// The operations of Hash.
const (
	HashHSet uint16 = iota
	HashHDel
	HashHClear
	HashHExpire
)

// The operations of Set.
const (
	SetSAdd uint16 = iota
	SetSRem
	SetSMove
	SetSClear
	SetSExpire
)

// The operations of Sorted Set.
const (
	ZSetZAdd uint16 = iota
	ZSetZRem
	ZSetZClear
	ZSetZExpire
)

// build string indexes.
func (db *FastDB) buildStringIndex(idx *index.Indexer, entry *storage.Entry) {
	if db.strIndex == nil || idx == nil {
		return
	}

	switch entry.GetMark() {
	case StringSet:
		db.strIndex.idxList.Put(idx.Meta.Key, idx)
	case StringRem:
		db.strIndex.idxList.Remove(idx.Meta.Key)
	case StringExpire:
		if entry.Timestamp < uint64(time.Now().Unix()) {
			db.strIndex.idxList.Remove(idx.Meta.Key)
		} else {
			db.expires[String][string(idx.Meta.Key)] = int64(entry.Timestamp)
		}
	case StringPersist:
		db.strIndex.idxList.Put(idx.Meta.Key, idx)
		delete(db.expires[String], string(idx.Meta.Key))
	}
}

// load String、List、Hash、Set、ZSet indexes from db files.
func (db *FastDB) loadIdxFromFiles() error {
	if db.archFiles == nil && db.activeFile == nil {
		return nil
	}

	wg := sync.WaitGroup{}
	wg.Add(DataStructureNum)
	for dataType := 0; dataType < DataStructureNum; dataType++ {
		go func(dType uint16) {
			defer func() {
				wg.Done()
			}()

			// archived files
			var fileIds []int
			dbFile := make(map[uint32]*storage.DBFile)
			for k, v := range db.archFiles[dType] {
				dbFile[k] = v
				fileIds = append(fileIds, int(k))
			}

			// active file
			dbFile[db.activeFileIds[dType]] = db.activeFile[dType]
			fileIds = append(fileIds, int(db.activeFileIds[dType]))

			// load the db files in a specified order.
			sort.Ints(fileIds)
			for i := 0; i < len(fileIds); i++ {
				fid := uint32(fileIds[i])
				df := dbFile[fid]
				var offset int64 = 0

				for offset <= db.config.BlockSize {
					if e, err := df.Read(offset); err == nil {
						idx := &index.Indexer{
							Meta:      e.Meta,
							FileId:    fid,
							EntrySize: e.Size(),
							Offset:    offset,
						}
						offset += int64(e.Size())

						if len(e.Meta.Key) > 0 {
							if err := db.buildIndex(e, idx); err != nil {
								log.Fatalf("a fatal err occurred, the db can not open.[%+v]", err)
							}
						}
					} else {
						if err == io.EOF {
							break
						}
						log.Fatalf("a fatal err occurred, the db can not open.[%+v]", err)
					}
				}
			}
		}(uint16(dataType))
	}
	wg.Wait()
	return nil
}
