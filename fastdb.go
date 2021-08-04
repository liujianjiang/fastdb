package fastdb

import (
	"encoding/json"
	"errors"
	"fastdb/index"
	"fastdb/storage"
	"fastdb/utils"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"
)

const (

	// The path for saving rosedb config file.
	configSaveFile = string(os.PathSeparator) + "DB.CFG"

	// The path for saving rosedb meta info.
	dbMetaSaveFile = string(os.PathSeparator) + "DB.META"

	// rosedb reclaim path, a temporary dir, will be removed after reclaim.
	reclaimPath = string(os.PathSeparator) + "rosedb_reclaim"

	// Separator of the extra info, some commands can`t contains it.
	ExtraSeparator = "\\0"

	// DataStructureNum the num of different data structures, there are five now(string, list, hash, set, zset).
	DataStructureNum = 5
)

var (
	// ErrEmptyKey the key is empty
	ErrEmptyKey = errors.New("rosedb: the key is empty")

	// ErrKeyNotExist key not exist
	ErrKeyNotExist = errors.New("rosedb: key not exist")

	// ErrKeyTooLarge the key too large
	ErrKeyTooLarge = errors.New("rosedb: key exceeded the max length")

	// ErrValueTooLarge the value too large
	ErrValueTooLarge = errors.New("rosedb: value exceeded the max length")

	// ErrNilIndexer the indexer is nil
	ErrNilIndexer = errors.New("rosedb: indexer is nil")

	// ErrCfgNotExist the config is not exist
	ErrCfgNotExist = errors.New("rosedb: the config file not exist")

	// ErrReclaimUnreached not ready to reclaim
	ErrReclaimUnreached = errors.New("rosedb: unused space not reach the threshold")

	// ErrExtraContainsSeparator extra contains separator
	ErrExtraContainsSeparator = errors.New("rosedb: extra contains separator \\0")

	// ErrInvalidTTL ttl is invalid
	ErrInvalidTTL = errors.New("rosedb: invalid ttl")

	// ErrKeyExpired the key is expired
	ErrKeyExpired = errors.New("rosedb: key is expired")

	// ErrDBisReclaiming reclaim and single reclaim can`t execute at the same time.
	ErrDBisReclaiming = errors.New("rosedb: can`t do reclaim and single reclaim at the same time")
)

type (
	// RoseDB the rosedb struct, represents a db instance.
	FastDB struct {
		activeFile         ActiveFiles     // Current active files.
		activeFileIds      ActiveFileIds   // Current active file ids.
		archFiles          ArchivedFiles   // The archived files.
		strIndex           *StrIndex       // String indexes(a skip list).
		config             Config          // Config info of rosedb.
		mu                 sync.RWMutex    // mutex.
		meta               *storage.DBMeta // Meta info for rosedb.
		expires            Expires         // Expired directory.
		isReclaiming       bool
		isSingleReclaiming bool
	}

	// ActiveFiles current active files for different data types.
	ActiveFiles map[DataType]*storage.DBFile

	// ActiveFileIds current active files id for different data types.
	ActiveFileIds map[DataType]uint32

	// ArchivedFiles define the archived files, which mean these files can only be read.
	// and will never be opened for writing.
	ArchivedFiles map[DataType]map[uint32]*storage.DBFile

	// Expires saves the expire info of different keys.
	Expires map[DataType]map[string]int64
)

func (db *FastDB) checkKeyValue(key []byte, value ...[]byte) error {
	keySize := uint32(len(key))
	if keySize == 0 {
		return ErrEmptyKey
	}

	config := db.config
	if keySize > config.MaxKeySize {
		return ErrKeyTooLarge
	}

	for _, v := range value {
		if uint32(len(v)) > config.MaxValueSize {
			return ErrValueTooLarge
		}
	}

	return nil
}

// Check whether key is expired and delete it if needed.
func (db *FastDB) checkExpired(key []byte, dType DataType) (expired bool) {
	deadline, exist := db.expires[dType][string(key)]
	if !exist {
		return
	}

	if time.Now().Unix() > deadline {
		expired = true

		var e *storage.Entry
		switch dType {
		case String:
			e = storage.NewEntryNoExtra(key, nil, String, StringRem)
			if ele := db.strIndex.idxList.Remove(key); ele != nil {
				db.incrReclaimableSpace(key)
			}
		}
		if err := db.store(e); err != nil {
			log.Println("checkExpired: store entry err: ", err)
			return
		}
		// delete the expire info stored at key.
		delete(db.expires[dType], string(key))
	}
	return
}

// write entry to db file.
func (db *FastDB) store(e *storage.Entry) error {
	// sync the db file if file size is not enough, and open a new db file.
	config := db.config
	if db.activeFile[e.GetType()].Offset+int64(e.Size()) > config.BlockSize {
		if err := db.activeFile[e.GetType()].Sync(); err != nil {
			return err
		}

		// save the old db file as arched file.
		activeFileId := db.activeFileIds[e.GetType()]
		db.archFiles[e.GetType()][activeFileId] = db.activeFile[e.GetType()]
		activeFileId = activeFileId + 1

		newDbFile, err := storage.NewDBFile(config.DirPath, activeFileId, config.RwMethod, config.BlockSize, e.GetType())
		if err != nil {
			return err
		}
		db.activeFile[e.GetType()] = newDbFile
		db.activeFileIds[e.GetType()] = activeFileId
		db.meta.ActiveWriteOff[e.GetType()] = 0
	}

	// write entry to db file.
	if err := db.activeFile[e.GetType()].Write(e); err != nil {
		return err
	}

	db.meta.ActiveWriteOff[e.GetType()] = db.activeFile[e.GetType()].Offset

	// persist db file according to the config.
	if config.Sync {
		if err := db.activeFile[e.GetType()].Sync(); err != nil {
			return err
		}
	}

	return nil
}

func Open(config Config) (*FastDB, error) {
	// create the dir path if not exists.
	if !utils.Exist(config.DirPath) {
		if err := os.MkdirAll(config.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// load the db files from disk.
	archFiles, activeFileIds, err := storage.Build(config.DirPath, config.RwMethod, config.BlockSize)
	if err != nil {
		return nil, err
	}

	// set active files for writing.
	activeFiles := make(ActiveFiles)
	for dataType, fileId := range activeFileIds {
		file, err := storage.NewDBFile(config.DirPath, fileId, config.RwMethod, config.BlockSize, dataType)
		if err != nil {
			return nil, err
		}
		activeFiles[dataType] = file
	}

	// load db meta info, only active file`s write offset right now.
	meta := storage.LoadMeta(config.DirPath + dbMetaSaveFile)
	for dataType, file := range activeFiles {
		file.Offset = meta.ActiveWriteOff[dataType]
	}

	db := &FastDB{
		activeFile:    activeFiles,
		activeFileIds: activeFileIds,
		archFiles:     archFiles,
		config:        config,
		strIndex:      NewStrIdx(),
		meta:          meta,

		expires: make(Expires),
	}
	for i := 0; i < DataStructureNum; i++ {
		db.expires[uint16(i)] = make(map[string]int64)
	}

	// load indexes from db files.
	if err := db.loadIdxFromFiles(); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *FastDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.saveConfig(); err != nil {
		return err
	}
	if err := db.saveMeta(); err != nil {
		return err
	}

	// close and sync the active file.
	for _, file := range db.activeFile {
		if err := file.Close(true); err != nil {
			return err
		}
	}

	// close the archived files.
	for _, archFile := range db.archFiles {
		for _, file := range archFile {
			if err := file.Sync(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (db *FastDB) saveConfig() (err error) {
	path := db.config.DirPath + configSaveFile
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)

	b, err := json.Marshal(db.config)
	_, err = file.Write(b)
	err = file.Close()

	return
}

func (db *FastDB) saveMeta() error {
	metaPath := db.config.DirPath + dbMetaSaveFile
	return db.meta.Store(metaPath)
}

// build the indexes for different data structures.
func (db *FastDB) buildIndex(entry *storage.Entry, idx *index.Indexer) error {
	if db.config.IdxMode == KeyValueMemMode {
		idx.Meta.Value = entry.Meta.Value
		idx.Meta.ValueSize = uint32(len(entry.Meta.Value))
	}

	switch entry.GetType() {
	case storage.String:
		db.buildStringIndex(idx, entry)
	}
	return nil
}

// Reopen the db according to the specific config path.
func Reopen(path string) (*FastDB, error) {
	if exist := utils.Exist(path + configSaveFile); !exist {
		return nil, ErrCfgNotExist
	}

	var config Config

	b, err := ioutil.ReadFile(path + configSaveFile)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, &config); err != nil {
		return nil, err
	}
	return Open(config)
}
