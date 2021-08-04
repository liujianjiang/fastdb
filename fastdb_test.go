package fastdb

import (
	"log"

	"fastdb/storage"
)

var dbPath = "/tmp/fastdb_server_data"

func InitDb() *FastDB {
	config := DefaultConfig()
	//config.DirPath = dbPath
	config.IdxMode = KeyOnlyMemMode
	config.RwMethod = storage.FileIO
	//config.BlockSize = 4 * 1024 * 1024

	db, err := Open(config)
	if err != nil {
		log.Fatal(err)
	}

	return db
}

func ReopenDb() *FastDB {
	db, err := Reopen(dbPath)
	if err != nil {
		log.Fatal(err)
	}

	return db
}
