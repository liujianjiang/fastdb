package storage

import (
	"log"
	"os"
	"testing"
)

const (
	path1            = "/tmp/fastdb"
	fileID1          = 0
	path2            = "/tmp/fastdb"
	fileID2          = 1
	defaultBlockSize = 8 * 1024 * 1024
)

func init() {
	os.MkdirAll(path1, os.ModePerm)
	_, err := os.OpenFile("/tmp/fastdb/000000000.data.str", os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		log.Println("create file err. ", err)
	}
	os.OpenFile("/tmp/fastdb/000000001.data.list", os.O_CREATE|os.O_RDWR, os.ModePerm)
}

func TestNewDBFile(t *testing.T) {
	os.MkdirAll(path1, os.ModePerm)
	newOne := func(method FileRWMethod, dataType uint16) {
		_, err := NewDBFile(path1, fileID1, method, defaultBlockSize, dataType)
		if err != nil {
			t.Error("new db file error ", err)
		}
	}

	t.Run("new db file file io", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			newOne(FileIO, uint16(i))
		}
	})

	t.Run("new db file mmap", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			newOne(MMap, uint16(i))
		}
	})
}

func TestDBFile_Write(t *testing.T) {

	var df, _ = NewDBFile(path2, fileID2, MMap, defaultBlockSize, 1)

	writeEntry := func(key, value []byte) {
		defer df.Sync()
		e := &Entry{
			Meta: &Meta{
				Key:   key,
				Value: value,
			},
		}
		e.Meta.KeySize = uint32(len(e.Meta.Key))
		e.Meta.ValueSize = uint32(len(e.Meta.Value))

		if err := df.Write(e); err != nil {
			t.Error("write data err: ", err)
		}
	}
	writeEntry([]byte("mmap_key_001"), []byte("mmap_val_001"))
	writeEntry([]byte("mmap_key_002"), []byte("mmap_val_002"))
	writeEntry([]byte("mmap_key_003"), []byte("mmap_val_003"))
}

func TestDBFile_Read(t *testing.T) {
	var df, _ = NewDBFile(path2, fileID2, MMap, defaultBlockSize, 1)

	readEntry := func(offset int64) {
		if e, err := df.Read(offset); err != nil {
			t.Error("数据读取失败", err)
		} else {
			t.Log(string(e.Meta.Key), e.Meta.KeySize, string(e.Meta.Value), e.Meta.ValueSize, e.crc32)
		}
	}
	readEntry(0)
	readEntry(50)
	readEntry(100)
}
