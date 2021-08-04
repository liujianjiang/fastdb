package storage

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/roseduan/mmap-go"
)

const (
	// FilePerm 默认的创建文件权限
	// default permission of the created file
	FilePerm = 0644

	// PathSeparator the default path separator
	PathSeparator = string(os.PathSeparator)
)

var (
	// DBFileFormatNames 默认数据文件名称格式化
	// default name format of the db files.
	DBFileFormatNames = map[uint16]string{
		0: "%09d.data.str",
		1: "%09d.data.list",
		2: "%09d.data.hash",
		3: "%09d.data.set",
		4: "%09d.data.zset",
	}

	// DBFileSuffixName represent the suffix names of the db files.
	DBFileSuffixName = []string{"str", "list", "hash", "set", "zset"}
)

var (
	// ErrEmptyEntry the entry is empty
	ErrEmptyEntry = errors.New("storage/db_file: entry or the Key of entry is empty")
)

// FileRWMethod 文件数据读写的方式
// db file read and write method
type FileRWMethod uint8

const (

	// FileIO 表示文件数据读写使用系统标准IO
	// Indicates that data file read and write using system standard IO
	FileIO FileRWMethod = iota

	// MMap 表示文件数据读写使用Mmap
	// MMap指的是将文件或其他设备映射至内存，具体可参考Wikipedia上的解释 https://en.wikipedia.org/wiki/Mmap
	// Indicates that data file read and write using mmap
	MMap
)

type DBFile struct {
	Id     uint32
	path   string
	File   *os.File
	mmap   mmap.MMap
	Offset int64
	method FileRWMethod
}

func (df *DBFile) Write(e *Entry) error {
	if e == nil || e.Meta.KeySize == 0 {
		return ErrEmptyEntry
	}

	method := df.method
	writeOffset := df.Offset
	encodeValue, err := e.Encode()
	if err != nil {
		return err
	}

	if method == FileIO {
		_, err := df.File.WriteAt(encodeValue, writeOffset)
		if err != nil {
			return err
		}
	}
	if method == MMap {
		copy(df.mmap[writeOffset:], encodeValue)
	}
	df.Offset += int64(e.Size())
	return nil
}

func (df *DBFile) Read(offset int64) (e *Entry, err error) {
	var buf []byte
	if buf, err = df.readBuf(offset, int64(entryHeaderSize)); err != nil {
		return
	}
	if e, err = Decode(buf); err != nil {
		return
	}
	//log.Println(e.Meta.ExtraSize)
	offset += entryHeaderSize
	if e.Meta.KeySize > 0 {
		var key []byte
		if key, err = df.readBuf(offset, int64(e.Meta.KeySize)); err != nil {
			return
		}
		e.Meta.Key = key
	}
	//log.Println(string(e.Meta.Key))
	offset += int64(e.Meta.KeySize)
	if e.Meta.ValueSize > 0 {
		var val []byte
		if val, err = df.readBuf(offset, int64(e.Meta.KeySize)); err != nil {
			return
		}
		e.Meta.Value = val
	}
	//log.Println(string(e.Meta.Key))
	offset += int64(e.Meta.ValueSize)
	if e.Meta.ExtraSize > 0 {
		var val []byte
		if val, err = df.readBuf(offset, int64(e.Meta.ExtraSize)); err != nil {
			return
		}
		e.Meta.Extra = val
	}
	log.Println(e.Meta.Value)
	checkCrc := crc32.ChecksumIEEE(e.Meta.Value)
	if checkCrc != e.crc32 {
		return nil, ErrInvalidCrc
	}
	return

}

func (df *DBFile) readBuf(offset int64, n int64) ([]byte, error) {
	buf := make([]byte, n)
	if df.method == FileIO {
		_, err := df.File.ReadAt(buf, offset)
		if err != nil {
			return nil, err
		}
	}

	//内存
	if df.method == MMap && offset <= int64(len(df.mmap)) {
		copy(buf, df.mmap[offset:])
	}

	return buf, nil
}

//根据不同的数据类型新建数据库文件
func NewDBFile(path string, fileId uint32, method FileRWMethod, blockSize int64, eType uint16) (*DBFile, error) {
	filePath := path + PathSeparator + fmt.Sprintf(DBFileFormatNames[eType], fileId)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, FilePerm)
	if err != nil {
		return nil, err
	}

	df := &DBFile{Id: fileId, path: path, Offset: 0, method: method}

	if method == FileIO {
		df.File = file
	} else {
		if err = file.Truncate(blockSize); err != nil {
			return nil, err
		}
		m, err := mmap.Map(file, os.O_RDWR, 0)
		if err != nil {
			return nil, err
		}
		df.mmap = m
	}
	return df, nil

}

// Sync 数据持久化
func (df *DBFile) Sync() (err error) {
	if df.File != nil {
		err = df.File.Sync()
	}

	if df.mmap != nil {
		err = df.mmap.Flush()
	}
	return
}

// Build 加载数据文件
// build db files.
func Build(path string, method FileRWMethod, blockSize int64) (map[uint16]map[uint32]*DBFile, map[uint16]uint32, error) {
	dir, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, nil, err
	}

	fileIdsMap := make(map[uint16][]int)
	for _, d := range dir {
		if strings.Contains(d.Name(), ".data") {
			splitNames := strings.Split(d.Name(), ".")
			id, _ := strconv.Atoi(splitNames[0])

			// find the different types of file.
			switch splitNames[2] {
			case DBFileSuffixName[0]:
				fileIdsMap[0] = append(fileIdsMap[0], id)
			case DBFileSuffixName[1]:
				fileIdsMap[1] = append(fileIdsMap[1], id)
			case DBFileSuffixName[2]:
				fileIdsMap[2] = append(fileIdsMap[2], id)
			case DBFileSuffixName[3]:
				fileIdsMap[3] = append(fileIdsMap[3], id)
			case DBFileSuffixName[4]:
				fileIdsMap[4] = append(fileIdsMap[4], id)
			}
		}
	}

	// load all the db files.
	activeFileIds := make(map[uint16]uint32)
	archFiles := make(map[uint16]map[uint32]*DBFile)
	var dataType uint16 = 0
	for ; dataType < 5; dataType++ {
		fileIDs := fileIdsMap[dataType]
		sort.Ints(fileIDs)
		files := make(map[uint32]*DBFile)
		var activeFileId uint32 = 0

		if len(fileIDs) > 0 {
			activeFileId = uint32(fileIDs[len(fileIDs)-1])

			for i := 0; i < len(fileIDs)-1; i++ {
				id := fileIDs[i]

				file, err := NewDBFile(path, uint32(id), method, blockSize, dataType)
				if err != nil {
					return nil, nil, err
				}
				files[uint32(id)] = file
			}
		}
		archFiles[dataType] = files
		activeFileIds[dataType] = activeFileId
	}
	return archFiles, activeFileIds, nil
}

// Close 读写后进行关闭操作
// sync 关闭前是否持久化数据
// close the db file, sync means whether to persist data before closing
func (df *DBFile) Close(sync bool) (err error) {
	if sync {
		err = df.Sync()
	}

	if df.File != nil {
		err = df.File.Close()
	}
	if df.mmap != nil {
		err = df.mmap.Unmap()
	}
	return
}
