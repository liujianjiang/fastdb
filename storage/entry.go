package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"time"
)

const (
	// 4 * 4 + 8 + 2 = 26
	entryHeaderSize = 26
)

var (
	ErrInvalidEntry = errors.New("storage/entry: invalid entry")

	ErrInvalidCrc = errors.New("storage/entry: invalid crc")
)

const (
	String uint16 = iota
	List
	Hash
	Set
	ZSet
)

type Meta struct {
	Key       []byte
	Value     []byte
	Extra     []byte
	KeySize   uint32 //4
	ValueSize uint32 //4
	ExtraSize uint32 //4
}

type Entry struct {
	Meta      *Meta  //12
	state     uint16 //2 类型
	crc32     uint32 //4 数据校验
	Timestamp uint64 //8 时间戳
}

func newInternal(key, value, extra []byte, state uint16, timestamp uint64) *Entry {
	return &Entry{
		state: state, Timestamp: timestamp,
		Meta: &Meta{
			Key:       key,
			Value:     value,
			Extra:     extra,
			KeySize:   uint32(len(key)),
			ValueSize: uint32(len(value)),
			ExtraSize: uint32(len(extra)),
		},
	}
}

// Size the entry`s total size.
func (e *Entry) Size() uint32 {
	return entryHeaderSize + e.Meta.KeySize + e.Meta.ValueSize + e.Meta.ExtraSize
}

// NewEntry create a new entry.
func CreateEntry(key, value, extra []byte, t, mark uint16) *Entry {
	var state uint16 = 0
	// set type and mark.
	state = state | (t << 8)
	state = state | mark
	return newInternal(key, value, extra, state, uint64(time.Now().UnixNano()))
}

func (e *Entry) Encode() ([]byte, error) {
	if e == nil || e.Meta.KeySize == 0 {
		return nil, ErrInvalidEntry
	}

	ks, vs := e.Meta.KeySize, e.Meta.ValueSize
	es := e.Meta.ExtraSize
	buf := make([]byte, e.Size())

	crc := crc32.ChecksumIEEE(e.Meta.Value)
	binary.BigEndian.PutUint32(buf[0:4], crc)
	binary.BigEndian.PutUint32(buf[4:8], ks)
	binary.BigEndian.PutUint32(buf[8:12], vs)
	binary.BigEndian.PutUint32(buf[12:16], es)
	binary.BigEndian.PutUint16(buf[16:18], e.state)
	binary.BigEndian.PutUint64(buf[18:26], e.Timestamp)
	copy(buf[entryHeaderSize:entryHeaderSize+ks], e.Meta.Key)
	copy(buf[entryHeaderSize+ks:(entryHeaderSize+ks+vs)], e.Meta.Value)
	if es > 0 {
		copy(buf[(entryHeaderSize+ks+vs):(entryHeaderSize+ks+vs+es)], e.Meta.Extra)
	}

	return buf, nil

}

func Decode(buf []byte) (*Entry, error) {
	crc := binary.BigEndian.Uint32(buf[0:4])
	ks := binary.BigEndian.Uint32(buf[4:8])
	vs := binary.BigEndian.Uint32(buf[8:12])
	es := binary.BigEndian.Uint32(buf[12:16])
	state := binary.BigEndian.Uint16(buf[16:18])
	timestamp := binary.BigEndian.Uint64(buf[18:26])

	return &Entry{
		Meta: &Meta{
			KeySize:   ks,
			ValueSize: vs,
			ExtraSize: es,
		},
		state:     state,
		crc32:     crc,
		Timestamp: timestamp,
	}, nil
}

func (e *Entry) GetType() uint16 {
	return e.state >> 8
}

func (e *Entry) GetMark() uint16 {
	return e.state & (2<<7 - 1)
}

func NewEntryWithExpire(key, value []byte, deadline int64, t, mark uint16) *Entry {
	var state uint16 = 0
	// set type and mark.
	state = state | (t << 8)
	state = state | mark

	return newInternal(key, value, nil, state, uint64(deadline))
}

// NewEntry create a new entry.
func NewEntry(key, value, extra []byte, t, mark uint16) *Entry {
	var state uint16 = 0
	// set type and mark.
	state = state | (t << 8)
	state = state | mark
	return newInternal(key, value, extra, state, uint64(time.Now().UnixNano()))
}

// NewEntryNoExtra create a new entry without extra info.
func NewEntryNoExtra(key, value []byte, t, mark uint16) *Entry {
	return NewEntry(key, value, nil, t, mark)
}
