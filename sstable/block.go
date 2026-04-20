package sstable

import (
	"encoding/binary"
	"sort"

	"github.com/DecarbonizedGlucose/granite/comparer"
	gerrors "github.com/DecarbonizedGlucose/granite/errors"
	"github.com/DecarbonizedGlucose/granite/util"
)

type block struct {
	bpool          *util.BufferPool
	bp             blockPointer // block pointer, used for indexing
	data           []byte       // whole block data, including restarts
	restartsLen    int
	restartsOffset int
}

// returns (index, offset) of the most recent restart point before the key
func (b *block) recentRestart(cmp comparer.Comparer, ristart, rilimit int, key []byte) (idx, off int, err error) {
	f := func(i int) bool {
		offset := int(binary.LittleEndian.Uint32(b.data[b.restartsOffset+(ristart+i)*4:])) + 1 // +1 to skip 0x00 (shared=0)
		keyLen, keyBytes := binary.Uvarint(b.data[offset:])
		_, valueBytes := binary.Uvarint(b.data[offset+keyBytes:])
		m := keyBytes + valueBytes
		return cmp.Compare(b.data[m:m+int(keyLen)], key) > 0 // first to > key
	}
	idx = max(sort.Search(rilimit-ristart, f)+ristart-1, ristart) // and -1 for < key
	off = int(binary.LittleEndian.Uint32(b.data[b.restartsOffset+idx*4:]))
	return
}

// returns index of the restart point for the given offset
func (b *block) getRestartIndex(ristart, rilimit int, off int) int {
	return sort.Search(rilimit-ristart, func(i int) bool {
		return int(binary.LittleEndian.Uint32(b.data[b.restartsOffset+(ristart+i)*4:])) > off // first > off
	}) + ristart - 1 // and -1 for <= off (hare == off)
}

// returns offset of the restart point for the given index
func (b *block) getRestartOffset(idx int) int {
	return int(binary.LittleEndian.Uint32(b.data[b.restartsOffset+idx*4:]))
}

func (b *block) getEntry(off int) (key, value []byte, kShared, size int, err error) {
	if off > b.restartsOffset {
		// TODO: the file would be broken, should bring more infomation
		err = gerrors.ErrFileBroken
		return
	}
	if off == b.restartsOffset {
		// off has over the last entry, nothing to get
		return
	}

	shared, sharedBytes := binary.Uvarint(b.data[off:])
	keyLen, keyLenBytes := binary.Uvarint(b.data[off+sharedBytes:])
	valueLen, valueLenBytes := binary.Uvarint(b.data[off+sharedBytes+keyLenBytes:])
	headerLen := sharedBytes + keyLenBytes + valueLenBytes
	size = headerLen + int(keyLen) + int(valueLen)
	if sharedBytes <= 0 || keyLenBytes <= 0 || valueLenBytes <= 0 || off+size > b.restartsOffset {
		// TODO: the file would be broken, should bring more infomation
		err = gerrors.ErrFileBroken
		return
	}

	key = b.data[off+headerLen : off+headerLen+int(keyLen)]
	value = b.data[off+headerLen+int(keyLen) : off+size]
	kShared = int(shared)
	return
}

func (b *block) Close() {
	b.bpool.Put(b.data)
	b.bpool = nil
	b.data = nil
}
