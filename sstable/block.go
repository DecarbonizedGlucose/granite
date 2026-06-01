package sstable

import (
	"encoding/binary"
	"sort"

	"github.com/DecarbonizedGlucose/granite/comparer"
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
func (b *block) recentRestart(cmp comparer.Comparer, ristart, rilimit int, key []byte) (idx, off int) {
	idx = sort.Search(b.restartsLen-ristart-(b.restartsLen-rilimit), func(i int) bool {
		offset := int(binary.LittleEndian.Uint32(b.data[b.restartsOffset+4*(ristart+i):]))
		offset++                                    // shared always zero, since this is a restart point
		v1, n1 := binary.Uvarint(b.data[offset:])   // key length
		_, n2 := binary.Uvarint(b.data[offset+n1:]) // value length
		m := offset + n1 + n2
		return cmp.Compare(b.data[m:m+int(v1)], key) > 0
	}) + ristart - 1
	if idx < ristart {
		idx = ristart
	}
	off = int(binary.LittleEndian.Uint32(b.data[b.restartsOffset+4*idx:]))
	return
}

// returns index of the restart point for the given offset according to the range [ristart, rilimit)
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
		err = &ErrCorrupted{Reason: "entries offset not aligned"}
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
		err = &ErrCorrupted{Reason: "entries corrupted"}
		return
	}

	key = b.data[off+headerLen : off+headerLen+int(keyLen)]
	value = b.data[off+headerLen+int(keyLen) : off+size]
	kShared = int(shared)
	return
}

func (b *block) Release() {
	b.bpool.Put(b.data)
	b.bpool = nil
	b.data = nil
}
