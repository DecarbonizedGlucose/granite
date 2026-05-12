package sstable

import (
	"encoding/binary"

	"github.com/DecarbonizedGlucose/granite/filter"
	"github.com/DecarbonizedGlucose/granite/util"
)

type filterBlock struct {
	bpool      *util.BufferPool
	data       []byte // filter rare data
	oOffset    int    // start position of filter offset array
	baseLg     uint   // block size each seg of filter can handle
	filtersNum int    // num of filters
}

// contains returns if the key may be contained in sstable
func (b *filterBlock) contains(filter filter.FilterPolicy, offset uint64, key []byte) bool {
	i := int(offset >> uint64(b.baseLg))
	if i < b.filtersNum {
		o := b.data[b.oOffset+i*4:]
		n := int(binary.LittleEndian.Uint32(o))
		m := int(binary.LittleEndian.Uint32(o[4:]))
		if n < m && m <= b.oOffset {
			return filter.MayContain(b.data[n:m], key)
		} else if n == m {
			return false
		}
	}
	return true
}

func (b *filterBlock) Close() {
	b.bpool.Put(b.data)
	b.bpool = nil
	b.data = nil
}
