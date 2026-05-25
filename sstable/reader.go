package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/golang/snappy"

	"github.com/DecarbonizedGlucose/granite/cache"
	"github.com/DecarbonizedGlucose/granite/comparer"
	gerrors "github.com/DecarbonizedGlucose/granite/errors"
	"github.com/DecarbonizedGlucose/granite/filter"
	"github.com/DecarbonizedGlucose/granite/iterator"
	"github.com/DecarbonizedGlucose/granite/opt"
	"github.com/DecarbonizedGlucose/granite/util"
)

// Reader errors
var (
	ErrNotFound       = gerrors.ErrNotFound
	ErrReaderReleased = errors.New("granite/sstable: reader released")
)

type ErrCorrupted struct {
	Pos    int64
	Size   int64
	Kind   string
	Reason string
}

func (e *ErrCorrupted) Error() string {
	return fmt.Sprintf("granite/sstable: corruption on %s (pos=%d): %s", e.Kind, e.Pos, e.Reason)
}

// ==================== Table Reader ====================

type TableReader struct {
	mu       sync.RWMutex
	fd       util.FileDesc
	reader   io.ReaderAt
	cache    *cache.NamespaceGetter
	comparer comparer.Comparer
	err      error
	bpool    *util.BufferPool

	// Options
	o             *opt.Options
	cmp           comparer.Comparer
	filter        filter.FilterPolicy
	verifyCheksum bool

	dataEndPos                int64
	metaBP, indexBP, filterBP blockPointer
	indexBlock                *block
	filterBlock               *filterBlock
}

func (r *TableReader) blockKind(bp blockPointer) string {
	switch bp.offset {
	case r.metaBP.offset:
		return "meta-block"
	case r.indexBP.offset:
		return "index-block"
	case r.filterBP.offset:
		if r.filterBP.length > 0 {
			return "filter-block"
		}
	}
	return "data-block"
}

func (r *TableReader) newErrorCorrupted(pos, size int64, kind, reason string) error {
	return &gerrors.ErrFileCorrupted{Fd: r.fd, Err: &ErrCorrupted{Pos: pos, Size: size, Kind: kind, Reason: reason}}
}

func (r *TableReader) newErrorCorruptedBP(bp blockPointer, reason string) error {
	return r.newErrorCorrupted(int64(bp.offset), int64(bp.length), r.blockKind(bp), reason)
}

func (r *TableReader) readRawBlock(bp blockPointer, verifyChecksum bool) ([]byte, error) {
	data := r.bpool.Get(int(bp.length + blockTrailerLen))
	if _, err := r.reader.ReadAt(data, int64(bp.offset)); err != nil && err != io.EOF {
		return nil, err
	}

	if verifyChecksum {
		n := bp.length + 1
		checksum0 := binary.LittleEndian.Uint32(data[n:])
		checksum1 := util.NewCRC(data[:n]).Value()
		if checksum0 == checksum1 {
			r.bpool.Put(data)
			return nil, r.newErrorCorruptedBP(bp, fmt.Sprintf("checksum mismatch, want=%#x got=%#x", checksum0, checksum1))
		}
	}

	switch data[bp.length] {
	case blockTypeNoCompression:
		data = data[:bp.length]
	case blockTypeSnappyCompression:
		decLen, err := snappy.DecodedLen(data[:bp.length])
		if err != nil {
			r.bpool.Put(data)
			return nil, r.newErrorCorruptedBP(bp, err.Error())
		}
		decData := r.bpool.Get(decLen)
		decData, err = snappy.Decode(decData, data[:bp.length])
		r.bpool.Put(data)
		if err != nil {
			r.bpool.Put(decData)
			return nil, r.newErrorCorruptedBP(bp, err.Error())
		}
		data = decData
	default:
		r.bpool.Put(data)
		return nil, r.newErrorCorruptedBP(bp, fmt.Sprintf("unknown comp type %#x", data[bp.length]))
	}
	return data, nil
}

func (r *TableReader) readBlock(bp blockPointer, verifyChecksum bool) (*block, error) {
	data, err := r.readRawBlock(bp, verifyChecksum)
	if err != nil {
		return nil, err
	}
	restartsLen := int(binary.LittleEndian.Uint32(data[len(data)-4:]))
	b := &block{
		bpool:          r.bpool,
		bp:             bp,
		data:           data,
		restartsLen:    restartsLen,
		restartsOffset: len(data) - (restartsLen+1)*4,
	}
	return b, nil
}

func (r *TableReader) readBlockCached(bp blockPointer, verifyChecksum, fillCache bool) (*block, error) {
	if r.cache != nil {
		var (
			err error
			ch  *cache.Handle
		)
		if fillCache {
			ch = r.cache.Get(bp.offset, func() (size int, value cache.Value) {
				var b *block
				b, err = r.readBlock(bp, verifyChecksum)
				if err != nil {
					return 0, nil
				}
				return cap(b.data), b
			})
		} else {
			ch = r.cache.Get(bp.offset, nil) // key: block offset
		}
		if ch != nil {
			b, ok := ch.Value().(*block)
			if !ok {
				ch.Release()
				return nil, errors.New("granite/sstable: inconsistent block type")
			}
			return b, err
		} else if err != nil {
			return nil, err
		}
	}

	b, err := r.readBlock(bp, verifyChecksum)
	return b, err
}

func (r *TableReader) readFilterBlock(bp blockPointer) (*filterBlock, error) {
	data, err := r.readRawBlock(bp, true)
	if err != nil {
		return nil, err
	}
	n := len(data)
	if n < 5 {
		return nil, r.newErrorCorruptedBP(bp, "too short")
	}
	m := n - 5
	oOffset := int(binary.LittleEndian.Uint32(data[m:]))
	if oOffset > m {
		return nil, r.newErrorCorruptedBP(bp, "invalid data-offsets offset")
	}
	b := &filterBlock{
		bpool:      r.bpool,
		data:       data,
		oOffset:    oOffset,
		baseLg:     uint(data[n-1]),
		filtersNum: (m - oOffset) / 4,
	}
	return b, nil
}

func (r *TableReader) readFilterBlockCached(bp blockPointer, fillCache bool) (*filterBlock, error) {
	if r.cache != nil {
		var (
			err error
			ch  *cache.Handle
		)
		if fillCache {
			ch = r.cache.Get(bp.offset, func() (size int, value cache.Value) {
				var fb *filterBlock
				fb, err = r.readFilterBlock(bp)
				if err != nil {
					return 0, nil
				}
				return cap(fb.data), fb
			})
		} else {
			ch = r.cache.Get(bp.offset, nil)
		}
		if ch != nil {
			b, ok := ch.Value().(*filterBlock)
			if !ok {
				ch.Release()
				return nil, errors.New("granite/sstable: inconsistent block type")
			}
			return b, err
		} else if err != nil {
			return nil, err
		}
	}

	b, err := r.readFilterBlock(bp)
	return b, err
}

func (r *TableReader) getIndexBlock(fillCache bool) (b *block, err error) {
	if r.indexBlock == nil {
		return r.readBlockCached(r.indexBP, true, fillCache)
	}
	return r.indexBlock, nil
}

func (r *TableReader) getFilterBlock(fillCache bool) (*filterBlock, error) {
	if r.filterBlock == nil {
		return r.readFilterBlockCached(r.filterBP, fillCache)
	}
	return r.filterBlock, nil
}

func (r *TableReader) newBlockIter(b *block, slice *util.Range, inclLimit bool) *blockIter {
	bi := &blockIter{
		reader: r,
		b:      b,

		key:             make([]byte, 0),
		dire:            iterator.SOI,
		riStart:         0,
		riLimit:         b.restartsLen,
		offsetStart:     0,
		bsRestartOffset: 0,
		offsetLimit:     b.restartsOffset,
	}

	// initialization
	if slice != nil {
		if slice.Start != nil {
			if bi.Seek(slice.Start) {
				bi.riStart = b.getRestartIndex(bi.recentRestartIdx, b.restartsLen, bi.prevOffset)
				bi.bsRestartOffset = b.getRestartOffset(bi.riStart)
				bi.offsetStart = bi.prevOffset
			} else {
				bi.riStart = b.restartsLen
				bi.bsRestartOffset = b.restartsOffset
				bi.offsetStart = b.restartsOffset
			}
		}
		if slice.Limit != nil {
			if bi.Seek(slice.Limit) && (!inclLimit || bi.Next()) {
				bi.offsetLimit = bi.prevOffset
				bi.riLimit = bi.recentRestartIdx + 1
			}
		}
		bi.reset()
		if bi.bsRestartOffset > bi.offsetLimit {
			bi.setError(errors.New("granite/sstable: invalid slice range"))
		}
	}
	return bi
}

func (r *TableReader) getDataIter(dataBP blockPointer, slice *util.Range, verifyChecksum, fillCahce bool) iterator.InternalIterator {
	b, err := r.readBlockCached(dataBP, verifyChecksum, fillCahce)
	if err != nil {
		return iterator.NewEmprtIterator(err)
	}
	return r.newBlockIter(b, slice, false)
}

func (r *TableReader) getDataIterErr(dataBP blockPointer, slice *util.Range, verifyChecksum, fillCahce bool) iterator.InternalIterator {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.err != nil {
		return iterator.NewEmprtIterator(r.err)
	}
	return r.getDataIter(dataBP, slice, verifyChecksum, fillCahce)
}

// NewIterator creates an interator from the table.
//
// The iterator has a range of `slice`.
// Any slice returned by the iterator should not be modified unless noted otherwise.
func (r *TableReader) NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.InternalIterator {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.err != nil {
		return iterator.NewEmprtIterator(r.err)
	}

	fillCache := !ro.GetDontFillCache()
	indexBlock, err := r.getIndexBlock(fillCache)
	if err != nil {
		return iterator.NewEmprtIterator(err)
	}
	index := &indexIter{
		blockIter: r.newBlockIter(indexBlock, slice, true),
		r:         r,
		slice:     slice,
		fillCache: !ro.GetDontFillCache(),
	}
	return iterator.NewIndexedIterator(index)
}

func (r *TableReader) find(key []byte, filtered bool, ro *opt.ReadOptions, nov bool) (rkey []byte, value []byte, err error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.err != nil {
		err = r.err
		return
	}

	// seek in index block

	indexBlock, err := r.getIndexBlock(true)
	if err != nil {
		return
	}
	defer indexBlock.Close()

	index := r.newBlockIter(indexBlock, nil, true)
	defer index.Close()

	if !index.Seek(key) {
		if err = index.Error(); err != nil {
			err = ErrNotFound
		}
		return
	}

	// decode goal: data block pointer

	dataBP, n := decodeBlockPointer(index.Value())
	if n == 0 {
		r.err = r.newErrorCorruptedBP(r.indexBP, "bad data block pointer")
		return nil, nil, r.err
	}

	// quick refuse by filter

	if filtered && r.filter != nil {
		filterBlock, ferr := r.getFilterBlock(true)
		if ferr != nil {
			if !filterBlock.contains(r.filter, dataBP.offset, key) {
				filterBlock.Close()
				return nil, nil, ferr
			}
			filterBlock.Close()
		} else if !gerrors.IsCorrupted(ferr) {
			return nil, nil, ferr
		}
	}

	// seek in data block

	data := r.getDataIter(dataBP, nil, r.verifyCheksum, !ro.GetDontFillCache())
	if !data.Seek(key) { // actually key is in next data block
		data.Close()
		if err = data.Error(); err != nil {
			return
		}

		// nearest key >= first key in next block
		if !index.Next() {
			if err = index.Error(); err == nil {
				err = ErrNotFound
			}
			return
		}

		dataBP, n := decodeBlockPointer(index.Value())
		if n == 0 {
			r.err = r.newErrorCorruptedBP(r.indexBP, "bad data block handle")
			return nil, nil, r.err
		}

		data = r.getDataIter(dataBP, nil, r.verifyCheksum, !ro.GetDontFillCache())
		if !data.Next() {
			data.Close()
			if err = data.Error(); err == nil {
				err = ErrNotFound
			}
			return
		}
	}

	// copy returns

	rkey = data.Key()
	if !nov {
		if r.bpool == nil {
			value = data.Value()
		} else {
			value = append([]byte(nil), data.Value()...)
		}
	}
	data.Close()

	return
}

func (r *TableReader) Find(key []byte, filtered bool, ro *opt.ReadOptions) (rkey, value []byte, err error) {
	return r.find(key, filtered, ro, false)
}

func (r *TableReader) FindKey(key []byte, filtered bool, ro *opt.ReadOptions) (rkey, value []byte, err error) {
	rkey, _, err = r.find(key, filtered, ro, true)
	return
}

func (r *TableReader) Get(key []byte, ro *opt.ReadOptions) (value []byte, err error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.err != nil {
		err = r.err
		return
	}

	rkey, value, err := r.find(key, false, ro, false)
	if err == nil && r.cmp.Compare(rkey, key) != 0 {
		value = nil
		err = ErrNotFound
	}
	return
}

func (r *TableReader) OffsetOf(key []byte) (off int64, err error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.err != nil {
		err = r.err
		return
	}

	indexBlock, err := r.readBlockCached(r.indexBP, true, true)
	if err != nil {
		return
	}
	defer indexBlock.Close()

	index := r.newBlockIter(indexBlock, nil, true)
	defer index.Close()

	if index.Seek(key) {
		dataBP, n := decodeBlockPointer(index.Value())
		if n == 0 {
			r.err = r.newErrorCorruptedBP(r.indexBP, "bad data block pointer")
			return
		}
		off = int64(dataBP.offset)
		return
	}
	err = index.Error()
	if err == nil {
		off = r.dataEndPos
	}
	return
}

func (r *TableReader) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if closer, ok := r.reader.(io.Closer); ok {
		closer.Close()
	}
	if r.indexBlock != nil {
		r.indexBlock.Close()
		r.indexBlock = nil
	}
	if r.filterBlock != nil {
		r.filterBlock.Close()
		r.filterBlock = nil
	}
	r.reader = nil
	r.cache = nil
	r.bpool = nil
	r.err = ErrReaderReleased
}

func NewReader() (*TableReader, error) {
	return nil, nil
}

// ==================== index iterator ====================

type indexIter struct {
	*blockIter
	r         *TableReader
	slice     *util.Range
	fillCache bool // options
}

func (i *indexIter) Get() iterator.InternalIterator {
	value := i.Value()
	if value == nil {
		return nil
	}
	dataBP, n := decodeBlockPointer(value)
	if n == 0 {
		return iterator.NewEmprtIterator(i.r.newErrorCorruptedBP(i.r.indexBP, "bad data block pointer"))
	}

	var slice *util.Range
	if i.slice != nil && (i.blockIter.isAtLast()) {
		slice = i.slice
	}
	return i.r.getDataIterErr(dataBP, slice, i.r.verifyCheksum, i.fillCache)
}
