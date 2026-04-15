package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/DecarbonizedGlucose/granite/comparer"
	"github.com/DecarbonizedGlucose/granite/filter"
	"github.com/DecarbonizedGlucose/granite/opt"
	"github.com/DecarbonizedGlucose/granite/util"
)

// ==================== Block Writer ====================

// Writer for any type of block
type blockWriter struct {
	restartGap int // distance between two restart point
	buf        bytes.Buffer
	cntEntry   int // count of entries

	psRestart []uint32 // positions of restart points
	lastKey   []byte

	scratch []byte
}

func (w *blockWriter) appendKV(key, value []byte) (err error) {
	lShared := 0
	if w.cntEntry%w.restartGap == 0 {
		w.psRestart = append(w.psRestart, uint32(w.buf.Len()))
	} else {
		lShared = sharedPrefixLen(w.lastKey, key)
	}
	// key_shared_len
	n := binary.PutUvarint(w.scratch[0:], uint64(lShared))
	// key_not_shared_len
	n += binary.PutUvarint(w.scratch[n:], uint64(len(key)-lShared))
	// value_len
	n += binary.PutUvarint(w.scratch[n:], uint64(len(value)))
	if _, err = w.buf.Write(w.scratch[:n]); err != nil {
		return err
	}
	// key_not_shared_str
	if _, err = w.buf.Write(key[lShared:]); err != nil {
		return err
	}
	// value_str
	if _, err = w.buf.Write(value); err != nil {
		return err
	}
	// update last key
	w.lastKey = append(w.lastKey[:0], key...) // no need to alloc
	w.cntEntry++
	return nil
}

// Write restarts entries
func (w *blockWriter) finish() error {
	if w.cntEntry == 0 {
		// Must have at least 1 restart point
		w.psRestart = append(w.psRestart, 0)
	}
	w.psRestart = append(w.psRestart, uint32(len(w.psRestart)))
	for _, x := range w.psRestart {
		var buf4 [4]byte
		binary.LittleEndian.PutUint32(buf4[:], x)
		if _, err := w.buf.Write(buf4[:]); err != nil {
			return err
		}
	}
	return nil
}

func (w *blockWriter) reset() {
	w.buf.Reset()
	w.cntEntry = 0
	w.psRestart = w.psRestart[:0]
}

func (w *blockWriter) datalen() int {
	lenRestart := max(len(w.psRestart), 1)
	// len of restart points + len of len + len of entries
	return lenRestart*4 + 4 + w.buf.Len()
}

// ==================== Filter Writer ====================

type filterWriter struct {
	buf bytes.Buffer
	gen filter.FilterGenerator

	cntKey  int
	offsets []uint32
	baseLg  uint
}

func (w *filterWriter) add(key []byte) {
	if w.gen == nil {
		return
	}
	w.gen.Add(key)
	w.cntKey++
}

func (w *filterWriter) generate() {
	// record offset
	w.offsets = append(w.offsets, uint32(w.buf.Len()))
	// gen filter data
	if w.cntKey > 0 {
		w.gen.Generate(&w.buf)
		w.cntKey = 0
	}
}

func (w *filterWriter) flush(offset uint64) {
	if w.gen == nil {
		return
	}
	for x := int(offset / (1 << w.baseLg)); x > len(w.offsets); {
		w.generate()
	}
}

func (w *filterWriter) finish() error {
	if w.gen == nil {
		return nil
	}

	if w.cntKey > 0 {
		w.generate()
	}
	w.offsets = append(w.offsets, uint32(w.buf.Len()))
	for _, x := range w.offsets {
		var buf4 [4]byte
		binary.LittleEndian.PutUint32(buf4[:], x)
		if _, err := w.buf.Write(buf4[:]); err != nil {
			return err
		}
	}
	return w.buf.WriteByte(byte(w.baseLg))
}

// ==================== SSTable Writer ====================

type TableWriter struct {
	comparer    comparer.Comparer // key comparer and separator/successor generator
	filter      filter.FilterPolicy
	compression opt.CompressionType
	blockSize   int // data block size

	writer io.Writer // writing destinaiton
	err    error     // temporarily save the error state of TableWriter

	bpool       *util.BufferPool // used to reduce working pressure on GC
	dataBlock   blockWriter
	indexBlock  blockWriter
	filterBlock filterWriter
	pendingBP   blockPointer // temp bp that written to file but not encoded into the index block
	offset      uint64       // offset already written to the file
	cntEntry    int          // count of entries successfully written so far

	// temp buffers
	scratch             [50]byte // 5 uvarint
	comparerScratch     []byte
	compressionScreatch []byte
}

// It will be called only after all entries are added, and the block is finished
func (w *TableWriter) writeBlock(buf *bytes.Buffer, compType opt.CompressionType) (bp blockPointer, err error) {
	// TODO
	// 先进行可能的压缩，再写入，要计算checksum，最后返回block handle
	return
}

func (w *TableWriter) flushPendingBP(key []byte) error {
	if w.pendingBP.length == 0 {
		return nil
	}
	var sep []byte
	if len(key) == 0 {
		sep = w.comparer.Successor(w.comparerScratch[:0], w.dataBlock.lastKey)
	} else {
		sep = w.comparer.Separator(w.comparerScratch[:0], w.dataBlock.lastKey, key)
	}
	if sep == nil {
		sep = w.dataBlock.lastKey
	} else {
		w.comparerScratch = sep
	}
	n := encodeBlockPointer(w.scratch[:20], w.pendingBP)
	// Append to index block
	if err := w.indexBlock.appendKV(sep, w.scratch[:n]); err != nil {
		return err
	}
	// Reset last key of data block
	w.dataBlock.lastKey = w.dataBlock.lastKey[:0]
	// Clear pending block pointer
	w.pendingBP = blockPointer{}
	return nil
}

func (w *TableWriter) finishBlock() error {
	if err := w.dataBlock.finish(); err != nil {
		return err
	}
	bp, err := w.writeBlock(&w.dataBlock.buf, w.compression)
	if err != nil {
		return err
	}
	w.pendingBP = bp
	// Reset data block
	w.dataBlock.reset()
	// Flush filter block
	w.filterBlock.flush(w.offset)
	return nil
}

// Append appends a k/v pair to the table. The keys must be in increasing order.
func (w *TableWriter) Append(key, value []byte) error {
	if w.err != nil {
		return w.err
	}
	if w.cntEntry > 0 && w.comparer.Compare(w.dataBlock.lastKey, key) >= 0 {
		w.err = fmt.Errorf("granite/sstable: TableWriter: keys are not in increasing order: %q, %q", w.dataBlock.lastKey, key)
		return w.err
	}

	if err := w.flushPendingBP(key); err != nil {
		// cannot flush last block pointer
		return w.err
	}
	// Append key/valur pair to data block
	if err := w.dataBlock.appendKV(key, value); err != nil {
		return w.err
	}
	// Add key to filter block
	w.filterBlock.add(key)

	// Finish the date block if block size is reached
	if w.dataBlock.datalen() >= w.blockSize {
		if err := w.finishBlock(); err != nil {
			w.err = err
			return w.err
		}
	}
	w.cntEntry++
	return nil
}

// BlocksLen returns the number of blocks written so far in the table.
func (w *TableWriter) BlocksLen() int {
	n := w.indexBlock.cntEntry
	if w.dataBlock.cntEntry > 0 {
		n++
	}
	return n
}

// EntriesLen returns the number of entries added so far in the table.
func (w *TableWriter) EntriesLen() int {
	return w.cntEntry
}

// BytesLen returns the number of bytes written so far in the table.
func (w *TableWriter) BytesLen() int {
	return int(w.offset)
}

// Close finalize the table and release resources.
//
// It's not allowed to call Append after Close, but calling
// BlocksLen, EntriesLen, BytesLen is still valid after Close.
func (w *TableWriter) Close() error {
	defer func() {

	}()

	if w.err != nil {
		return w.err
	}

	// Finish last block if it has entries, or if no entry is added at all
	if w.dataBlock.cntEntry > 0 || w.cntEntry == 0 {
		if err := w.finishBlock(); err != nil {
			w.err = err
			return err
		}
	}
	if err := w.flushPendingBP(nil); err != nil {
		return err
	}

	// Filter block
	var filterBP blockPointer
	if err := w.filterBlock.finish(); err != nil {
		return err
	}
	if buf := &w.filterBlock.buf; buf.Len() > 0 {
		filterBP, w.err = w.writeBlock(buf, opt.NoCompression)
		if w.err != nil {
			return w.err
		}
	}

	// metaindex block
	if filterBP.length > 0 {
		key := []byte("filter." + w.filter.Name())
		n := encodeBlockPointer(w.scratch[:20], filterBP)
		if err := w.indexBlock.appendKV(key, w.scratch[:n]); err != nil {
			return err
		}
	}
	if err := w.dataBlock.finish(); err != nil {
		return err
	}
	metaindexBP, err := w.writeBlock(&w.dataBlock.buf, w.compression)
	if err != nil {
		w.err = err
		return w.err
	}

	// index block
	if err := w.indexBlock.finish(); err != nil {
		return err
	}
	indexBP, err := w.writeBlock(&w.indexBlock.buf, w.compression)
	if err != nil {
		w.err = err
		return w.err
	}

	// Write table footer
	footer := w.scratch[:footerLen]
	for i := range footer {
		footer[i] = 0
	}
	n := encodeBlockPointer(footer, metaindexBP)
	encodeBlockPointer(footer[n:], indexBP)
	copy(footer[footerLen-len(magicByte):], magicByte)
	if _, err := w.writer.Write(footer); err != nil {
		w.err = err
		return w.err
	}
	w.offset += uint64(footerLen)

	w.err = errors.New("granite/sstable: table writer is closed")
	return nil
}

// NewTableWriter creates a new TableWriter with the given options for the file.
func NewTableWriter(f io.Writer, o *opt.Options, pool *util.BufferPool, size int) *TableWriter {
	var bufBytes []byte
	if pool == nil {
		bufBytes = make([]byte, size)
	} else {
		bufBytes = pool.Get(size)
	}
	bufBytes = bufBytes[:0] // set length = 0

	w := &TableWriter{
		writer:          f,
		comparer:        o.GetComparer(),
		filter:          o.GetFilter(),
		compression:     o.GetCompressionType(),
		blockSize:       o.GetBlockSize(),
		comparerScratch: make([]byte, 0),
		bpool:           pool,
		dataBlock:       blockWriter{buf: *bytes.NewBuffer(bufBytes)},
	}
	// data block
	w.dataBlock.restartGap = o.GetBlockRestartGap()
	// The first 20-bytes are used for encoding block handle.
	w.dataBlock.scratch = w.scratch[20:]
	// index block
	w.indexBlock.restartGap = 1
	w.indexBlock.scratch = w.scratch[20:]
	// filter block
	if w.filter != nil {
		w.filterBlock.gen = w.filter.NewGenerator()
		w.filterBlock.baseLg = uint(o.GetFilterBaseLg())
		w.filterBlock.flush(0)
	}
	return w
}

// ==================== Utils ====================

func sharedPrefixLen(a, b []byte) (i int) {
	n := min(len(a), len(b))
	for i = 0; i < n && a[i] == b[i]; i++ {
	}
	return
}
