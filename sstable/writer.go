package sstable

import (
	"encoding/binary"

	"github.com/DecarbonizedGlucose/granite/filter"
	"github.com/DecarbonizedGlucose/granite/util"
)

// ==================== Block Writer ====================

// Writer for any type of block
type blockWriter struct {
	restartGap int // distance between two restart point
	buf        util.Buffer
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
		buf4 := w.buf.Allocate(4)
		binary.LittleEndian.PutUint32(buf4, x)
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
	buf util.Buffer
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
		buf4 := w.buf.Allocate(4)
		binary.LittleEndian.PutUint32(buf4, x)
	}
	return w.buf.WriteByte(byte(w.baseLg))
}

// ==================== Utils ====================

func sharedPrefixLen(a, b []byte) (i int) {
	n := min(len(a), len(b))
	for i = 0; i < n && a[i] == b[i]; i++ {
	}
	return
}
