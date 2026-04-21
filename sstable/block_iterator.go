package sstable

import (
	"sync/atomic"

	gerrors "github.com/DecarbonizedGlucose/granite/errors"
	"github.com/DecarbonizedGlucose/granite/iterator"
)

type blockIter struct {
	b      *block
	reader *TableReader

	key, value []byte // current full key and value
	dire       iterator.Direction
	// next entry's start offset, after Next() or
	// current entry's end offset, after Prev()
	offset int
	// current entry's start offset, after Next()
	prevOffset int

	// cache for backward iteration,
	// [offset0, kOff0, vOff0, vLen0, kOff1, vOff1, vLen1, ...]
	prevNodes []int
	// all keys cached pieced together
	prevKeys []byte

	riStart, riLimit int // restart index range [start, limit)
	recentRestartIdx int // recent restart index before current entry

	bsRestartOffset int // restart point offset before `offsetStart`
	offsetLimit     int // first entry that can not be shown
	offsetStart     int // first entry that can be shown in range

	closed atomic.Bool
	err    error
}

func (i *blockIter) reset() {

}

func (i *blockIter) First() bool {
	if i.err != nil {
		return false
	} else if i.dire == iterator.Released {
		i.err = gerrors.ErrIterClosed
		return false
	}

	if i.dire == iterator.Backward {
		i.prevNodes = i.prevNodes[:0] // cache expired
		i.prevKeys = i.prevKeys[:0]
	}
	i.dire = iterator.SOI
	return i.Next()
}

func (i *blockIter) Last() bool {
	if i.err != nil {
		return false
	} else if i.dire == iterator.Released {
		i.err = gerrors.ErrIterClosed
		return false
	}

	if i.dire == iterator.Backward {
		i.prevNodes = i.prevNodes[:0]
		i.prevKeys = i.prevKeys[:0]
	}
	i.dire = iterator.EOI
	return i.Prev()
}

func (i *blockIter) Seek(key []byte) bool {
	if !i.Valid() {
		return false
	} else if i.dire == iterator.Released {
		i.err = gerrors.ErrIterClosed
		return false
	}

	newRestartIdx, newOff := i.b.recentRestart(i.reader.comparer, i.riStart, i.riLimit, key)
	i.recentRestartIdx = newRestartIdx
	i.offset = max(newOff, i.bsRestartOffset)
	if i.dire == iterator.SOI || i.dire == iterator.EOI {
		i.dire = iterator.Forward
	}
	for i.Next() {
		if i.reader.comparer.Compare(i.key, key) >= 0 {
			return true
		}
	}
	return false
}

func (i *blockIter) Next() bool {
	if !i.Valid() {
		return false
	} else if i.dire == iterator.Released {
		i.err = gerrors.ErrIterClosed
		return false
	}

	if i.dire == iterator.SOI { // before range first
		i.recentRestartIdx = i.riStart
		i.offset = i.bsRestartOffset
	} else if i.dire == iterator.Backward {
		i.prevNodes = i.prevNodes[:0] // cache expired
		i.prevKeys = i.prevKeys[:0]
	}

	// skip entries which will not be shown in range
	for i.offset < i.offsetStart {
		key, value, kShared, size, err := i.b.getEntry(i.offset)
		if err != nil {
			i.err = err // TODO: file would be broken
			return false
		}
		if size == 0 {
			// iteration ends
			i.dire = iterator.EOI
			return false
		}
		i.key = append(i.key[:kShared], key...)
		i.value = value
		i.offset += size
	}
	if i.offset >= i.offsetLimit {
		i.dire = iterator.EOI
		if i.offset > i.offsetLimit {
			i.err = gerrors.ErrFileBroken // TODO: file would be broken, should bring more infomation
		}
		return false
	}
	// decode the entry at offset
	key, value, kShared, size, err := i.b.getEntry(i.offset)
	if err != nil {
		i.err = err // TODO: file would be broken
		return false
	}
	if size == 0 {
		// iteration ends
		i.dire = iterator.EOI
		return false
	}
	i.key = append(i.key[:kShared], key...)
	i.value = value
	i.prevOffset = i.offset
	i.offset += size
	i.dire = iterator.Forward
	return true
}

func (i *blockIter) Prev() bool {
	if !i.Valid() {
		return false
	} else if i.dire == iterator.Released {
		i.err = gerrors.ErrIterClosed
		return false
	}

	var newRestartIdx int
	if i.dire == iterator.Forward {
		// change direction
		i.offset = i.prevOffset
		if i.offset == i.offsetStart {
			i.dire = iterator.SOI
			return false
		}
		// may crossed the restart point backward, find a new one
		newRestartIdx = i.b.getRestartIndex(i.recentRestartIdx, i.riLimit, i.offset)
		i.dire = iterator.Backward
	} else if i.dire == iterator.EOI {
		i.recentRestartIdx = i.riLimit
		i.offset = i.offsetLimit
		if i.offset == i.offsetStart {
			i.dire = iterator.SOI
			return false
		}
		newRestartIdx = i.riLimit - 1
		i.dire = iterator.Backward
	} else if len(i.prevNodes) == 1 {
		i.offset = i.prevNodes[0]
		i.prevNodes = i.prevNodes[:0]
		if i.recentRestartIdx == i.riStart {
			i.dire = iterator.SOI
			return false
		}
		i.recentRestartIdx--
		newRestartIdx = i.recentRestartIdx
	} else {
		// For consecutive Prev() fast paths,
		// pop prevNodes
		n := len(i.prevNodes) - 3
		node := i.prevNodes[n:]
		i.prevNodes = i.prevNodes[:n]
		// get key
		ko := node[0]
		i.key = append(i.key[:0], i.prevKeys[ko:]...)
		// get value
		vo := node[1]
		vl := node[2]
		i.value = i.b.data[vo : vo+vl]
		i.offset = vo + vl
		return true
	}
	// build cache
	i.key = i.key[:0]
	i.value = nil
	newOff := i.b.getRestartOffset(newRestartIdx)
	if newOff == i.offset {
		newRestartIdx-- // read backward over the restart point
		if newRestartIdx < 0 {
			i.dire = iterator.SOI
			return false
		}
		newOff = i.b.getRestartOffset(newRestartIdx)
	}
	i.prevNodes = append(i.prevNodes, newOff)
	for {
		key, value, kShared, size, err := i.b.getEntry(i.offset)
		if err != nil {
			i.err = err // TODO: file would be broken
			return false
		}
		if newOff >= i.offsetStart {
			if i.value != nil {
				i.prevNodes = append(i.prevNodes,
					len(i.prevKeys),     // key offset in prevKeys
					newOff-len(i.value), // value offset in block data
					len(i.value),        // value len
				)
				i.prevKeys = append(i.prevKeys, i.key...)
			}
			i.value = value
		}
		i.key = append(i.key[:kShared], key...)
		newOff += size
		if newOff >= i.offset {
			if newOff > i.offset {
				i.err = gerrors.ErrFileBroken // TODO: file would be broken, should bring more infomation
				return false
			}
			break
		}
	}
	i.recentRestartIdx = newRestartIdx
	i.offset = newOff
	return true
}

func (i *blockIter) Key() []byte {
	if !i.Valid() {
		return nil
	}
	return i.key
}

func (i *blockIter) Value() []byte {
	if !i.Valid() {
		return nil
	}
	return i.value
}

func (i *blockIter) Valid() bool {
	return i.err == nil && i.dire.Valid()
}

func (i *blockIter) Error() error {
	return i.err
}

func (i *blockIter) Close() error {
	if i.Closed() {
		return gerrors.ErrIterClosed
		// TODO: should bring more infomation
	}
	// TODO: Release the block's buffer back to the pool
	i.closed.Store(true)
	return nil
}

func (i *blockIter) Closed() bool {
	return i.closed.Load()
}

func (i *blockIter) isAtFirst() bool {
	return false
}

func (i *blockIter) isAtLast() bool {
	return false
}
