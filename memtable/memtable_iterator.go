package memtable

import (
	"sync/atomic"

	gerrors "github.com/DecarbonizedGlucose/granite/errors"
	"github.com/DecarbonizedGlucose/granite/iterator"
	"github.com/DecarbonizedGlucose/granite/util"
)

type direction bool

type mtIter struct {
	m                *MemTable
	r                *util.Range
	curNodeIdx       int
	lastDire         iterator.Direction
	curKey, curValue []byte
	closed           atomic.Bool
	err              error
}

func (i *mtIter) peek(checkStart, checkLimit bool) bool {
	if i.curNodeIdx != 0 {
		ko := i.m.nodeData[i.curNodeIdx]
		vo := i.m.nodeData[i.curNodeIdx+nKey]
		i.curKey = i.m.kvData[ko:vo]
		if i.r != nil {
			switch {
			case checkStart && i.r.Limit != nil && i.m.cmp.Compare(i.curKey, i.r.Limit) >= 0:
				fallthrough
			case checkLimit && i.r.Start != nil && i.m.cmp.Compare(i.curKey, i.r.Start) < 0:
				i.curNodeIdx = 0
				goto bail
			}
		}
		i.curValue = i.m.kvData[vo : vo+i.m.nodeData[i.curNodeIdx+nVal]]
	}
bail:
	i.curKey = nil
	i.curValue = nil
	return false
}

// First moves the iterator to the first key and returns true if the iterator is valid.
func (i *mtIter) First() bool {
	if i.Closed() {
		i.err = gerrors.ErrIterClosed
		return false
	}
	i.lastDire = iterator.Forward
	i.m.mu.RLock()
	defer i.m.mu.RUnlock()
	if i.r != nil && i.r.Start != nil {
		i.curNodeIdx, _ = i.m.findGE(i.r.Start, false)
	} else {
		i.curNodeIdx = i.m.nodeData[nNext] // head node's next[0]
	}
	return i.peek(false, true) // prevent the key >= limit
}

// Last moves the iterator to the last key and returns true if the iterator is valid.
func (i *mtIter) Last() bool {
	if i.Closed() {
		i.err = gerrors.ErrIterClosed
		return false
	}
	i.lastDire = iterator.Backward
	i.m.mu.RLock()
	defer i.m.mu.RUnlock()
	if i.r != nil && i.r.Limit != nil {
		i.curNodeIdx = i.m.findLT(i.r.Limit)
	} else {
		i.curNodeIdx = i.m.findLast()
	}
	return i.peek(true, false) // prevent the key < start
}

// Seek moves the iterator to the first key that is >= `key`
// and returns true if the iterator is valid.
func (i *mtIter) Seek(key []byte) bool {
	if i.Closed() {
		i.err = gerrors.ErrIterClosed
		return false
	}
	i.lastDire = iterator.Forward
	i.m.mu.RLock()
	defer i.m.mu.RUnlock()
	if i.r != nil && i.r.Start != nil && i.m.cmp.Compare(key, i.r.Start) < 0 {
		key = i.r.Start // key < start, too little, seek to start
	}
	i.curNodeIdx, _ = i.m.findGE(key, false)
	return i.peek(false, true)
}

// Prev moves the iterator to the previous key and returns true if the iterator is valid.
func (i *mtIter) Prev() bool {
	if i.Closed() {
		i.err = gerrors.ErrIterClosed
		return false
	}
	if i.curNodeIdx == 0 {
		if i.lastDire == iterator.Forward {
			return i.Last() // iter not valid, but permitted to seek from back
		}
		return false
	}
	i.lastDire = iterator.Backward
	i.m.mu.RLock()
	defer i.m.mu.RUnlock()
	i.curNodeIdx = i.m.findLT(i.curKey)
	return i.peek(true, false)
}

// Next moves the iterator to the next key and returns true if the iterator is valid.
func (i *mtIter) Next() bool {
	if i.Closed() {
		i.err = gerrors.ErrIterClosed
		return false
	}
	if i.curNodeIdx == 0 {
		if i.lastDire == iterator.Backward {
			return i.First() // iter not valid, but permitted to seek from front
		}
		return false
	}
	i.lastDire = iterator.Forward
	i.m.mu.RLock()
	defer i.m.mu.RUnlock()
	i.curNodeIdx = i.m.nodeData[i.curNodeIdx+nNext] // next[0], no need to compare
	return i.peek(false, true)
}

// Key returns the current key. If the iterator is invalid, it returns nil.
func (i *mtIter) Key() []byte {
	// If the iterator is invalid, key will be nil.
	return i.curKey
}

// Value returns the current value. If the iterator is invalid, it returns nil.
func (i *mtIter) Value() []byte {
	// If the iterator is invalid, value will be nil.
	return i.curValue
}

// Valid returns true if the iterator is valid.
func (i *mtIter) Valid() bool {
	return !i.Closed() && i.curNodeIdx != 0
}

// Error returns the error of the iterator, if any.
func (i *mtIter) Error() error {
	return i.err
}

// Close closes the iterator and releases any resources associated with it.
func (i *mtIter) Close() error {
	if i.Closed() {
		return gerrors.ErrIterClosed
	}
	i.m = nil
	i.r = nil
	i.curKey = nil
	i.curValue = nil
	i.closed.Store(true)
	return nil
}

// Closed returns true if the iterator is closed.
func (i *mtIter) Closed() bool {
	return i.closed.Load()
}

func NewMTIter() iterator.InternalIterator {
	return &mtIter{}
}
