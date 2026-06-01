package iterator

import (
	"sync/atomic"

	gerrors "github.com/DecarbonizedGlucose/granite/errors"
)

type BasicArray interface {
	// Len returns length of the array
	Len() int

	// Search returns the smallest index of key >= the given key
	Search(key []byte) int
}

type Array interface {
	BasicArray

	Index(i int) (key, value []byte)
}

type ArrayIndexer interface {
	BasicArray

	Get(i int) InternalIterator
}

type basicArrayIterator struct {
	array  BasicArray
	pos    int
	err    error
	closed atomic.Bool
}

func (i *basicArrayIterator) First() bool {
	if i.Closed() {
		i.err = ErrIterClosed
		return false
	}

	if i.array.Len() == 0 {
		i.pos = -1
		return false
	}
	i.pos = 0
	return true
}

func (i *basicArrayIterator) Last() bool {
	if i.Closed() {
		i.err = ErrIterClosed
		return false
	}

	n := i.array.Len()
	if n == 0 {
		i.pos = 0
		return false
	}
	i.pos = n - 1
	return true
}

func (i *basicArrayIterator) Seek(key []byte) bool {
	if i.Closed() {
		i.err = ErrIterClosed
		return false
	}

	n := i.array.Len()
	if n == 0 {
		i.pos = 0
		return false
	}
	i.pos = i.array.Search(key)
	return i.pos < n
}

func (i *basicArrayIterator) Next() bool {
	if i.Closed() {
		i.err = ErrIterClosed
		return false
	}

	i.pos++
	if n := i.array.Len(); i.pos >= n {
		i.pos = n
		return false
	}
	return true
}

func (i *basicArrayIterator) Prev() bool {
	if i.Closed() {
		i.err = ErrIterClosed
		return false
	}

	i.pos--
	if i.pos < 0 {
		i.pos = -1
		return false
	}
	return true
}

func (i *basicArrayIterator) Valid() bool {
	return i.pos >= 0 && i.pos < i.array.Len() && !i.Closed()
}

func (i *basicArrayIterator) Error() error {
	return i.err
}

func (i *basicArrayIterator) Close() error {
	if i.closed.Load() {
		return gerrors.ErrClosed
	}
	i.closed.Store(true)
	return nil
}

func (i *basicArrayIterator) Closed() bool {
	return i.closed.Load()
}

type arrayIterator struct {
	basicArrayIterator
	array      Array
	pos        int
	key, value []byte
}

func (i *arrayIterator) updateKV() {
	if i.pos == i.basicArrayIterator.pos {
		return
	}
	i.pos = i.basicArrayIterator.pos
	if i.Valid() {
		i.key, i.value = i.array.Index(i.pos)
	} else {
		i.key, i.value = nil, nil
	}
}

func (i *arrayIterator) Key() []byte {
	i.updateKV()
	return i.key
}

func (i *arrayIterator) Value() []byte {
	i.updateKV()
	return i.value
}

type arrayIteratorIndexer struct {
	basicArrayIterator
	array ArrayIndexer
}

func (i *arrayIteratorIndexer) Get() InternalIterator {
	if i.Valid() {
		return i.array.Get(i.basicArrayIterator.pos)
	}
	return nil
}

// Returns an iterator from the given array.
func NewArrayIterator(array Array) InternalIterator {
	return &arrayIterator{
		basicArrayIterator: basicArrayIterator{array: array, pos: -1},
		array:              array,
		pos:                -1,
	}
}

// Returns an index iterator from the given array.
func NewArrayIndexer(array ArrayIndexer) IteratorIndexer {
	return &arrayIteratorIndexer{
		basicArrayIterator: basicArrayIterator{array: array, pos: -1},
		array:              array,
	}
}
