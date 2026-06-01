package iterator

import "errors"

var (
	ErrIterClosed = errors.New("granite/iterator: iterator closed")
)

type CommonIterator interface {
	First() bool
	Last() bool
	Seek([]byte) bool
	Next() bool
	Prev() bool

	Valid() bool
	Error() error
	Close() error
	Closed() bool
}

type InternalIterator interface {
	CommonIterator

	Key() []byte
	Value() []byte
}

type emptyIterator struct {
	err    error
	closed bool
}

func (*emptyIterator) Valid() bool        { return false }
func (i *emptyIterator) First() bool      { return false }
func (i *emptyIterator) Last() bool       { return false }
func (i *emptyIterator) Seek([]byte) bool { return false }
func (i *emptyIterator) Next() bool       { return false }
func (i *emptyIterator) Prev() bool       { return false }
func (*emptyIterator) Key() []byte        { return nil }
func (*emptyIterator) Value() []byte      { return nil }
func (i *emptyIterator) Error() error     { return i.err }
func (i *emptyIterator) Close() error     { return i.err }
func (i *emptyIterator) Closed() bool     { return i.closed }

func NewEmptyIterator(err error) InternalIterator {
	return &emptyIterator{err: err}
}
