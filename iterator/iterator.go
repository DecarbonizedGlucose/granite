package iterator

import (
	"errors"

	"github.com/DecarbonizedGlucose/granite/util"
)

var (
	ErrIterReleased = errors.New("granite/iterator: iterator released")
)

type CommonIterator interface {
	util.Releaser
	util.ReleaseSetter

	First() bool
	Last() bool
	Seek([]byte) bool
	Next() bool
	Prev() bool

	Valid() bool
	Error() error
}

type InternalIterator interface {
	CommonIterator

	Key() []byte
	Value() []byte
}

type emptyIterator struct {
	util.BasicReleaser
	err error
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

func NewEmptyIterator(err error) InternalIterator {
	return &emptyIterator{err: err}
}
