package iterator

type InternalIterator interface {
	First() bool
	Last() bool
	Seek(key []byte) bool
	Next() bool
	Prev() bool

	Key() []byte
	Value() []byte
	Valid() bool

	Error() error
	Close() error
	Closed() bool
}
