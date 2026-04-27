package errors

import (
	"errors"
)

// I/O errors
var (
	ErrNegativeRead = errors.New("granite/util.Buffer: reader returned negative count from Read")
)

// Key-value errors
var (
	ErrNotFound = errors.New("granitedb: key not found")
)

// Iterator errors
var (
	ErrIterClosed = errors.New("granite/iterator: iterator is closed")
)

// SSTable errors
var (
	ErrFileBroken = errors.New("granite/sstable: broken file")
)

// Internal Key errors
var (
	ErrInvalidKeyLength = errors.New("granite: invalid key length")
	ErrInvalidKeyType   = errors.New("granite: invalid key type")
)
