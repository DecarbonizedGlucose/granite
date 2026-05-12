package errors

import (
	"errors"
	"fmt"

	"github.com/DecarbonizedGlucose/granite/util"
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
	ErrTableCorrupted = errors.New("granite/sstable: broken sstable")
)

type ErrFileCorrupted struct {
	Fd  util.FileDesc
	Err error
}

func (e *ErrFileCorrupted) Error() string {
	if !e.Fd.Zero() {
		return fmt.Sprintf("%v [file=%v]", e.Err, e.Fd)
	}
	return e.Err.Error()
}

func NewErrFileCorrupted(fd util.FileDesc, err error) error {
	return &ErrFileCorrupted{fd, err}
}

func IsCorrupted(err error) bool {
	switch err.(type) {
	case *ErrFileCorrupted:
		return true
	default:
		return false
	}
}

// Internal Key errors
var (
	ErrInvalidInternalKeyLength = errors.New("granite: invalid key length")
	ErrInvalidInternalKeyType   = errors.New("granite: invalid key type")
)

// Journal errors
var (
	ErrJournalInvalidEntryType = errors.New("granite: invalid journal entry type")
	ErrJournalNilKey           = errors.New("granite: nil journal entry key")
	ErrJournalNilValue         = errors.New("granite: nil journal entry value")
)
