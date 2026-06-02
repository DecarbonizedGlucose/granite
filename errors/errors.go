package errors

import (
	"errors"
	"fmt"

	"github.com/DecarbonizedGlucose/granite/util"
)

// Key-value errors
var (
	ErrNotFound = errors.New("granite: key not found")
)

// Storage errors
var (
	ErrInvalidFile = errors.New("granite/storage: invalid file")
	ErrLocked      = errors.New("granite/storage: already locked")
	ErrClosed      = errors.New("granite/storage: closed")
)

// ErrManifestCorrupted records manifest corruption. This error will be
// wrapped with errors.ErrCorrupted.
type ErrManifestCorrupted struct {
	Field  string
	Reason string
}

func (e *ErrManifestCorrupted) Error() string {
	return fmt.Sprintf("leveldb: manifest corrupted (field '%s'): %s", e.Field, e.Reason)
}

func NewErrManifestCorrupted(fd util.FileDesc, field, reason string) error {
	return NewErrFileCorrupted(fd, &ErrManifestCorrupted{field, reason})
}

// SSTable errors
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

// SetFd trys to set file information to error.
func SetFd(err error, fd util.FileDesc) error {
	switch x := err.(type) {
	case *ErrFileCorrupted:
		x.Fd = fd
		return x
	default:
		return err
	}
}
