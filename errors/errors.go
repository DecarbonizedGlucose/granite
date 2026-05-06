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
	ErrInvalidInternalKeyLength = errors.New("granite: invalid key length")
	ErrInvalidInternalKeyType   = errors.New("granite: invalid key type")
)

// Journal errors
var (
	ErrJournalInvalidEntryType = errors.New("granite: invalid journal entry type")
	ErrJournalNilKey           = errors.New("granite: nil journal entry key")
	ErrJournalNilValue         = errors.New("granite: nil journal entry value")
)
