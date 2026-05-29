package storage

import (
	"io"

	"github.com/DecarbonizedGlucose/granite/util"
)

type Syncer interface {
	Sync() error
}

type Reader interface {
	io.ReadSeeker
	io.ReaderAt
	io.Closer
}

type Writer interface {
	io.WriteCloser
	Syncer
}

type Locker interface {
	Unlock()
}

type Storage interface {
	// Lock locks the storage, any subsequent call to Lock will be failed
	// until the locker is released.
	Lock() (Locker, error)

	// Log logs a string.
	// Write to file, stdout, or do nothing.
	Log(str string)

	// SetMeta store 'fd', which can be got by GetMeta.
	// This function should be implemented as atomic.
	SetMeta(fd util.FileDesc) error

	// GetMeta returns the 'fd' stored in meta. Returns os.ErrNotExist if
	// meta does not store any 'fd', or 'fd' point to a non-existent file.
	GetMeta() (util.FileDesc, error)

	// List returns fds that match the given file types.
	// The file types may be OR'ed together.
	List(ft util.FileType) ([]util.FileDesc, error)

	// Open opens a file with 'fd' read-only. Returns os.ErrNotExist
	// if the file does not exist.
	// Returns ErrClosed if the underlying storage is closed.
	Open(fd util.FileDesc) (Reader, error)

	// Create creates a file with 'fd', truncate if already
	// exist and opens write-only.
	// Returns ErrClosed if the underlying storage is closed.
	Create(fd util.FileDesc) (Writer, error)

	// Remove removes the file with 'fd'.
	// Returns ErrClosed if the underlying storage is closed.
	Remove(fd util.FileDesc) error

	// Rename renames the file from 'oldFd' to 'newFd'.
	// Returns ErrClosed if the underlying storage is closed.
	Rename(oldFd, newFd util.FileDesc) error

	// Close closes the storage.
	// It is valid to call Close multiple times. Other methods should not
	// be called after Close.
	Close() error
}
