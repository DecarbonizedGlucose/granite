package granite

import (
	"errors"

	gerrors "github.com/DecarbonizedGlucose/granite/errors"
)

// Database-level common errors
var (
	ErrNotFound         = gerrors.ErrNotFound
	ErrReadOnly         = errors.New("granite: database is read-only")
	ErrSnapshotReleased = errors.New("granite: snapshot is released")
	ErrClosed           = errors.New("granite: database is closed")
	ErrIterReleased     = errors.New("granite: iterator is released")
)
