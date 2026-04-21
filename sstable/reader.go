package sstable

import (
	"sync"

	"github.com/DecarbonizedGlucose/granite/comparer"
	gerrors "github.com/DecarbonizedGlucose/granite/errors"
)

// Reader errors
var (
	ErrNotFound = gerrors.ErrNotFound
)

// ==================== Table Reader ====================

type TableReader struct {
	mu sync.RWMutex

	comparer comparer.Comparer
}
