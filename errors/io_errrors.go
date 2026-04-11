package errors

import (
	"errors"
)

var (
	ErrNegativeRead = errors.New("granite/util.Buffer: reader returned negative count from Read")
)
