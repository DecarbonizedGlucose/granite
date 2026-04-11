package errors

import (
	"errors"
)

var (
	ErrNotFound = errors.New("granitedb: key not found")
)
