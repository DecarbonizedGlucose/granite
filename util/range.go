package util

import (
	"slices"
)

// Range represents a key range with a start and limit key.
// [Start, Limit) - Start is inclusive, Limit is exclusive.
type Range struct {
	Start []byte
	Limit []byte
}

func (r *Range) IsEmpty() bool {
	if len(r.Start) == 0 && len(r.Limit) == 0 {
		return true
	}
	return slices.Equal(r.Start, r.Limit)
}

func RangePrefix(prefix []byte) *Range {
	var end []byte
	for i := len(prefix) - 1; i >= 0; i-- {
		c := prefix[i]
		if c < 0xff {
			end = make([]byte, i+1)
			copy(end, prefix)
			end[i] = c + 1
			break
		}
	}
	return &Range{
		Start: prefix,
		Limit: end,
	}
}
