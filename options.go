package granite

import (
	"github.com/DecarbonizedGlucose/granite/filter"
	"github.com/DecarbonizedGlucose/granite/opt"
)

func dupOptions(o *opt.Options) *opt.Options {
	newo := &opt.Options{}
	if o != nil {
		*newo = *o
	}
	return newo
}

func (s *session) setOptions(o *opt.Options) {
	no := dupOptions(o)
	// alternative filters
	if filters := no.GetAltFilters(); len(filters) > 0 {
		no.AltFilters = make([]filter.FilterPolicy, len(filters))
		for i, f := range filters {
			no.AltFilters[i] = &iFilter{f}
		}
	}
	// comparer
	s.ikc = &ikComparer{no.GetComparer()}

}

type cachedOptions struct {
	*opt.Options

	compactionExpandLimit []int
	compactionGPOverlaps  []int
	compactionSourceLimit []int
	compactionTableSize   []int
	compactionTotalSize   []int64
}
