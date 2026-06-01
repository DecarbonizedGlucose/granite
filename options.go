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
	if newo.Strict == 0 {
		newo.Strict = opt.DefaultStrict
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

const optCachedLevel = 7

type cachedOptions struct {
	*opt.Options

	compactionExpandLimit []int
	compactionGPOverlaps  []int
	compactionSourceLimit []int
	compactionTableSize   []int
	compactionTotalSize   []int64
}

func (co *cachedOptions) cache() {
	co.compactionExpandLimit = make([]int, optCachedLevel)
	co.compactionGPOverlaps = make([]int, optCachedLevel)
	co.compactionSourceLimit = make([]int, optCachedLevel)
	co.compactionTableSize = make([]int, optCachedLevel)
	co.compactionTotalSize = make([]int64, optCachedLevel)

	for l := 0; l < optCachedLevel; l++ {
		co.compactionExpandLimit[l] = co.Options.GetCompactionExpandLimit(l)
		co.compactionGPOverlaps[l] = co.Options.GetCompactionGPOverlaps(l)
		co.compactionSourceLimit[l] = co.Options.GetCompactionSourceLimit(l)
		co.compactionTableSize[l] = co.Options.GetCompactionTableSize(l)
		co.compactionTotalSize[l] = co.Options.GetCompactionTotalSize(l)
	}
}

func (co *cachedOptions) GetCompactionExpandLimit(level int) int {
	if level < optCachedLevel {
		return co.compactionExpandLimit[level]
	}
	return co.Options.GetCompactionExpandLimit(level)
}

func (co *cachedOptions) GetCompactionGPOverlaps(level int) int {
	if level < optCachedLevel {
		return co.compactionGPOverlaps[level]
	}
	return co.Options.GetCompactionGPOverlaps(level)
}

func (co *cachedOptions) GetCompactionSourceLimit(level int) int {
	if level < optCachedLevel {
		return co.compactionSourceLimit[level]
	}
	return co.Options.GetCompactionSourceLimit(level)
}

func (co *cachedOptions) GetCompactionTableSize(level int) int {
	if level < optCachedLevel {
		return co.compactionTableSize[level]
	}
	return co.Options.GetCompactionTableSize(level)
}

func (co *cachedOptions) GetCompactionTotalSize(level int) int64 {
	if level < optCachedLevel {
		return co.compactionTotalSize[level]
	}
	return co.Options.GetCompactionTotalSize(level)
}
