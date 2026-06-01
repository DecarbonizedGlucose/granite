package opt

import (
	"math"

	"github.com/DecarbonizedGlucose/granite/cache"
	"github.com/DecarbonizedGlucose/granite/comparer"
	"github.com/DecarbonizedGlucose/granite/filter"
)

const (
	KiB = 1024
	MiB = KiB * 1024
	GiB = MiB * 1024
)

type CompressionType int

const (
	DefaultCompression CompressionType = 0
	NoCompression      CompressionType = 1
	SnappyCompression  CompressionType = 2
	ZSTDCompression    CompressionType = 3
)

type Strict uint // DB strict level

const (
	StrictManifest Strict = 1 << iota
	StrictJournalChecksum
	StrictJournal
	StrictBlockChecksum
	StrictCompaction
	StrictReader
	StrictRecovery
	StrictOverride
	StrictAll     = StrictManifest | StrictJournalChecksum | StrictJournal | StrictBlockChecksum | StrictCompaction | StrictReader | StrictRecovery
	DefaultStrict = StrictJournalChecksum | StrictBlockChecksum | StrictCompaction | StrictReader
	NoStrict      = ^StrictAll
)

var (
	DefaultBlockCacher                   = LRUCacher
	DefaultBlockCacheCapacity            = 8 * MiB
	DefaultBlockSize                     = 4 * KiB
	DefaultBlockRestartGap               = 16
	DefaultCompactionExpandLimitFactor   = 25
	DefaultCompactionGPOverlapsFactor    = 10
	DefaultCompactionSourceLimitFactor   = 1
	DefaultCompactionTableSize           = 2 * MiB
	DefaultCompactionTableSizeMultiplier = 1.0
	DefaultCompactionTotalSize           = 10 * MiB
	DefaultCompactionTotalSizeMultiplier = 10.0
	DefaultCompressionType               = SnappyCompression
	DefaultFilterBaseLg                  = 11
	DefaultOpenFilesCacher               = LRUCacher
	DefaultOpenFilesCacheCapacity        = 4 * MiB
	DefaultWriteBufferSize               = 4 * MiB
)

type Cacher interface {
	New(capacity int) cache.Cacher
}

type cacherFunc struct {
	NewFunc func(capacity int) cache.Cacher
}

func (f *cacherFunc) New(capacity int) cache.Cacher {
	if f != nil && f.NewFunc != nil {
		return f.NewFunc(capacity)
	}
	return nil
}

func CacherFunc(f func(capacity int) cache.Cacher) Cacher {
	return &cacherFunc{NewFunc: f}
}

type passthroughCacher struct {
	Cacher cache.Cacher
}

func (p *passthroughCacher) New(capacity int) cache.Cacher {
	return p.Cacher
}

func PassthroughCacher(c cache.Cacher) Cacher {
	return &passthroughCacher{Cacher: c}
}

// NewLRU creates LRU 'passthrough cacher'
func NewLRU(capacity int) Cacher {
	return PassthroughCacher(cache.NewLRU(capacity))
}

var (
	LRUCacher = CacherFunc(cache.NewLRU)
	NoCacher  = CacherFunc(nil)
)

type Options struct {
	// AltFilters defines one or more 'alternative filters'.
	AltFilters []filter.FilterPolicy
	// BlockCacher provides cache algorithm for 'sorted table' blocks caching.
	BlockCacher Cacher
	// BlockRestartGap is the capacity of the 'sorted table' block caching.
	BlockCacheCapacity int
	// BlockCacheEvictRemoved wllows enable forced-eviction on cached block
	// belonging to removed 'sorted table'.
	BlockCacheEvictRemoved bool
	BlockRestartGap        int
	BlockSize              int
	// CompactionExpandLimitFactor limits compaction size after expanded.
	CompactionExpandLimitFactor int
	// CompactionGPOverlapsFactor limits overlaps in grandparent (Level + 2) that a
	// single 'sorted table' generates.
	CompactionGPOverlapsFactor int
	// CompactionSourceLimitFactor limits compaction source size.
	// This doesn't apply to level-0.
	CompactionSourceLimitFactor int
	// CompactionTableSize limits size of 'sorted table' that compaction generates.
	CompactionTableSize int
	// CompactionTableSizeMultiplier defines multiplier for CompactionTableSize.
	CompactionTableSizeMultiplier float64
	// CompactionTableSizeMultiplierPerLevel defines per-level
	// multiplier for CompactionTableSize.
	// Use zero to skip a level.
	CompactionTableSizeMultiplierPerLevel []float64
	// CompactionTotalSize limits total size of 'sorted table' for each level.
	CompactionTotalSize int
	// CompactionTotalSizeMultiplier defines multiplier for CompactionTotalSize.
	CompactionTotalSizeMultiplier float64
	// CompactionTotalSizeMultiplierPerLevel defines per-level multiplier for
	// CompactionTotalSize.
	// Use zero to skip a level.
	CompactionTotalSizeMultiplierPerLevel []float64
	Comparer                              comparer.Comparer
	Compression                           CompressionType
	// allows disable use of cache.Cache for 'sorted table' blocks
	DisableBlockCache bool
	// OpenFilesCacher provides cache algorithm for open files caching.
	OpenFilesCacher Cacher
	// OpenFilesCacheCapacity is the capacity for open files caching.
	OpenFilesCacheCapacity int
	// Whether the database is in read-only mode.
	ReadOnly bool
	// The DB strict level.
	Strict          Strict
	WriteBufferSize int
}

func (o *Options) GetAltFilters() []filter.FilterPolicy {
	if o == nil {
		return nil
	}
	return o.AltFilters
}

func (o *Options) GetBlockCacher() Cacher {
	if o == nil || o.BlockCacher == nil {
		return DefaultBlockCacher
	}
	return o.BlockCacher
}

func (o *Options) GetBlockCacheCapacity() int {
	if o == nil || o.BlockCacheCapacity == 0 {
		return DefaultBlockCacheCapacity
	} else if o.BlockCacheCapacity < 0 {
		return 0
	}
	return o.BlockCacheCapacity
}

func (o *Options) GetBlockCacheEvictRemoved() bool {
	if o == nil {
		return false
	}
	return o.BlockCacheEvictRemoved
}

func (o *Options) GetBlockRestartGap() int {
	if o == nil || o.BlockRestartGap <= 0 {
		return DefaultBlockRestartGap
	}
	return o.BlockRestartGap
}

func (o *Options) GetBlockSize() int {
	if o == nil || o.BlockSize <= 0 {
		return DefaultBlockSize
	}
	return o.BlockSize
}

func (o *Options) GetCompactionExpandLimit(level int) int {
	factor := DefaultCompactionExpandLimitFactor
	if o != nil && o.CompactionExpandLimitFactor > 0 {
		factor = o.CompactionExpandLimitFactor
	}
	return o.GetCompactionTableSize(level+1) * factor
}

func (o *Options) GetCompactionGPOverlaps(level int) int {
	factor := DefaultCompactionGPOverlapsFactor
	if o != nil && o.CompactionGPOverlapsFactor > 0 {
		factor = o.CompactionGPOverlapsFactor
	}
	return o.GetCompactionTableSize(level+2) * factor
}

func (o *Options) GetCompactionSourceLimit(level int) int {
	factor := DefaultCompactionSourceLimitFactor
	if o != nil && o.CompactionSourceLimitFactor > 0 {
		factor = o.CompactionSourceLimitFactor
	}
	return o.GetCompactionTableSize(level+1) * factor
}

func (o *Options) GetCompactionTableSize(level int) int {
	var (
		base = DefaultCompactionTableSize
		mult float64
	)
	if o != nil {
		if o.CompactionTableSize > 0 {
			base = o.CompactionTableSize
		}
		if level < len(o.CompactionTableSizeMultiplierPerLevel) && o.CompactionTableSizeMultiplierPerLevel[level] > 0 {
			mult = o.CompactionTableSizeMultiplierPerLevel[level]
		} else if o.CompactionTableSizeMultiplier > 0 {
			mult = math.Pow(o.CompactionTableSizeMultiplier, float64(level))
		}
	}
	if mult == 0 {
		mult = math.Pow(DefaultCompactionTableSizeMultiplier, float64(level))
	}
	return int(float64(base) * mult)
}

func (o *Options) GetCompactionTotalSize(level int) int64 {
	var (
		base = DefaultCompactionTotalSize
		mult float64
	)
	if o != nil {
		if o.CompactionTotalSize > 0 {
			base = o.CompactionTotalSize
		}
		if level < len(o.CompactionTotalSizeMultiplierPerLevel) && o.CompactionTotalSizeMultiplierPerLevel[level] > 0 {
			mult = o.CompactionTotalSizeMultiplierPerLevel[level]
		} else if o.CompactionTotalSizeMultiplier > 0 {
			mult = math.Pow(o.CompactionTotalSizeMultiplier, float64(level))
		}
	}
	if mult == 0 {
		mult = math.Pow(DefaultCompactionTotalSizeMultiplier, float64(level))
	}
	return int64(float64(base) * mult)
}

func (o *Options) GetComparer() comparer.Comparer {
	if o == nil || o.Comparer == nil {
		return comparer.DefaultComparer
	}
	return o.Comparer
}

func (o *Options) GetCompressionType() CompressionType {
	if o == nil {
		return DefaultCompressionType
	}
	return o.Compression
}

func (o *Options) GetDisableBlockCache() bool {
	if o == nil {
		return false
	}
	return o.DisableBlockCache
}

func (o *Options) GetFilter() filter.FilterPolicy {
	if o == nil || o.Filter == nil {
		return nil
	}
	return o.Filter
}

func (o *Options) GetFilterBaseLg() int {
	if o == nil || o.FilterBaseLg <= 0 {
		return DefaultFilterBaseLg
	}
	return o.FilterBaseLg
}

func (o *Options) GetOpenFilesCacher() Cacher {
	if o == nil || o.OpenFilesCacher == nil {
		return DefaultOpenFilesCacher
	}
	return o.OpenFilesCacher
}

func (o *Options) GetOpenFilesCacheCapacity() int {
	if o == nil || o.OpenFilesCacheCapacity == 0 {
		return DefaultOpenFilesCacheCapacity
	} else if o.OpenFilesCacheCapacity < 0 {
		return 0
	}
	return o.OpenFilesCacheCapacity
}

func (o *Options) GetReadOnly() bool {
	if o == nil {
		return false
	}
	return o.ReadOnly
}

func (o *Options) GetStrict(strict Strict) bool {
	if o == nil || o.Strict == 0 {
		return DefaultStrict&strict != 0
	}
	return o.Strict&strict != 0
}

func (o *Options) GetWriteBufferSize() int {
	if o == nil || o.WriteBufferSize <= 0 {
		return DefaultWriteBufferSize
	}
	return o.WriteBufferSize
}

type ReadOptions struct {
	DontFillCache bool
	Strict        Strict
}

func (ro *ReadOptions) GetDontFillCache() bool {
	if ro == nil {
		return false
	}
	return ro.DontFillCache
}

func (ro *ReadOptions) GetStrict(strict Strict) bool {
	if ro == nil {
		return false
	}
	return ro.Strict&strict != 0
}

type WriteOptions struct {
	SyncEachTime bool
}

func (wo *WriteOptions) GetSyncEachTime() bool {
	if wo == nil {
		return false
	}
	return wo.SyncEachTime
}

func GetStrict(o *Options, ro *ReadOptions, strict Strict) bool {
	if ro.GetStrict(StrictOverride) {
		return ro.GetStrict(strict)
	}
	return o.GetStrict(strict) || ro.GetStrict(strict)
}
