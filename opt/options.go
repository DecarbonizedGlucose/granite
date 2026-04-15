package opt

import (
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

var (
	DefaultBlockSize       = 4 * KiB
	DefaultBlockRestartGap = 16
	DefaultCompressionType = SnappyCompression
	DefaultFilterBaseLg    = 11
)

type Options struct {
	BlockRestartGap int
	BlockSize       int
	Comparer        comparer.Comparer
	Compression     CompressionType
	Filter          filter.FilterPolicy
	FilterBaseLg    int
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
