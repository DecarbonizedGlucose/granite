package filter

import (
	"github.com/DecarbonizedGlucose/granite/util"
)

func bloomHash(key []byte) uint32 {
	seed := uint32(0xbc9f1d34) // from MurmurHash2
	return util.Hash(key, seed)
}

type bloomFilter int

func (bloomFilter) Name() string {
	return "granite.DefaultBloomFilter"
}

func (f bloomFilter) MayContain(filter, key []byte) bool {
	nBytes := len(filter) - 1
	if nBytes == 0 {
		return false
	}
	nBits := uint32(nBytes * 8)
	k := filter[nBytes]
	if k > 30 {
		// Reserved for potentially new encodings for short bloom filters.
		// Consider it a match.
		return true
	}

	h := bloomHash(key)
	delta := h>>17 | h<<15 // rotate right 17 bits
	for j := uint8(0); j < k; j++ {
		bitPos := h % nBits
		if (filter[bitPos/8] & (1 << (bitPos % 8))) == 0 {
			return false
		}
		h += delta
	}

	return true
}

func (f bloomFilter) NewGenerator() FilterGenerator {
	// Round down to reduce probing cost a little bit.
	k := uint8(f * 69 / 100) // 0.69 is about ln(2)
	if k < 1 {
		k = 1
	} else if k > 30 {
		k = 30
	}
	return &bloomFilterGenerator{
		n: int(f),
		k: k,
	}
}

type bloomFilterGenerator struct {
	n int   // bits per key
	k uint8 // number of hash functions

	keyHashes []uint32 // temp storage for hashes of added keys
}

func (g *bloomFilterGenerator) Add(key []byte) {
	// Double hash, referenced from [Kirsch, Mitzenmacher 2006].
	g.keyHashes = append(g.keyHashes, bloomHash(key))
}

func (g *bloomFilterGenerator) Generate(b Buffer) {
	// bloom filter size
	nBits := uint32(max(g.n*len(g.keyHashes), 64))
	nBytes := (nBits + 7) / 8
	nBits = nBytes * 8 // round up to byte boundary

	dest := b.Allocate(int(nBytes) + 1)
	dest[nBytes] = g.k // store k in the last byte
	for _, kh := range g.keyHashes {
		delta := kh>>17 | kh<<15 // rotate right 17 bits
		for j := uint8(0); j < g.k; j++ {
			bitPos := kh % nBits
			dest[bitPos/8] |= 1 << (bitPos % 8)
			kh += delta
		}
	}
}

func NewBloomFilter(n int) FilterPolicy {
	return bloomFilter(n)
}
