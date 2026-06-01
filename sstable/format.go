package sstable

import (
	"encoding/binary"
)

const (
	blockTrailerLen = 5
	footerLen       = 48

	// First 64-bit of sum of "https://github.com/DecarbonizedGlucose/granite/".
	magicByte = "\x2a\x4f\x21\xd8\x05\xbd\xe3\xcf"

	blockTypeNoCompression     = 0
	blockTypeSnappyCompression = 1
)

type blockPointer struct {
	offset uint64
	length uint64
}

func decodeBlockPointer(src []byte) (blockPointer, int) {
	offset, n := binary.Uvarint(src)
	length, m := binary.Uvarint(src[n:])
	if n == 0 || m == 0 {
		return blockPointer{}, 0
	}
	return blockPointer{offset, length}, n + m
}

func encodeBlockPointer(dst []byte, b blockPointer) int {
	n := binary.PutUvarint(dst, b.offset)
	m := binary.PutUvarint(dst[n:], b.length)
	return n + m
}
