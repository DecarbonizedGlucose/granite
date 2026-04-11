package sstable

import (
	"encoding/binary"
)

const (
	footerLen = 48
	magicByte = "\x11\x45\x14\x19" // temp decision
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
