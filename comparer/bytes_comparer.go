package comparer

import "bytes"

type bytesComaprer struct{}

func (bytesComaprer) Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

func (bytesComaprer) Name() string {
	return "granite.BytesComparer"
}

func (bytesComaprer) Separator(dst, a, b []byte) []byte {
	i, n := 0, min(len(a), len(b))
	for ; i < n && a[i] == b[i]; i++ {
	}
	if i >= n {
		// one is prefix of the other, so we can't find a separator
		return nil
	} else if c := a[i]; c < 0xff && c+1 < b[i] {
		dst = append(dst, a[:i+1]...)
		dst[len(dst)-1]++
		return dst
	}
	return nil
}

func (bytesComaprer) Successor(dst, key []byte) []byte {
	for i, c := range key {
		if c < 0xff {
			dst = append(dst, key[:i+1]...)
			dst[len(dst)-1]++
			return dst
		}
	}
	return nil
}

var DefaultComparer Comparer = bytesComaprer{}
