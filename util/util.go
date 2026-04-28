package util

func EnsureBuffer(buf []byte, c int) []byte {
	if cap(buf) < c {
		return make([]byte, c)
	}
	return buf[:c]
}
