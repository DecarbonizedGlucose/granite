package granite

func ensureBuffer(buf []byte, c int) []byte {
	if cap(buf) < c {
		return make([]byte, c)
	}
	return buf[:c]
}
