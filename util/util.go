package util

type Releaser interface {
	Close()
}

type NoopReleaser struct{}

func (NoopReleaser) Close() {}

func EnsureBuffer(buf []byte, c int) []byte {
	if cap(buf) < c {
		return make([]byte, c)
	}
	return buf[:c]
}

func Itoa(buf []byte, i int, wid int) []byte {
	u := uint(i)
	if u == 0 && wid <= 1 {
		return append(buf, '0')
	}

	var b [32]byte
	bp := len(b)
	for ; u > 0 || wid > 0; u /= 10 {
		bp--
		wid--
		b[bp] = byte(u%10) + '0'
	}
	return append(buf, b[bp:]...)
}
