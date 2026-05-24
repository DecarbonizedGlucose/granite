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
