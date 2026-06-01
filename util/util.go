package util

import (
	"errors"
)

var (
	ErrReleased    = errors.New("granite: resource already released")
	ErrHasReleaser = errors.New("granite: releaser already defined")
)

type Releaser interface {
	Release()
}

type ReleaseSetter interface {
	SetReleaser(Releaser)
}

type BasicReleaser struct {
	releaser Releaser
	released bool
}

func (r *BasicReleaser) Released() bool {
	return r.released
}

func (r *BasicReleaser) SetReleaser(releaser Releaser) {
	if r.released {
		panic(ErrReleased)
	}
	if r.releaser != nil && releaser != nil {
		panic(ErrHasReleaser)
	}
	r.releaser = releaser
}

func (r *BasicReleaser) Release() {
	if r.released {
		return
	}
	if r.releaser != nil {
		r.releaser.Release()
		r.releaser = nil
	}
	r.released = true
}

type NoopReleaser struct{}

func (NoopReleaser) Release() {}

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
