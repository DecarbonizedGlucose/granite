package granite

import (
	"sync/atomic"
	"unsafe"
)

type tSet struct {
	level int
	table *tFile
}

type version struct {
	id int64 // unique monotonous increasing version id
	s  *session

	levels []tFiles

	cLevel int
	cScore float64

	cSeek unsafe.Pointer

	closing  bool
	ref      int
	released bool
}

func newVersion(s *session) *version {
	id := atomic.AddInt64(&s.ntVersionID, 1)
	nv := &version{s: s, id: id}
	return nv
}
