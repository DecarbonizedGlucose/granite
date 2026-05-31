package granite

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/DecarbonizedGlucose/granite/journal"
	"github.com/DecarbonizedGlucose/granite/util"
)

// Logging

type dropper struct {
	s  *session
	fd util.FileDesc
}

func (d dropper) Drop(err error) {
	if e, ok := err.(*journal.ErrCorrupted); ok {
		d.s.logf("journal@drop %s-%d S·%s %q", d.fd.Type, d.fd.Num, shortenb(int64(e.Size)), e.Reason)
	} else {
		d.s.logf("journal@drop %s-%d %q", d.fd.Type, d.fd.Num, err)
	}
}

func (s *session) log(v ...interface{}) {
	s.stor.Log(fmt.Sprint(v...))
}

func (s *session) logf(format string, v ...interface{}) {
	s.stor.Log(fmt.Sprintf(format, v...))
}

// File utils

func (s *session) newTemp() util.FileDesc {
	num := atomic.AddInt64(&s.stTempFileNum, 1) - 1
	return util.FileDesc{Type: util.TypeTemp, Num: num}
}

// Session state

const (
	// maxCachedNumber is the maximum number of version tasks
	// that can be cached in ref loop
	maxCachedNumber = 256

	// maxCachedTime is the maximum time for ref loop tp cache
	// a version task
	maxCachedTime = 5 * time.Minute
)

// vDelta is the change information between the next
// version and the current specified version, like
// what git diff dose
type vDelta struct {
	vid     int64
	added   []int64
	deleted []int64
}

// vTask defines a version task for aither reference or release
type vTask struct {
	vid     int64
	files   []tFiles
	created time.Time
}

func (s *session) nextFileNum() int64 {
	return atomic.LoadInt64(&s.stNextFileNum)
}

func (s *session) setNextFileNum(num int64) {
	atomic.StoreInt64(&s.stNextFileNum, num)
}

func (s *session) markFileNum(num int64) {
	nextFileNum := num + 1
	for {
		old, n := atomic.LoadInt64(&s.stNextFileNum), nextFileNum
		if old >= n {
			n = old
		}
		if atomic.CompareAndSwapInt64(&s.stNextFileNum, old, n) {
			break
		}
	}
}

func (s *session) allocFileNum() int64 {
	return atomic.AddInt64(&s.stNextFileNum, 1) - 1
}

func (s *session) reuseFileNum(num int64) {
	for {
		old, n := atomic.LoadInt64(&s.stNextFileNum), num
		if old != n+1 {
			n = old
		}
		if atomic.CompareAndSwapInt64(&s.stNextFileNum, old, n) {
			break
		}
	}
}

func (s *session) setCompPtr(level int, ikey internalKey) {
	if level >= len(s.stCompPtrs) {
		newCompPtrs := make([]internalKey, level+1)
		copy(newCompPtrs, s.stCompPtrs)
		s.stCompPtrs = newCompPtrs
	}
	s.stCompPtrs[level] = append(internalKey{}, ikey...)
}

func (s *session) getCompPre(level int, ikey internalKey) internalKey {
	if level >= len(s.stCompPtrs) {
		return nil
	}
	return s.stCompPtrs[level]
}
