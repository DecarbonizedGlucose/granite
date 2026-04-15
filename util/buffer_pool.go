package util

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type BufferPool struct {
	pool     [6]sync.Pool
	baseline [5]int

	// For statistic
	cntGet     atomic.Uint32
	cntPut     atomic.Uint32
	cntLess    atomic.Uint32
	cntEqual   atomic.Uint32
	cntGreater atomic.Uint32
	cntMiss    atomic.Uint32
}

func (p *BufferPool) choosePoolIndex(size int) int {
	for i, c := range p.baseline {
		if size <= c {
			return i
		}
	}
	return len(p.baseline)
}

// Get returns buffer with length of size.
func (p *BufferPool) Get(size int) []byte {
	if p == nil {
		return make([]byte, size)
	}

	p.cntGet.Add(1)
	pIdx := p.choosePoolIndex(size)
	b := p.pool[pIdx].Get().(*[]byte)

	if cap(*b) == 0 {
		p.cntMiss.Add(1)
		if pIdx == len(p.baseline) {
			*b = make([]byte, size)
			return *b
		}
		*b = make([]byte, size, p.baseline[pIdx])
		return *b
	} else {
		if size < cap(*b) {
			p.cntLess.Add(1)
			*b = (*b)[:size]
			return *b
		} else if size == cap(*b) {
			p.cntEqual.Add(1)
			*b = (*b)[:size]
			return *b
		} else {
			// cap too little
			p.cntGreater.Add(1)
			if pIdx == len(p.baseline) {
				*b = make([]byte, size)
				return *b
			}
			*b = make([]byte, size, p.baseline[pIdx])
			return *b
		}
	}
}

// Put adds given buffer to the pool.
func (p *BufferPool) Put(b []byte) {
	if p == nil {
		return
	}

	pIdx := p.choosePoolIndex(cap(b))
	p.cntPut.Add(1)
	p.pool[pIdx].Put(&b)
}

// For log & debug
func (p *BufferPool) String() string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf(
		"BufferPool{B·%d G·%d P·%d <·%d =·%d >·%d M·%d}",
		p.baseline,
		p.cntGet.Load(),
		p.cntPut.Load(),
		p.cntLess.Load(),
		p.cntEqual.Load(),
		p.cntGreater.Load(),
		p.cntMiss.Load(),
	)
}

func NewBufferPool(baseline int) *BufferPool {
	if baseline <= 0 {
		panic("Baseline can NOT be nagetive")
	}

	f := func() any { return new([]byte) }

	bp := &BufferPool{
		baseline: [...]int{
			baseline / 4,
			baseline / 2,
			baseline,
			baseline * 2,
			baseline * 4,
		},
		pool: [6]sync.Pool{
			{New: f},
			{New: f},
			{New: f},
			{New: f},
			{New: f},
			{New: f},
		},
	}
	return bp
}
