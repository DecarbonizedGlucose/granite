package cache

import (
	"sync"
	"unsafe"
)

type lruNode struct {
	n          *Node
	prev, next *lruNode
	h          *Handle
	ban        bool
}

func (n *lruNode) insert(at *lruNode) {
	x := at.next
	at.next = n
	n.prev = at
	n.next = x
	x.prev = n
}

func (n *lruNode) remove() {
	if n.prev == nil {
		panic("BUG: removing removed node")
	}
	n.prev.next = n.next
	n.next.prev = n.prev
	n.prev = nil
	n.next = nil
}

type lru struct {
	mu     sync.Mutex
	cap    int
	used   int
	recent lruNode
}

func (r *lru) reset() {
	r.recent.prev = &r.recent
	r.recent.next = &r.recent
	r.used = 0
}

func (r *lru) Capacity() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cap
}

func (r *lru) SetCap(cap int) {
	var evicted []*lruNode

	r.mu.Lock()
	r.cap = cap
	for r.used > r.cap {
		rn := r.recent.prev
		if rn == nil {
			panic("BUG: invalid LRU used for capacity counter")
		}
		rn.remove()
		rn.n.CacheData = nil
		r.used -= rn.n.Size()
		evicted = append(evicted, rn)
	}
	r.mu.Unlock()

	for _, rn := range evicted {
		rn.h.Release()
	}
}

func (r *lru) Promote(n *Node) {
	var evicted []*lruNode

	r.mu.Lock()
	if n.CacheData == nil {
		if n.Size() <= r.cap {
			rn := &lruNode{n: n, h: n.GetHandle()}
			rn.insert(&r.recent)
			n.CacheData = unsafe.Pointer(rn)
			r.used += n.Size()

			for r.used > r.cap {
				rn := r.recent.prev
				if rn == nil {
					panic("BUG: invalid LRU used for capacity counter")
				}
				rn.remove()
				rn.n.CacheData = nil
				r.used -= rn.n.Size()
				evicted = append(evicted, rn)
			}
		}
	} else {
		rn := (*lruNode)(n.CacheData)
		if !rn.ban {
			rn.remove()
			rn.insert(&r.recent)
		}
	}
	r.mu.Unlock()

	for _, rn := range evicted {
		rn.h.Release()
	}
}

func (r *lru) Ban(n *Node) {
	r.mu.Lock()
	if n.CacheData == nil {
		n.CacheData = unsafe.Pointer(&lruNode{n: n, ban: true})
	} else {
		rn := (*lruNode)(n.CacheData)
		if !rn.ban {
			rn.remove()
			rn.ban = true
			r.used -= rn.n.Size()
			r.mu.Unlock()

			rn.h.Release()
			rn.h = nil
			return
		}
	}
	r.mu.Unlock()
}

func (r *lru) Evict(n *Node) {
	r.mu.Lock()
	rn := (*lruNode)(n.CacheData)
	if rn == nil || rn.ban {
		r.mu.Unlock()
		return
	}
	rn.remove()
	r.used -= rn.n.Size()
	n.CacheData = nil
	r.mu.Unlock()

	rn.h.Release()
}

func NewLRU(capacity int) Cacher {
	r := &lru{cap: capacity}
	r.reset()
	return r
}
