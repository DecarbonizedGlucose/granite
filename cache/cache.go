package cache

import (
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/DecarbonizedGlucose/granite/util"
)

const (
	mInitialSize           = 1 << 4
	mOverflowThreshold     = 1 << 5
	mOverflowGrowThreshold = 1 << 7
)

const (
	bucketUninitialized = iota
	bucketInitialized
	bucketFrozen
)

type Cacher interface {
	// Capacity returns the capacity of the cache.
	Capacity() int

	// SetCap sets the capacity of the cache.
	SetCap(cap int)

	// Promote promotes the 'cache node'.
	Promote(n *Node)

	// Ban evicts the 'cache node' and prevent subsequent 'promote'.
	Ban(n *Node)

	// Evict evicts the 'cache node'.
	Evict(n *Node)
}


// Cache is a 'cache map', which is a concurrent hash
// table for mapping (ns, key) to value(cache node).
type Cache struct {
	mu     sync.RWMutex
	mHead  unsafe.Pointer // *mHead, concurrent hash tables
	cacher Cacher         // cache replacement policy
	closed bool

	statNodes  int64 // number of nodes in the cache
	statSize   int64 // total size of the values in the cache
	statGrow   int32 // number of times the cache has grown
	statShrink int32 // number of times the cache has shrunk
	statHit    int64 // number of cache hits
	statMiss   int64 // number of cache misses
	statSet    int64 // number of times a value is set in the cache
	statDel    int64 // number of times a value is deleted from the cache
}

// NewCache creates a new `Cache`. The `cacher` can be nil.
func NewCache(cacher Cacher) *Cache {
	h := &mHead{
		buckets:         make([]mBucket, mInitialSize),
		mask:            mInitialSize - 1,
		growThreshold:   int64(mInitialSize * mOverflowThreshold),
		shrinkThreshold: 0,
	}
	for i := range h.buckets {
		h.buckets[i].state = bucketUninitialized
	}
	r := &Cache{
		mHead:  unsafe.Pointer(h),
		cacher: cacher,
	}
	return r
}

func (r *Cache) getBucket(hash uint32) (*mHead, *mBucket) {
	h := (*mHead)(atomic.LoadPointer(&r.mHead))
	i := hash & h.mask
	return h, h.initBucket(i)
}


type Value any

type Node struct {
	hash      uint32 // hash of the key
	ns, key   uint64 // to label the node
	mu        sync.Mutex
	size      int            // size of the value
	value     Value          // real data what we want to cache
	ref       int32          // reference count
	delFuncs  []func()       // delete callback functions
	CacheData unsafe.Pointer // pointer to lruNode
}

type Handle struct {
	n unsafe.Pointer // *Node
}

func (h *Handle) Value() Value {
	n := (*Node)(atomic.LoadPointer(&h.n))
	if n != nil {
		return n.value
	}
	return nil
}

type mNodes []*Node

func (x mNodes) Len() int { return len(x) }
func (x mNodes) Less(i, j int) bool {
	a, b := x[i].ns, x[j].ns
	if a == b {
		return x[i].key < x[j].key
	}
	return a < b
}
func (x mNodes) Swap(i, j int) { x[i], x[j] = x[j], x[i] }
func (x mNodes) sort()         { sort.Sort(x) }
func (x mNodes) search(ns, key uint64) int {
	return sort.Search(len(x), func(i int) bool {
		a := x[i].ns
		if a == ns {
			return x[i].key >= key
		}
		return a > ns
	})
}

type mBucket struct {
	mu    sync.Mutex
	nodes mNodes
	state int8
}

func (b *mBucket) freeze() mNodes {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == bucketInitialized {
		b.state = bucketFrozen
	} else if b.state == bucketUninitialized {
		panic("BUG: freezing uninitialized bucket")
	}
	return b.nodes
}

func (b *mBucket) frozen() bool {
	if b.state == bucketFrozen {
		return true
	}
	if b.state == bucketUninitialized {
		panic("BUG: checking frozen state of uninitialized bucket")
	}
	return false
}

func (b *mBucket) get(r *Cache, h *mHead, hash uint32, ns, key uint64, getOnly bool) (done, created bool, n *Node) {
	b.mu.Lock()

	if b.frozen() {
		return
	}

	// Search for the node.
	i := b.nodes.search(ns, key)
	if i < len(b.nodes) {
		n = b.nodes[i]
		if n.ns == ns && n.key == key {
			atomic.AddInt32(&n.ref, 1)
			b.mu.Unlock()
			return true, false, n
		}
	}

	// Get only
	if getOnly {
		b.mu.Unlock()
		return false, false, nil
	}

	// create a new node
	n = &Node{
		r:    r,
		hash: hash,
		ns:   ns,
		key:  key,
		ref:  1,
	}
	// add node to bucket
	if i == len(b.nodes) {
		b.nodes = append(b.nodes, n)
	} else {
		b.nodes = append(b.nodes[:i+1], b.nodes[i:]...) // delete nodes[i]
		b.nodes[i] = n
	}
	bLen := len(b.nodes)
	b.mu.Unlock()

	// update counter
	grow := atomic.AddInt64(&r.statNodes, 1) >= h.growThreshold
	if bLen > mOverflowGrowThreshold {
		grow = grow || atomic.AddInt32(&h.overflow, 1) >= mOverflowThreshold
	}

	// grow
	if grow && atomic.CompareAndSwapInt32(&h.resizeInProgress, 0, 1) {
		nhLen := len(h.buckets) << 1
		nh := &mHead{
			buckets:         make([]mBucket, nhLen),
			mask:            uint32(nhLen - 1),
			predecessor:     unsafe.Pointer(h),
			growThreshold:   int64(nhLen * mOverflowThreshold / 100),
			shrinkThreshold: int64(bLen * mOverflowThreshold / 100),
		}
		ok := atomic.CompareAndSwapPointer(&r.mHead, unsafe.Pointer(h), unsafe.Pointer(nh))
		if !ok {
			panic("BUG: failed swapping head during grow")
		}
		atomic.AddInt32(&r.statGrow, 1)
		go nh.initBuckets()
	}

	return true, true, n
}

func (b *mBucket) delete(r *Cache, h *mHead, ns, key uint64) (done bool, deleted bool) {
	b.mu.Lock()

	if b.frozen() {
		return
	}

	// Search for the node.
	i := b.nodes.search(ns, key)
	if i == len(b.nodes) {
		b.mu.Unlock()
		return false, false
	}
	n := b.nodes[i]
	var bLen int
	if n.ns == ns && n.key == key {
		if atomic.LoadInt32(&n.ref) == 0 {
			deleted = true

			// save and clear value
			if n.value != nil {
				// call releaser
				if r, ok := n.value.(util.Releaser); ok {
					r.Close()
				}
				n.value = nil
			}

			// remove node from bucket
			b.nodes = append(b.nodes[:i], b.nodes[i+1:]...)
			bLen = len(b.nodes)
		}
	}

	if deleted {
		// call delete funcs
		for _, f := range n.delFuncs {
			f()
		}

		atomic.AddInt64(&r.statSize, int64(n.size)*-1)
		shrink := atomic.AddInt64(&r.statNodes, -1) < h.shrinkThreshold
		if bLen < mOverflowThreshold {
			atomic.AddInt32(&h.overflow, -1)
		}

		// shrink
		if shrink && len(h.buckets) > mInitialSize && atomic.CompareAndSwapInt32(&h.resizeInProgress, 0, 1) {
			nhLen := len(h.buckets) >> 1
			nh := &mHead{
				buckets:         make([]mBucket, nhLen),
				mask:            uint32(nhLen - 1),
				predecessor:     unsafe.Pointer(h),
				growThreshold:   int64(nhLen * mOverflowThreshold),
				shrinkThreshold: int64(nhLen*mOverflowThreshold) >> 1,
			}
			ok := atomic.CompareAndSwapPointer(&r.mHead, unsafe.Pointer(h), unsafe.Pointer(nh))
			if !ok {
				panic("BUG: failed swapping head during shrink")
			}
			atomic.AddInt32(&r.statShrink, 1)
			go nh.initBuckets()
		}
	}

	return true, deleted
}

type mHead struct {
	buckets          []mBucket
	mask             uint32
	predecessor      unsafe.Pointer // *mHead
	resizeInProgress int32

	overflow        int32
	growThreshold   int64
	shrinkThreshold int64
}

func (h *mHead) initBucket(i uint32) *mBucket {
	b := &h.buckets[i]
	b.mu.Lock()
	if b.state >= bucketInitialized {
		b.mu.Unlock()
		return b
	}

	p := (*mHead)(atomic.LoadPointer(&h.predecessor))
	if p == nil {
		panic("BUG: uninitialized bucket dose not have predecessor")
	}

	var nodes mNodes
	if h.mask > p.mask {
		// grow
		m := p.initBucket(i & p.mask).freeze()
		// split nodes
		for _, x := range m {
			if x.hash&h.mask == i {
				nodes = append(nodes, x)
			}
		}
	} else {
		// shrink
		m0 := p.initBucket(i).freeze()
		m1 := p.initBucket(i + uint32(len(h.buckets))).freeze()
		// merge nodes
		nodes = make(mNodes, 0, len(m0)+len(m1))
		nodes = append(nodes, m0...)
		nodes = append(nodes, m1...)
		nodes.sort()
	}
	b.nodes = nodes
	b.state = bucketInitialized
	b.mu.Unlock()
	return b
}

func (h *mHead) initBuckets() {
	for i := range h.buckets {
		h.initBucket(uint32(i))
	}
	atomic.StorePointer(&h.predecessor, nil)
}

