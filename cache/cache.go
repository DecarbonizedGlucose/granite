package cache

import (
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"
)

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
