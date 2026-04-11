package memtable

import (
	"math/rand"
	"sync"

	"github.com/DecarbonizedGlucose/granite/comparer"
)

const (
	slMaxHeight = 12
)

// used in nodeData for serching index
const (
	nKV     = iota // 0: KV offset, aka position in kvData
	nKey           // 1: Key length
	nVal           // 2: Value length
	nHeight        // 3: Height
	nNext          // 4: Start of forward pointers
)

// Memory table logic structure
type MemTable struct {
	mu     sync.RWMutex
	kvData []byte
	// Node Data:
	// [node+0]    kv offset
	// [node+1]    key length
	// [node+2]    value length
	// [node+3]    node height in skip list
	// [node+4..h] forward pointers to next nodes at each level
	//
	// This data structure writing method compresses
	// the traditional skip list (with struct Node{}) into []byte * 2
	// Sequence 0 is the sentinel node
	nodeData []int

	prevNode  [slMaxHeight]int // for remember prev node at insertion
	maxHeight int              // current skip list max height
	n         int
	kvSize    int

	rnd *rand.Rand
	cmp comparer.BasicComparer
}

// ==================== Construction & Reset ====================

func NewMemTable(cmp comparer.BasicComparer, capacity int) *MemTable {
	m := &MemTable{
		rnd:       rand.New(rand.NewSource(0xdeadbeef)),
		cmp:       cmp,
		maxHeight: 1,
		kvData:    make([]byte, 0, capacity),
		nodeData:  make([]int, 4+slMaxHeight),
	}
	m.nodeData[nHeight] = slMaxHeight
	return m
}

func (m *MemTable) Reset() {
	m.mu.Lock()
	m.rnd = rand.New(rand.NewSource(0xdeadbeef))
	m.maxHeight = 1
	m.n = 0
	m.kvSize = 0
	m.kvData = m.kvData[:0]
	m.nodeData = m.nodeData[:nNext+slMaxHeight]
	m.nodeData[nKV] = 0
	m.nodeData[nKey] = 0
	m.nodeData[nVal] = 0
	m.nodeData[nHeight] = slMaxHeight
	for n := 0; n < slMaxHeight; n++ {
		m.nodeData[nNext+n] = 0
		m.prevNode[n] = 0
	}
}

// ==================== Num funcs ====================

func (m *MemTable) Capacity() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return cap(m.kvData)
}

func (m *MemTable) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.kvSize
}

func (m *MemTable) FreeSpace() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return cap(m.kvData) - len(m.kvData)
}

func (m *MemTable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.n
}
