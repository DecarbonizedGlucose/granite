package memtable

import (
	"math/rand"
	"sync"

	"github.com/DecarbonizedGlucose/granite/comparer"
	gerrors "github.com/DecarbonizedGlucose/granite/errors"
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

// ==================== Searching and Writing ====================

// find greater or equal ...
// returns fist kv node that node.key >= key and if node.key == key
// if there's no node.key >= key, return (0, false)
// if prev == true, it needs to be called under RWLock
func (m *MemTable) findGE(key []byte, prev bool) (int, bool) {
	h := m.maxHeight - 1
	node := 0
	for {
		next := m.nodeData[node+nNext+h]
		cmpRes := 1
		if next != 0 {
			off := m.nodeData[next]
			cmpRes = m.cmp.Compare(m.kvData[off:off+m.nodeData[next+nKey]], key)
		}
		if cmpRes < 0 {
			node = next // this key is too little
		} else {
			if prev {
				m.prevNode[h] = node
			} else if cmpRes == 0 {
				return next, true
			}
			if h == 0 {
				return next, cmpRes == 0
			}
			h--
		}
	}
}

// find less than ...
// returns largest kv node that node.key < key
// if there's no node.key < key, return 0
func (m *MemTable) findLT(key []byte) int {
	h := m.maxHeight - 1
	node := 0
	for {
		next := m.nodeData[node+nNext+h]
		off := m.nodeData[next]
		if next != 0 && m.cmp.Compare(m.kvData[off:off+m.nodeData[next+nKey]], key) < 0 {
			node = next
		} else {
			if h == 0 {
				break
			}
			h--
		}
	}
	return node
}

// find the max node in the skip list
func (m *MemTable) findLast() int {
	h := m.maxHeight - 1 // start with 0
	node := 0
	for {
		next := m.nodeData[node+nNext+h]

		// next == 0 means that this node is the last one in current level
		if next == 0 {
			if h == 0 {
				break
			}
			h--
		} else {
			node = next
		}
	}
	return node
}

// Put sets value for "key". It overwrites previous values.
func (m *MemTable) Put(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if node, exact := m.findGE(key, true); exact {
		// for being called as api
		// exact will NOT be true when memtable used as db component
		kvlen := len(m.kvData)
		m.kvData = append(m.kvData, key...)
		m.kvData = append(m.kvData, value...)
		m.nodeData[node] = kvlen
		vlo := m.nodeData[node+nVal]
		m.nodeData[node+nVal] = len(value)
		m.kvSize += len(value) - vlo
		return nil
	}

	// normal put using internal key

	newh := m.genRandomHeight()
	if newh > m.maxHeight {
		for i := m.maxHeight; i < newh; i++ {
			m.prevNode[i] = 0 // initial
		}
		m.maxHeight = newh
	}

	kvlen := len(m.kvData)
	m.kvData = append(m.kvData, key...)
	m.kvData = append(m.kvData, value...)

	newNode := len(m.nodeData)
	m.nodeData = append(m.nodeData, kvlen, len(key), len(value), newh)
	for i, nodeo := range m.prevNode[:newh] {
		nexto := nodeo + nNext + i
		// set node->next at each level
		m.nodeData = append(m.nodeData, m.nodeData[nexto])
		// set node->prev->next = node
		m.nodeData[nexto] = newNode
	}

	m.kvSize += len(key) + len(value)
	m.n++

	return nil
}

// Contains returns if "key" is in the memtable.
func (m *MemTable) Contains(key []byte) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, exact := m.findGE(key, false)
	return exact
}

// Find finds {k, v} >= "key".
// If wanted {k, v} not found, it returns with ErrNotFound.
func (m *MemTable) Find(key []byte) (rkey, value []byte, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	node, _ := m.findGE(key, false)
	if node != 0 {
		ko := m.nodeData[node]
		vo := ko + m.nodeData[node+nKey]
		rkey = m.kvData[ko:vo]
		value = m.kvData[vo : vo+m.nodeData[node+nVal]]
	} else {
		err = gerrors.ErrNotFound
	}
	return
}

// Get gets the value for "key".
// Return with ErrNotFound if "key" does not exist.
func (m *MemTable) Get(key []byte) (value []byte, err error) {
	m.mu.Lock()
	defer m.mu.RUnlock()

	node, exact := m.findGE(key, false)
	if exact {
		vo := m.nodeData[node] + m.nodeData[node+nKey] // value offset
		value = m.kvData[vo : vo+m.nodeData[node+nVal]]
	} else {
		err = gerrors.ErrNotFound
	}
	return
}

// Delete deletes "key" and its value.
// Return with ErrNotFound if "key" does not exist.
func (m *MemTable) Delete(key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	toDelNode, exact := m.findGE(key, true)
	if exact == false {
		return gerrors.ErrNotFound
	}

	nh := m.nodeData[toDelNode+nHeight]
	for i, nodeo := range m.prevNode[:nh] {
		nexto := nodeo + nNext + i
		m.nodeData[nexto] = m.nodeData[m.nodeData[nexto]+nNext+i] // node->next = node->next->next
	}

	m.kvSize -= m.nodeData[toDelNode+nKey] + m.nodeData[toDelNode+nVal]
	m.n--

	return nil
}

// ==================== Utils ====================

func (m *MemTable) genRandomHeight() int {
	const branching = 4
	h := 1
	for h < slMaxHeight && m.rnd.Int()%branching == 0 {
		h++
	}
	return h
}
