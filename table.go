package granite

import (
	"bytes"
	"sort"
	"sync/atomic"

	"github.com/DecarbonizedGlucose/granite/util"
)

// tFile holds basic information about a table
type tFile struct {
	fd         util.FileDesc
	seekLeft   int32
	size       int64
	imin, imax internalKey
}

// these functions are used for user keys

// returns true if the given key is after the largest key in the table
func (t *tFile) after(c *ikComparer, ukey []byte) bool {
	return ukey != nil && c.ucmp.Compare(ukey, t.imax.ukey()) > 0
}

// returns true if the given key is before the smallest key in the table
func (t *tFile) before(c *ikComparer, ukey []byte) bool {
	return ukey != nil && c.ucmp.Compare(ukey, t.imin.ukey()) < 0
}

// returns true if given key range overlaps with the table key range
func (t *tFile) overlaps(c *ikComparer, umin, umax []byte) bool {
	return !t.after(c, umin) && !t.before(c, umax)
}

// consumes one seek and return current seeks left
func (t *tFile) consumeSeek() int32 {
	return atomic.AddInt32(&t.seekLeft, -1)
}

// create new tFile
func newTableFile(fd util.FileDesc, size int64, imin, imax internalKey) *tFile {
	// 1 seek = 16KB compactor work
	// 如果一次查询命中了L0层多个文件，或者命中了不同层的多个文件，可能会导致大量的磁盘寻道，
	// 性能急剧下降。为了避免这种情况，我们可以为每个文件设置一个seekLeft计数器，表示该文件
	// 还可以被查询多少次。保底100次，访问额度归零后应尽快做compaction。

	return &tFile{
		fd:       fd,
		size:     size,
		imin:     imin,
		imax:     imax,
		seekLeft: max(100, int32(size/16384)),
	}
}

func tableFileFromRecord(r atRecord) *tFile {
	return newTableFile(util.FileDesc{Type: util.TypeSSTable, Num: r.num}, r.size, r.imin, r.imax)
}

type tFiles []*tFile

func (ts tFiles) Len() int      { return len(ts) }
func (ts tFiles) Swap(i, j int) { ts[i], ts[j] = ts[j], ts[i] }

// Returns true if i smallest key > j.
// This used for sort by key in ascending order.
func (ts tFiles) lessByKey(c *ikComparer, i, j int) bool {
	a, b := ts[i], ts[j]
	n := c.Compare(a.imin, b.imin)
	if n == 0 {
		return a.fd.Num < b.fd.Num
	}
	return n < 0
}

// Return true if i's number > j's number.
func (ts tFiles) lessByNum(i, j int) bool {
	return ts[i].fd.Num > ts[j].fd.Num
}

// Returns sum of all file sizes in the list.
func (ts tFiles) size() (sum int64) {
	for _, t := range ts {
		sum += t.size
	}
	return sum
}

// Searches smalletst index of tables whose its
// smallest key is >= ikey.
// Returns len(ts) if there is no such table.
func (ts tFiles) searchMin(c *ikComparer, ikey internalKey) int {
	return sort.Search(len(ts), func(i int) bool {
		return c.Compare(ts[i].imin, ikey) >= 0
	})
}

// Searches smallest index of tables whose its
// largest key is >= ikey.
// Returns len(ts) if there is no such table.
func (ts tFiles) searchMax(c *ikComparer, ikey internalKey) int {
	return sort.Search(len(ts), func(i int) bool {
		return c.Compare(ts[i].imax, ikey) >= 0
	})
}

// Searches smallest index of tables whose its number is < num
// Returns len(ts) if there is no such table.
func (ts tFiles) searchNumLess(num int64) int {
	return sort.Search(len(ts), func(i int) bool {
		return ts[i].fd.Num < num
	})
}

// Searches smallest index of tables whose its
// smallest key is after the given key.
func (ts tFiles) searchMinUKey(c *ikComparer, umin []byte) int {
	return sort.Search(len(ts), func(i int) bool {
		return c.ucmp.Compare(ts[i].imin.ukey(), umin) > 0
	})
}

// Searches smallest index of tables whose its
// largest key is after the given key.
func (ts tFiles) searchMaxUkey(c *ikComparer, umax []byte) int {
	return sort.Search(len(ts), func(i int) bool {
		return c.ucmp.Compare(ts[i].imax.ukey(), umax) > 0
	})
}

func (ts tFiles) overlaps(c *ikComparer, umin, umax []byte, unsorted bool) bool {
	if unsorted {
		// check against all tables
		for _, t := range ts {
			if t.overlaps(c, umin, umax) {
				return true
			}
		}
		return false
	}

	i := 0
	if len(umin) > 0 {
		// find the earliest possible internal key for min
		i = ts.searchMax(c, createInternalKey(nil, umin, keyMaxSeq, keyTypeSeek))
	}
	if i >= len(ts) {
		return false
	}
	return !ts[i].before(c, umax)
}

func (ts tFiles) getOverlaps(dst tFiles, c *ikComparer, umin, umax []byte, overlapped bool) tFiles {
	if len(ts) == 0 {
		return nil
	}
	//
	if !overlapped {
		var begin, end int
		// determine the begin index of the overlapped file
		if umin != nil {
			index := ts.searchMinUKey(c, umin)
			if index == 0 {
				begin = 0
			} else if bytes.Compare(ts[index-1].imax.ukey(), umin) >= 0 {
				// the min ukey overlaps with the index-1 file, expand it
				begin = index - 1
			} else {
				begin = index
			}
		}
		// determine the end index of the overlapped file
		if umax != nil {
			index := ts.searchMaxUkey(c, umax)
			if index == len(ts) {
				end = len(ts)
			} else if bytes.Compare(ts[index].imin.ukey(), umax) <= 0 {
				// the max ukey overlaps with the index file, expand it
				end = index + 1
			} else {
				end = index
			}
		} else {
			end = len(ts)
		}
		// Ensure the overlapped files are valid
		if begin >= end {
			return nil
		}
		dst = make([]*tFile, end-begin)
		copy(dst, ts[begin:end])
		return dst
	}

	dst = dst[:0]
	for i := 0; i < len(ts); {
		t := ts[i]
		if t.overlaps(c, umin, umax) {
			if umin != nil && c.ucmp.Compare(t.imin.ukey(), umin) < 0 {
				umin = t.imin.ukey()
				dst = dst[:0]
				i = 0
				continue
			} else if umax != nil && c.ucmp.Compare(t.imax.ukey(), umax) > 0 {
				umax = t.imax.ukey()
				// restart search if it is overlapped
				dst = dst[:0]
				i = 0
				continue
			}

			dst = append(dst, t)
		}
		i++
	}

	return dst
}

func (ts tFiles) getRange(c *ikComparer) (imin, imax internalKey) {
	for i, t := range ts {
		if i == 0 {
			imin, imax = t.imin, t.imax
			continue
		}
		if c.Compare(t.imin, imin) < 0 {
			imin = t.imin
		}
		if c.Compare(t.imax, imax) > 0 {
			imax = t.imax
		}
	}
	return
}
