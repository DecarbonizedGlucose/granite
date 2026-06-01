package granite

import (
	"bytes"
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/DecarbonizedGlucose/granite/cache"
	"github.com/DecarbonizedGlucose/granite/iterator"
	"github.com/DecarbonizedGlucose/granite/opt"
	"github.com/DecarbonizedGlucose/granite/sstable"
	"github.com/DecarbonizedGlucose/granite/storage"
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

// Helper type for sortByKey.
type tFilesSortByKey struct {
	tFiles
	ikc *ikComparer
}

func (x *tFilesSortByKey) Less(i, j int) bool {
	return x.lessByKey(x.ikc, i, j)
}

func (ts tFiles) sortByKey(c *ikComparer) {
	sort.Sort(&tFilesSortByKey{tFiles: ts, ikc: c})
}

// Helper type for sortByNum.
type tFilesSortByNum struct {
	tFiles
}

func (x *tFilesSortByNum) Less(i, j int) bool {
	return x.lessByNum(i, j)
}

func (ts tFiles) sortByNum() {
	sort.Sort(&tFilesSortByNum{tFiles: ts})
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

func (ts tFiles) newIndexIterator(tops *tOps, ikc *ikComparer, slice *util.Range, ro *opt.ReadOptions) iterator.IteratorIndexer {
	if slice != nil {
		var start, limit int
		if slice.Start != nil {
			start = ts.searchMax(ikc, internalKey(slice.Start))
		}
		if slice.Limit != nil {
			limit = ts.searchMin(ikc, internalKey(slice.Limit))
		} else {
			limit = ts.Len()
		}
		ts = ts[start:limit]
	}
	return iterator.NewArrayIndexer(&tFilesArrayIndexer{
		tFiles: ts,
		tops:   tops,
		ikc:    ikc,
		slice:  slice,
		ro:     ro,
	})
}

// Tables interator index
type tFilesArrayIndexer struct {
	tFiles
	tops  *tOps
	ikc   *ikComparer
	slice *util.Range
	ro    *opt.ReadOptions
}

func (a *tFilesArrayIndexer) Search(key []byte) int {
	return a.searchMax(a.ikc, internalKey(key))
}

func (a *tFilesArrayIndexer) Get(i int) iterator.InternalIterator {
	if i == 0 || i == a.Len()-1 {
		return a.tops.newIterator(a.tFiles[i], a.slice, a.ro)
	}
	return a.tops.newIterator(a.tFiles[i], nil, a.ro)
}

// tWriter wraps table writer, keeps track of fd and added key range
type tWriter struct {
	t *tOps

	fd util.FileDesc
	w  storage.Writer
	tw *sstable.TableWriter

	first, last []byte
}

// Append k/v pair to the table
func (w *tWriter) append(key, value []byte) error {
	if w.first == nil {
		w.first = append([]byte(nil), key...)
	}
	w.last = append(w.last[:0], key...)
	return w.tw.Append(key, value)
}

// Returns true if the table is empty.
func (w *tWriter) empty() bool {
	return w.first == nil
}

// Close the storage.Writer
func (w *tWriter) close() error {
	if w.w == nil {
		return nil
	}
	if err := w.w.Close(); err != nil {
		return err
	}
	w.w = nil
	return nil
}

// Finalize the table and returns table file
func (w *tWriter) finish() (f *tFile, err error) {
	defer func() {
		if cerr := w.close(); cerr != nil {
			if err == nil {
				err = cerr
			} else {
				err = fmt.Errorf("error opening file (%v); error unlocking file (%v)", err, cerr)
			}
		}
	}()

	err = w.tw.Close()
	if err != nil {
		return
	}
	if !w.t.noSync {
		err = w.w.Sync()
		if err != nil {
			return
		}
	}
	f = newTableFile(w.fd, int64(w.tw.BytesLen()), internalKey(w.first), internalKey(w.last))
	return
}

// Drop the table
func (w *tWriter) drop() error {
	if err := w.close(); err != nil {
		return err
	}
	w.tw = nil
	w.first = nil
	w.last = nil
	if err := w.t.s.stor.Remove(w.fd); err != nil {
		return err
	}
	w.t.s.reuseFileNum(w.fd.Num)
	return nil
}

// Table operations
type tOps struct {
	s            *session
	noSync       bool
	evictRemoved bool
	fileCache    *cache.Cache
	blockCache   *cache.Cache
	blockBuffer  *util.BufferPool
}

// creates an empty table and returns table writer
func (t *tOps) create(tSize int) (*tWriter, error) {
	fd := util.FileDesc{Type: util.TypeSSTable, Num: t.s.allocFileNum()}
	fw, err := t.s.stor.Create(fd)
	if err != nil {
		return nil, err
	}
	return &tWriter{
		t:  t,
		fd: fd,
		w:  fw,
		tw: sstable.NewTableWriter(fw, t.s.o.Options, t.blockBuffer, tSize),
	}, nil
}

// builds a table from the given iterator
func (t *tOps) createFrom(src iterator.InternalIterator) (f *tFile, n int, err error) {
	w, err := t.create(0)
	if err != nil {
		return
	}

	defer func() {
		if err != nil {
			if derr := w.drop(); derr != nil {
				err = fmt.Errorf("error createFrom (%v); error dropping (%v)", err, derr)
			}
		}
	}()

	for src.Next() {
		err = w.append(src.Key(), src.Value())
		if err != nil {
			return
		}
	}
	err = src.Error()
	if err != nil {
		return
	}

	n = w.tw.EntriesLen()
	f, err = w.finish()
	return
}

// opens a table and returns a cache handle, which
// should be released after use
func (t *tOps) open(f *tFile) (ch *cache.Handle, err error) {
	ch = t.fileCache.Get(0, uint64(f.fd.Num), func() (size int, value cache.Value) {
		var r storage.Reader
		r, err = t.s.stor.Open(f.fd)
		if err != nil {
			return 0, nil
		}

		var blockCache *cache.NamespaceGetter
		if t.blockCache != nil {
			blockCache = &cache.NamespaceGetter{Cache: t.blockCache, NS: uint64(f.fd.Num)}
		}

		var tr *sstable.TableReader
		tr, err = sstable.NewReader(r, f.size, f.fd, blockCache, t.blockBuffer, t.s.o.Options)
		if err != nil {
			_ = r.Close()
			return 0, nil
		}
		return 1, tr
	})
	if ch == nil && err == nil {
		err = ErrClosed
	}
	return
}

// finds k/v pair whose key is >= given key
func (t *tOps) find(f *tFile, key []byte, ro *opt.ReadOptions) (rk, rv []byte, err error) {
	ch, err := t.open(f)
	if err != nil {
		return nil, nil, err
	}
	defer ch.Release()
	return ch.Value().(*sstable.TableReader).Find(key, true, ro)
}

// finds key that is >= given key
func (t *tOps) findKey(f *tFile, key []byte, ro *opt.ReadOptions) (rk []byte, err error) {
	ch, err := t.open(f)
	if err != nil {
		return nil, err
	}
	defer ch.Release()
	return ch.Value().(*sstable.TableReader).FindKey(key, true, ro)
}

// returns approximate offset of given key
func (t *tOps) offsetOf(f *tFile, key []byte) (off int64, err error) {
	ch, err := t.open(f)
	if err != nil {
		return
	}
	defer ch.Release()
	return ch.Value().(*sstable.TableReader).OffsetOf(key)
}

// create an iterator for given table
func (t *tOps) newIterator(f *tFile, slice *util.Range, ro *opt.ReadOptions) iterator.InternalIterator {
	ch, err := t.open(f)
	if err != nil {
		return iterator.NewEmptyIterator(err)
	}
	iter := ch.Value().(*sstable.TableReader).NewIterator(slice, ro)
	iter.SetReleaser(ch)
	return iter
}

// Removes table from persistence storage.
// It waits until no one use the table.
func (t *tOps) remove(fd util.FileDesc) {
	t.fileCache.Delete(0, uint64(fd.Num), func() {
		if err := t.s.stor.Remove(fd); err != nil {
			t.s.logf("table@remove removing @%d %q", fd.Num, err)
		} else {
			t.s.logf("table@remove removed @%d", fd.Num)
		}
		if t.evictRemoved && t.blockCache != nil {
			t.blockCache.EvictNS(uint64(fd.Num))
		}
		// try to reuse the file num, useful for discarded transaction
		t.s.reuseFileNum(fd.Num)
	})
}

// Close the table ops instance. It will close all
// tables, regardless still used or not.
func (t *tOps) close() {
	t.fileCache.Close(true)
	if t.blockCache != nil {
		t.blockCache.Close(false)
	}
}

// Creates new initialized table ops instance.
func newTableOps(s *session) *tOps {
	var (
		fileCacher  cache.Cacher
		blockCache  *cache.Cache
		blockBuffer *util.BufferPool
	)
	if s.o.GetOpenFilesCacheCapacity() > 0 {
		fileCacher = s.o.GetOpenFilesCacher().New(s.o.GetOpenFilesCacheCapacity())
	}
	if !s.o.GetDisableBlockCache() {
		var blockCacher cache.Cacher
		if s.o.GetBlockCacheCapacity() > 0 {
			blockCacher = s.o.GetBlockCacher().New(s.o.GetBlockCacheCapacity())
		}
		blockCache = cache.NewCache(blockCacher)
	}
	if !s.o.GetDisableBufferPool() {
		blockBuffer = util.NewBufferPool(s.o.GetBlockSize() + 5)
	}
	return &tOps{
		s:            s,
		noSync:       s.o.GetNoSync(),
		evictRemoved: s.o.GetBlockCacheEvictRemoved(),
		fileCache:    cache.NewCache(fileCacher),
		blockCache:   blockCache,
		blockBuffer:  blockBuffer,
	}
}
