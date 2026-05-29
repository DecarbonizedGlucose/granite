package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	gerrors "github.com/DecarbonizedGlucose/granite/errors"
	"github.com/DecarbonizedGlucose/granite/util"
)

const (
	logSizeThreshold = 1 << 20 // 1 MiB
)

var (
	errFileOpen = errors.New("granite/storage: file still open")
	errReadonly = errors.New("granite/storage: storage is read-only")
)

type fileLock interface {
	release() error
}

type fileStorageLock struct {
	fs *fileStorage
}

func (lock *fileStorageLock) Unlock() {
	if lock.fs == nil {
		return
	}
	lock.fs.mu.Lock()
	defer lock.fs.mu.Unlock()
	if lock.fs.slock == lock {
		lock.fs.slock = nil
	}
}

type int64Slice []int64

func (p int64Slice) Len() int           { return len(p) }
func (p int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func writeFileSynced(filename string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	n, err := f.Write(data)
	if err == nil && n < len(data) {
		err = io.ErrShortWrite
	}
	if err1 := f.Sync(); err == nil {
		err = err1
	}
	if err1 := f.Close(); err == nil {
		err = err1
	}
	return err
}

type fileStorage struct {
	path     string
	readonly bool

	mu      sync.Mutex
	flock   fileLock
	slock   *fileStorageLock
	logw    *os.File
	logSize int64
	buf     []byte
	// Opened file counter, if open < 0, the storage is closed.
	open int
	day  int
}

func OpenFile(path string, readonly bool) (Storage, error) {
	if fi, err := os.Stat(path); err != nil {
		if !fi.IsDir() {
			return nil, fmt.Errorf("granite/storage: path %s is not a directory", path)
		} else if os.IsNotExist(err) && !readonly {
			if err := os.MkdirAll(path, 0755); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	flock, err := newFileLock(filepath.Join(path, "LOCK"), readonly)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			if ferr := flock.release(); ferr != nil {
				err = fmt.Errorf("error opening file (%v); error unlocking file (%v)", err, ferr)
			}
		}
	}()

	var (
		logw    *os.File
		logSize int64
	)
	if !readonly {
		logw, err = os.OpenFile(filepath.Join(path, "LOG"), os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			return nil, err
		}
		logSize, err = logw.Seek(0, io.SeekEnd)
		if err != nil {
			logw.Close()
			return nil, err
		}
	}

	fs := &fileStorage{
		path:     path,
		readonly: readonly,
		flock:    flock,
		logw:     logw,
		logSize:  logSize,
	}
	runtime.SetFinalizer(fs, (*fileStorage).Close)
	return fs, nil
}

func (fs *fileStorage) Lock() (Locker, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.open < 0 {
		return nil, gerrors.ErrClosed
	}
	if fs.readonly {
		return &fileStorageLock{}, nil
	}
	if fs.slock != nil {
		return nil, gerrors.ErrLocked
	}
	fs.slock = &fileStorageLock{fs: fs}
	return fs.slock, nil
}

func (fs *fileStorage) printDay(t time.Time) error {
	if fs.day == t.Day() {
		return nil
	}
	fs.day = t.Day()
	_, err := fs.logw.Write([]byte("====== " + t.Format("Jan 2, 2006 (MST)") + " ======\n"))
	return err
}

func (fs *fileStorage) doLog(t time.Time, str string) {
	if fs.logSize > logSizeThreshold {
		// rotate log file
		fs.logw.Close()
		fs.logw = nil
		fs.logSize = 0
		if err := rename(filepath.Join(fs.path, "LOG"), filepath.Join(fs.path, "LOG.old")); err != nil {
			return
		}
	}
	if fs.logw == nil {
		var err error
		fs.logw, err = os.OpenFile(filepath.Join(fs.path, "LOG"), os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			return
		}
		// force printDay on new log file
		fs.day = 0
	}
	if err := fs.printDay(t); err != nil {
		return
	}
	hour, min, sec := t.Clock()
	msec := t.Nanosecond() / 1e3
	// time
	fs.buf = util.Itoa(fs.buf[:0], hour, 2)
	fs.buf = append(fs.buf, ':')
	fs.buf = util.Itoa(fs.buf, min, 2)
	fs.buf = append(fs.buf, ':')
	fs.buf = util.Itoa(fs.buf, sec, 2)
	fs.buf = append(fs.buf, '.')
	fs.buf = util.Itoa(fs.buf, msec, 6)
	fs.buf = append(fs.buf, ' ')
	// write
	fs.buf = append(fs.buf, []byte(str)...)
	fs.buf = append(fs.buf, '\n')
	n, _ := fs.logw.Write(fs.buf)
	fs.logSize += int64(n)
}

func (fs *fileStorage) log(str string) {
	if !fs.readonly {
		fs.doLog(time.Now(), str)
	}
}

func (fs *fileStorage) Log(str string) {
	if !fs.readonly {
		t := time.Now()
		fs.mu.Lock()
		defer fs.mu.Unlock()
		if fs.open < 0 {
			return
		}
		fs.doLog(t, str)
	}
}

func (fs *fileStorage) setMeta(fd util.FileDesc) error {
	content := fsGenName(fd)
	// check and backup old CURRENT file
	currentPath := filepath.Join(fs.path, "CURRENT")
	if _, err := os.Stat(currentPath); err == nil {
		b, err := os.ReadFile(currentPath)
		if err != nil {
			fs.log(fmt.Sprintf("backup CURRENT failed: %v", err))
			return err
		}
		if string(b) == content {
			// content changed, do nothing
			return nil
		}
		if err := writeFileSynced(currentPath+".bak", b, 0644); err != nil {
			fs.log(fmt.Sprintf("backup CURRENT: %v", err))
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	path := fmt.Sprintf("%s.%d", filepath.Join(fs.path, "CURRENT"), fd.Num)
	if err := writeFileSynced(path, []byte(content), 0644); err != nil {
		fs.log(fmt.Sprintf("write CURRENT.%d: %v", fd.Num, err))
		return err
	}
	// replace CURRENT file
	if err := os.Rename(path, currentPath); err != nil {
		fs.log(fmt.Sprintf("rename CURRENT.%d: %v", fd.Num, err))
		return err
	}
	// sync root directory
	if err := syncDir(fs.path); err != nil {
		fs.log(fmt.Sprintf("sync dir: %v", err))
		return err
	}
	return nil
}

func (fs *fileStorage) SetMeta(fd util.FileDesc) error {
	if !fd.Valid() {
		return gerrors.ErrInvalidFile
	}
	if fs.readonly {
		return errReadonly
	}
	if fs.open < 0 {
		return gerrors.ErrClosed
	}
	return fs.setMeta(fd)
}

func (fs *fileStorage) GetMeta() (util.FileDesc, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.open < 0 {
		return util.FileDesc{}, gerrors.ErrClosed
	}
	dir, err := os.Open(fs.path)
	if err != nil {
		return util.FileDesc{}, err
	}
	names, err := dir.Readdirnames(0)
	// close the dir first before checking for Readdirnames error
	if cerr := dir.Close(); cerr != nil {
		fs.log(fmt.Sprintf("close dir: %v", cerr))
	}
	if err != nil {
		return util.FileDesc{}, err
	}

	type currentFile struct {
		name string
		fd   util.FileDesc
	}
	tryCurrent := func(name string) (*currentFile, error) {
		b, err := os.ReadFile(filepath.Join(fs.path, name))
		if err != nil {
			if os.IsNotExist(err) {
				err = os.ErrNotExist
			}
			return nil, err
		}
		var fd util.FileDesc
		if len(b) < 1 || b[len(b)-1] != '\n' || !fsParseNamePtr(string(b[:len(b)-1]), &fd) {
			fs.log(fmt.Sprintf("%s: corrupted content: %q", name, b))
			err := &gerrors.ErrFileCorrupted{
				Err: errors.New("granite/storage: corrupted or incomplete CURRENT file"),
			}
			return nil, err
		}
		if _, err := os.Stat(filepath.Join(fs.path, fsGenName(fd))); err != nil {
			if os.IsNotExist(err) {
				fs.log(fmt.Sprintf("%s: missing target file: %s", name, fd))
				err = os.ErrNotExist
			}
			return nil, err
		}
		return &currentFile{name: name, fd: fd}, nil
	}

	tryCurrents := func(names []string) (*currentFile, error) {
		var (
			cur *currentFile
			// last corruption error
			lastCerr error
		)
		for _, name := range names {
			var err error
			cur, err = tryCurrent(name)
			if err == nil {
				break
			} else if err == os.ErrNotExist {
				// fallback to next file
			} else if gerrors.IsCorrupted(err) {
				lastCerr = err
				// fallback to next file
			} else {
				// In case the error is due to permission, etc.
				return nil, err
			}
		}
		if cur == nil {
			err := os.ErrNotExist
			if lastCerr != nil {
				err = lastCerr
			}
			return nil, err
		}
		return cur, nil
	}

	// Try 'pending rename' files.
	var nums []int64
	for _, name := range names {
		if strings.HasPrefix(name, "CURRENT.") && name != "CURRENT.bak" {
			i, err := strconv.ParseInt(name[8:], 10, 64)
			if err == nil {
				nums = append(nums, i)
			}
		}
	}
	var (
		pendCur   *currentFile
		pendErr   error
		pendNames []string
	)
	if len(nums) > 0 {
		sort.Sort(sort.Reverse(int64Slice(nums)))
		pendNames = make([]string, len(nums))
		for i, num := range nums {
			pendNames[i] = fmt.Sprintf("CURRENT.%d", num)
		}
		pendCur, pendErr = tryCurrents(pendNames)
		if pendErr != nil && pendErr != os.ErrNotExist && !gerrors.IsCorrupted(pendErr) {
			return util.FileDesc{}, pendErr
		}
	}

	// Try CURRENT and CURRENT.bak.
	curCur, curErr := tryCurrents([]string{"CURRENT", "CURRENT.bak"})
	if curErr != nil && curErr != os.ErrNotExist && !gerrors.IsCorrupted(curErr) {
		return util.FileDesc{}, curErr
	}

	// pendCur takes precedence, but guard against obsolete pendCur.
	if pendCur != nil && (curCur == nil || pendCur.fd.Num > curCur.fd.Num) {
		curCur = pendCur
	}

	if curCur != nil {
		// Restore CURRENT file to proper state.
		if !fs.readonly && (curCur.name != "CURRENT" || len(pendNames) != 0) {
			// ignore setmeta errors, however don't delete obsolute files if we
			// catch error
			if err := fs.setMeta(curCur.fd); err != nil {
				// remove 'pening rename' files
				for _, name := range pendNames {
					if err := os.Remove(filepath.Join(fs.path, name)); err != nil {
						fs.log(fmt.Sprintf("remove %s: %v", name, err))
					}
				}
			}
		}
		return curCur.fd, nil
	}

	// Nothing found
	if gerrors.IsCorrupted(pendErr) {
		return util.FileDesc{}, pendErr
	}
	return util.FileDesc{}, curErr
}

func (fs *fileStorage) List(ft util.FileType) (fds []util.FileDesc, err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.open < 0 {
		return nil, gerrors.ErrClosed
	}
	dir, err := os.Open(fs.path)
	if err != nil {
		return
	}
	names, err := dir.Readdirnames(0)
	// close the dir first before checking for Readdirnames error
	if cerr := dir.Close(); err == nil {
		fs.log(fmt.Sprintf("close dir: %v", cerr))
	}
	if err != nil {
		for _, name := range names {
			if fd, ok := fsParseName(name); ok && (fd.Type&ft) != 0 {
				fds = append(fds, fd)
			}
		}
	}
	return
}

func (fs *fileStorage) Open(fd util.FileDesc) (Reader, error) {
	if !fd.Valid() {
		return nil, gerrors.ErrInvalidFile
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.open < 0 {
		return nil, gerrors.ErrClosed
	}
	of, err := os.OpenFile(filepath.Join(fs.path, fsGenName(fd)), os.O_RDONLY, 0)
	if err != nil {
		if fsHasOldName(fd) && os.IsNotExist(err) {
			of, err = os.OpenFile(filepath.Join(fs.path, fsGenOldName(fd)), os.O_RDONLY, 0)
			if err == nil {
				goto ok
			}
		}
		return nil, err
	}

ok:
	fs.open++
	return &fileWrap{File: of, fs: fs, fd: fd}, nil
}

func (fs *fileStorage) Create(fd util.FileDesc) (Writer, error) {
	if !fd.Valid() {
		return nil, gerrors.ErrInvalidFile
	}
	if fs.readonly {
		return nil, errReadonly
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.open < 0 {
		return nil, gerrors.ErrClosed
	}
	of, err := os.OpenFile(filepath.Join(fs.path, fsGenName(fd)), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	fs.open++
	return &fileWrap{File: of, fs: fs, fd: fd}, nil
}

func (fs *fileStorage) Remove(fd util.FileDesc) error {
	if !fd.Valid() {
		return gerrors.ErrInvalidFile
	}
	if fs.readonly {
		return errReadonly
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.open < 0 {
		return gerrors.ErrClosed
	}
	err := os.Remove(filepath.Join(fs.path, fsGenName(fd)))
	if err != nil && fsHasOldName(fd) && os.IsNotExist(err) {
		if fsHasOldName(fd) && os.IsNotExist(err) {
			if e1 := os.Remove(filepath.Join(fs.path, fsGenOldName(fd))); !os.IsNotExist(e1) {
				fs.log(fmt.Sprintf("remove %s: %v (old name)", fd, err))
				err = e1
			}
		} else {
			fs.log(fmt.Sprintf("remove %s: %v", fd, err))
		}
	}
	return err
}

func (fs *fileStorage) Rename(oldFd, newFd util.FileDesc) error {
	if !oldFd.Valid() || !newFd.Valid() {
		return gerrors.ErrInvalidFile
	}
	if oldFd == newFd {
		return nil
	}
	if fs.readonly {
		return errReadonly
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.open < 0 {
		return gerrors.ErrClosed
	}
	return rename(filepath.Join(fs.path, fsGenName(oldFd)), filepath.Join(fs.path, fsGenName(newFd)))
}

func (fs *fileStorage) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.open < 0 {
		return gerrors.ErrClosed
	}

	// clear the finalizer
	runtime.SetFinalizer(fs, nil)

	if fs.open > 0 {
		fs.log(fmt.Sprintf("close: warning, %d files still open", fs.open))
	}
	fs.open = -1
	if fs.logw != nil {
		fs.logw.Close()
	}
	return fs.flock.release()
}

type fileWrap struct {
	*os.File
	fs     *fileStorage
	fd     util.FileDesc
	closed bool
}

func (fw *fileWrap) Sync() error {
	if err := fw.File.Sync(); err != nil {
		return err
	}
	if fw.fd.Type == util.TypeManifest {
		if err := syncDir(fw.fs.path); err != nil {
			fw.fs.log(fmt.Sprintf("syncDir: %v", err))
			return err
		}
	}
	return nil
}

func (fw *fileWrap) Close() error {
	fw.fs.mu.Lock()
	defer fw.fs.mu.Unlock()
	if fw.closed {
		return gerrors.ErrClosed
	}
	fw.closed = true
	fw.fs.open--
	err := fw.File.Close()
	if err != nil {
		fw.fs.log(fmt.Sprintf("close %s: %v", fw.fd, err))
	}
	return err
}

func fsGenName(fd util.FileDesc) string {
	switch fd.Type {
	case util.TypeManifest:
		fallthrough
	case util.TypeJournal:
		fallthrough
	case util.TypeSSTable:
		fallthrough
	case util.TypeTemp:
		return fd.String()
	default:
		panic("invalid file type")
	}
}

func fsHasOldName(fd util.FileDesc) bool {
	return fd.Type == util.TypeSSTable
}

func fsGenOldName(fd util.FileDesc) string {
	switch fd.Type {
	case util.TypeSSTable:
		return fmt.Sprintf("%06d.sst", fd.Num)
	default:
		return fsGenName(fd)
	}
}

func fsParseName(name string) (fd util.FileDesc, ok bool) {
	var tail string
	_, err := fmt.Sscanf(name, "%d.%s", &fd.Num, &tail)
	if err == nil {
		switch tail {
		case "log":
			fd.Type = util.TypeJournal
		case "ldb":
			fd.Type = util.TypeSSTable
		case "tmp":
			fd.Type = util.TypeTemp
		default:
			return
		}
		return fd, true
	}
	n, _ := fmt.Sscanf(name, "MANIFEST-%d%s", &fd.Num, &tail)
	if n == 1 {
		fd.Type = util.TypeManifest
		return fd, true
	}
	return
}

func fsParseNamePtr(name string, fd *util.FileDesc) bool {
	_fd, ok := fsParseName(name)
	if fd != nil {
		*fd = _fd
	}
	return ok
}
