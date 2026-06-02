package granite

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	gerrors "github.com/DecarbonizedGlucose/granite/errors"
	"github.com/DecarbonizedGlucose/granite/journal"
	"github.com/DecarbonizedGlucose/granite/opt"
	"github.com/DecarbonizedGlucose/granite/storage"
	"github.com/DecarbonizedGlucose/granite/util"
)

type ErrManifestCorrupted struct {
	Field  string
	Reason string
}

func (e *ErrManifestCorrupted) Error() string {
	return fmt.Sprintf("leveldb: manifest corrupted (field '%s'): %s", e.Field, e.Reason)
}

func newErrManifestCorrupted(fd util.FileDesc, field, reason string) error {
	return gerrors.NewErrFileCorrupted(fd, &ErrManifestCorrupted{field, reason})
}

type session struct {
	stNextFileNum    int64
	stJournalNum     int64
	stPrevJournalNum int64
	stTempFileNum    int64
	stSeqNum         uint64

	stor     *iStorage
	storLock storage.Locker
	o        *cachedOptions
	ikc      *ikComparer
	tops     *tOps

	manifest       *journal.JournalWriter
	manifestWriter storage.Writer
	manifestFd     util.FileDesc

	stCompPtrs  []internalKey // compaction pointers
	stVersion   *version      // current version
	ntVersionID int64         // next version id
	refCh       chan *vTask
	relCh       chan *vTask
	deltaCh     chan *vDelta
	abandon     chan int64
	closeC      chan struct{}
	closeW      sync.WaitGroup
	vmu         sync.Mutex

	fileRefCh chan chan map[int64]int // channal used to pass current reference stat
}

// Creates new initialized session instance.
func newSession(stor storage.Storage, o *opt.Options) (s *session, err error) {
	if stor == nil {
		return nil, os.ErrInvalid
	}
	storLock, err := stor.Lock()
	if err != nil {
		return
	}
	s = &session{
		stor:      newIStorage(stor),
		storLock:  storLock,
		refCh:     make(chan *vTask),
		relCh:     make(chan *vTask),
		deltaCh:   make(chan *vDelta),
		abandon:   make(chan int64),
		fileRefCh: make(chan chan map[int64]int),
		closeC:    make(chan struct{}),
	}
	s.setOptions(o)
	s.tops = newTableOps(s)

	s.closeW.Add(1)
	go s.refLoop()
	s.setVersion(nil, newVersion(s))
	s.log("log@legend F·NumFile S·FileSize N·Entry C·BadEntry B·BadBlock Ke·KeyError D·DroppedEntry L·Level Q·SeqNum T·TimeElapsed")
	return
}

// Close session
func (s *session) close() {
	s.tops.close()
	if s.manifest != nil {
		s.manifest.Close()
	}
	if s.manifestWriter != nil {
		s.manifestWriter.Close()
	}
	s.manifest = nil
	s.manifestWriter = nil
	s.setVersion(nil, &version{s: s, closing: true, id: s.ntVersionID})

	// close all background goroutines
	close(s.closeC)
	s.closeW.Wait()
}

// Release session lock
func (s *session) release() {
	s.storLock.Unlock()
}

// Recover a database session(manifest).
// It needs external sync.
func (s *session) create() error {
	return s.newManifest(nil, nil)
}

// Recover a database session.
// It needs external sync.
func (s *session) recover() (err error) {
	defer func() {
		if os.IsNotExist(err) {
			// Don't return os.ErrNotExist if the underlying storage contains
			// other files that belong to LevelDB. So the DB won't get trashed.
			if fds, _ := s.stor.List(util.TypeAll); len(fds) > 0 {
				err = &gerrors.ErrFileCorrupted{Err: errors.New("database entry point either missing or corrupted")}
			}
		}
	}()

	fd, err := s.stor.GetMeta()
	if err != nil {
		return
	}

	reader, err := s.stor.Open(fd)
	if err != nil {
		return
	}
	defer reader.Close()

	var (
		// Options.
		strict = s.o.GetStrict(opt.StrictManifest)

		jr      = journal.NewReader(reader, dropper{s, fd}, strict, true)
		rec     = &sessionRecord{}
		staging = s.stVersion.newStaging()
	)
	for {
		var r io.Reader
		r, err = jr.Next()
		if err != nil {
			if err == io.EOF {
				err = nil
				break
			}
			return gerrors.SetFd(err, fd)
		}

		err = rec.decode(r)
		if err == nil {
			// save compact pointers
			for _, r := range rec.compPtrs {
				s.setCompPtr(r.level, r.ikey)
			}
			// commit record to version staging
			staging.commit(rec)
		} else {
			err = gerrors.SetFd(err, fd)
			if strict || !gerrors.IsCorrupted(err) {
				return
			}
			s.logf("manifest error: %v (skipped)", gerrors.SetFd(err, fd))
		}
		rec.resetCompPtrs()
		rec.resetAddedTables()
		rec.resetDeletedTables()
	}

	switch {
	case !rec.has(recComparer):
		return newErrManifestCorrupted(fd, "comparer", "missing")
	case rec.comparer != s.ikc.ucmp.Name():
		return newErrManifestCorrupted(fd, "comparer", fmt.Sprintf("mismatch: want '%s', got '%s'", s.ikc.ucmp.Name(), rec.comparer))
	case !rec.has(recNextFileNum):
		return newErrManifestCorrupted(fd, "next-file-num", "missing")
	case !rec.has(recJournalNum):
		return newErrManifestCorrupted(fd, "journal-file-num", "missing")
	case !rec.has(recSeqNum):
		return newErrManifestCorrupted(fd, "seq-num", "missing")
	}

	s.manifestFd = fd
	s.setVersion(rec, staging.finish(false))
	s.setNextFileNum(rec.nextFileNum)
	s.recordCommited(rec)
	return nil
}

// Commit session; need external synchronization.
func (s *session) commit(r *sessionRecord, trivial bool) (err error) {
	v := s.version()
	defer v.release()

	// spawn new version based on current version
	nv := v.spawn(r, trivial)

	// abandon useless version id to prevent blocking version processing loop
	defer func() {
		if err != nil {
			s.abandon <- nv.id
			s.logf("commit@abandon useless vid D%d", nv.id)
		}
	}()

	if s.manifest == nil {
		// manifest journal writer not yet created, create one
		err = s.newManifest(r, nv)
	} else if s.manifest.Size() >= s.o.GetMaxManifestFileSize() {
		err = s.newManifest(nil, nv) // avoid over-reference table file
	} else {
		err = s.flushManifest(r)
	}

	// finally, apply new version if no error rise
	if err == nil {
		s.setVersion(r, nv)
	}

	return
}
