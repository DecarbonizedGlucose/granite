package granite

import (
	"sync"

	"github.com/DecarbonizedGlucose/granite/journal"
	"github.com/DecarbonizedGlucose/granite/storage"
	"github.com/DecarbonizedGlucose/granite/util"
)

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
	manifestedFd   util.FileDesc

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
