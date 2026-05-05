package journal

import (
	"encoding/binary"
	"hash/crc32"
	"io"

	gerrors "github.com/DecarbonizedGlucose/granite/errors"
	"github.com/DecarbonizedGlucose/granite/util"
)

type RecordType byte

const (
	SeqMask     RecordType = 0x0F
	EntryPut    byte       = 1
	EntryDelete byte       = 2
)

/*
Record is the on-disk unit:
[CRC32(4)][len(4)][seq(8)][cnt(4)][entries...]
[     Head(8)    ][         Body(len)        ]
entries:
[type(1)][keyLen(4)][key][valLen(4)][val]

one seq is for one batch
*/

type Entry struct {
	Type  byte
	Key   []byte
	Value []byte
}

type Record struct {
	entries []*Entry
}

// AddEntry appends one piece of operation entry to the record.
// The entry should be not modified after added.
func (r *Record) AddEntry(entry *Entry) {
	if entry == nil {
		return
	}
	r.entries = append(r.entries, entry)
}

type JourType int

const (
	FreeJour JourType = iota
	WritingJour
	ReadingJour
)

type Journal struct {
	f    io.ReadWriteCloser
	path string

	buf     []byte
	off     int
	t       JourType
	lastCRC uint32
	size    int

	closed  bool
	closeCh chan struct{}
	err     error
}

func (j *Journal) writeHeader(size int) {
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	h.Write(j.buf[:size])
	crc := h.Sum32()

	var header [8]byte
	binary.LittleEndian.PutUint32(header[0:4], crc)
	binary.LittleEndian.PutUint32(header[4:8], uint32(size))
	if j.err != nil {
		return
	}
	_, j.err = j.f.Write(header[:])
}

func (j *Journal) fillEntry(e *Entry) {
	if e == nil {
		return
	}
	if e.Type != EntryPut && e.Type != EntryDelete {
		panic("granite: invalid entry type")
	}
	if e.Key == nil || e.Value == nil {
		panic("granite: nil key or value")
	}
	if len(e.Key) < 8 {
		panic("granite: key too short")
	}

	j.buf[j.off] = e.Type
	j.off++
	binary.LittleEndian.PutUint32(j.buf[j.off:], uint32(len(e.Key)))
	j.off += 4
	copy(j.buf[j.off:], e.Key)
	j.off += len(e.Key)
	binary.LittleEndian.PutUint32(j.buf[j.off:], uint32(len(e.Value)))
	j.off += 4
	copy(j.buf[j.off:], e.Value)
	j.off += len(e.Value)
}

func (j *Journal) writeRecord() {
	if j.err != nil {
		return
	}
	_, j.err = j.f.Write(j.buf[:j.off])
}

func (j *Journal) flush() {
	if j.err != nil {
		return
	}
	if f, ok := j.f.(interface{ Sync() error }); ok {
		j.err = f.Sync()
	}
}

// OpenCreate create a new file for MemTable
func OpenCreate(path string) (*Journal, error) {
	return nil, nil
}

// Append appends a record, including a batch of entries to the journal file
func (j *Journal) Append(seq uint64, re *Record, s bool) (size int, err error) {
	size = 0
	for _, e := range re.entries {
		size += 1 + 4 + len(e.Key) + 4 + len(e.Value)
	}
	util.EnsureBuffer(j.buf, size)
	j.off = 0
	for _, e := range re.entries {
		j.fillEntry(e)
	}
	j.writeHeader(size)
	j.writeRecord()
	if s {
		j.flush()
	}
	return
}

func (j *Journal) Delete() error {
	return nil
}

// OpenReplay reads a existing file to reconstruct MemTable
func OpenReplay(path string) (*Journal, error) {
	return nil, nil
}

func (j *Journal) readHeader() int {
	if j.err != nil {
		return -1
	}
	var header [8]byte
	_, j.err = io.ReadFull(j.f, header[:])
	if j.err != nil {
		return -1
	}
	j.lastCRC = binary.LittleEndian.Uint32(header[0:4])
	if !j.check() {
		j.err = gerrors.ErrFileBroken
		return -1
	}
	return int(binary.LittleEndian.Uint32(header[4:8]))
}

func (j *Journal) readRecord(bodyLen int) {
	if j.err != nil {
		return
	}
	util.EnsureBuffer(j.buf, bodyLen)
	j.buf = j.buf[:bodyLen]
	_, j.err = io.ReadFull(j.f, j.buf)
}

func (j *Journal) check() bool {
	if j.err != nil {
		return false
	}
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	h.Write(j.buf)
	return h.Sum32() == j.lastCRC
}

func (j *Journal) takeoutEntry() (*Entry, bool) {
	if j.err != nil || j.off >= len(j.buf) {
		return nil, false
	}
	e := &Entry{}
	e.Type = j.buf[j.off]
	j.off++

	kLen := binary.LittleEndian.Uint32(j.buf[j.off:])
	j.off += 4
	e.Key = make([]byte, kLen)
	copy(e.Key, j.buf[j.off:j.off+int(kLen)])
	j.off += int(kLen)

	vLen := binary.LittleEndian.Uint32(j.buf[j.off:])
	j.off += 4
	e.Value = make([]byte, vLen)
	copy(e.Value, j.buf[j.off:j.off+int(vLen)])
	j.off += int(vLen)

	return e, true
}

func (j *Journal) Replay() ([]*Record, error) {
	var records []*Record
	for {
		bodyLen := j.readHeader()
		if j.err == io.EOF {
			j.err = nil
			break
		}
		if j.err != nil || bodyLen <= 0 {
			return nil, j.err
		}
		j.readRecord(bodyLen)
		if j.err != nil {
			return nil, j.err
		}
		if !j.check() {
			j.err = nil
			break
		}
		j.off = 0
		r := &Record{}
		for {
			e, ok := j.takeoutEntry()
			if !ok {
				break
			}
			r.AddEntry(e)
		}
		records = append(records, r)
	}
	return records, j.err
}

// Close saves journal and closes the open file
func (j *Journal) Close() error {
	return nil
}

func (j *Journal) IsClosed() bool {
	return j.closed
}

func (j *Journal) Size() int {
	return j.size
}

func (j *Journal) Reset() error {
	return nil
}

func (j *Journal) Path() string {
	return j.path
}

func (j *Journal) Type() JourType {
	return j.t
}

func (j *Journal) SetType(t JourType) {
	switch t { // TODO: add more file status check
	case FreeJour:
		j.t = FreeJour
	case WritingJour:
		j.t = WritingJour
	case ReadingJour:
		j.t = ReadingJour
	}
}

func NewJournal() *Journal {
	j := &Journal{}
	return j
}
