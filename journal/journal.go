package journal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	gerrors "github.com/DecarbonizedGlucose/granite/errors"
	"github.com/DecarbonizedGlucose/granite/util"
)

const (
	fullChunkType   = 1
	firstChunkType  = 2
	middleChunkType = 3
	lastChunkType   = 4
)

const (
	blockSize  = 32 * 1024
	headerSize = 7
)

type flusher interface {
	Flush() error
}

type ErrCorrupted struct {
	Size   int
	Reason string
}

var errSkip = errors.New("granite/journal: skipped")

func (e *ErrCorrupted) Error() string {
	return fmt.Sprintf("granite/journal: block/chunk corrupted: %s (%d bytes)", e.Reason, e.Size)
}

// The Drop method will be called when the journal reader dropping a block or chunk.
type Dropper interface {
	Drop(err error)
}

type JournalReader struct {
	r        io.Reader
	dropper  Dropper
	strict   bool
	checksum bool
	// sequence number of current journal
	seq int
	// buf[i:j] is the unread portion of the current chunk's payload.
	// The low bound, i, excludes the chunk header.
	i, j int
	// n is the number of bytes of buf that are valid. Once reading has started,
	// only the final block can have n < blockSize.
	n    int
	last bool
	err  error
	buf  [blockSize]byte
}

func NewReader(r io.Reader, dropper Dropper, strict, checksum bool) *JournalReader {
	return &JournalReader{
		r:        r,
		dropper:  dropper,
		strict:   strict,
		checksum: checksum,
		last:     true,
	}
}

func (r *JournalReader) corrupt(n int, reason string, skip bool) error {
	if r.dropper != nil {
		r.dropper.Drop(&ErrCorrupted{n, reason})
	}
	if !skip {
		r.err = gerrors.NewErrFileCorrupted(util.FileDesc{}, &ErrCorrupted{n, reason})
		return r.err
	}
	return errSkip
}

// nextChunk sets r.buf[r.i:r.j] to hold the next chunk's payload, reading the
// next block into the buffer if necessary.
func (r *JournalReader) nextChunk(first bool) error {
	for {
		if r.j+headerSize <= r.n {
			checksum := binary.LittleEndian.Uint32(r.buf[r.j+0 : r.j+4])
			length := binary.LittleEndian.Uint16(r.buf[r.j+4 : r.j+6])
			chunkType := r.buf[r.j+6]
			unprocBlock := r.n - r.j
			if checksum == 0 && length == 0 && chunkType == 0 {
				// Drop entire block.
				r.i = r.n
				r.j = r.n
				return r.corrupt(unprocBlock, "zero header", false)
			}
			if chunkType < fullChunkType || chunkType > lastChunkType {
				// Drop entire block.
				r.i = r.n
				r.j = r.n
				return r.corrupt(unprocBlock, fmt.Sprintf("invalid chunk type %#x", chunkType), false)
			}
			r.i = r.j + headerSize
			r.j = r.j + headerSize + int(length)
			if r.j > r.n {
				// Drop entire block.
				r.i = r.n
				r.j = r.n
				return r.corrupt(unprocBlock, "chunk length overflows block", false)
			} else if r.checksum && checksum != util.NewCRC(r.buf[r.i-1:r.j]).Value() {
				// Drop entire block.
				r.i = r.n
				r.j = r.n
				return r.corrupt(unprocBlock, "checksum mismatch", false)
			}
			if first && chunkType != fullChunkType && chunkType != firstChunkType {
				chunkLength := (r.j - r.i) + headerSize
				r.i = r.j
				// Report the error, but skip it.
				return r.corrupt(chunkLength, "orphan chunk", true)
			}
			r.last = chunkType == fullChunkType || chunkType == lastChunkType
			return nil
		}
		// The last block.
		if r.n < blockSize && r.n > 0 {
			if !first {
				return r.corrupt(0, "missing chunk part", false)
			}
			r.err = io.EOF
			return r.err
		}
		// Read block.
		n, err := io.ReadFull(r.r, r.buf[:])
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return err
		}
		if n == 0 {
			if !first {
				return r.corrupt(0, "missing chunk part", false)
			}
			r.err = io.EOF
			return r.err
		}
		r.i, r.j, r.n = 0, 0, n
	}
}

// Next returns a reader for the next journal. It returns io.EOF if
// there are no more journals. The reader returned becomes invalid
// after next call.
// If strict is false, the reader will returns io.ErrUnexpectedEOF
// when found corrupted journal, which is not readable anymore.
func (r *JournalReader) Next() (io.Reader, error) {
	r.seq++
	if r.err != nil {
		return nil, r.err
	}
	r.i = r.j
	for {
		if err := r.nextChunk(true); err == nil {
			break
		} else if err != errSkip {
			return nil, err
		}
	}
	return &singleReader{r: r, seq: r.seq, err: nil}, nil
}

func (r *JournalReader) Reset(reader io.Reader, dropper Dropper, strict, checksum bool) error {
	r.seq++
	err := r.err
	r.r = reader
	r.dropper = dropper
	r.strict = strict
	r.checksum = checksum
	r.i, r.j, r.n = 0, 0, 0
	r.last = true
	r.err = nil
	return err
}

type singleReader struct {
	r   *JournalReader
	seq int
	err error
}

func (x *singleReader) Read(p []byte) (int, error) {
	r := x.r
	if r.seq != x.seq {
		return 0, errors.New("granite/journal: stale reader")
	}
	if x.err != nil {
		return 0, x.err
	}
	if r.err != nil {
		return 0, r.err
	}
	for r.i == r.j {
		if r.last {
			return 0, io.EOF
		}
		x.err = r.nextChunk(false)
		if x.err != nil {
			if x.err == errSkip {
				x.err = io.ErrUnexpectedEOF
			}
			return 0, x.err
		}
	}
	n := copy(p, r.buf[r.i:r.j])
	r.i += n
	return n, nil
}

func (x *singleReader) ReadByte() (byte, error) {
	r := x.r
	if r.seq != x.seq {
		return 0, errors.New("granite/journal: stale reader")
	}
	if x.err != nil {
		return 0, x.err
	}
	if r.err != nil {
		return 0, r.err
	}
	for r.i == r.j {
		if r.last {
			return 0, io.EOF
		}
		x.err = r.nextChunk(false)
		if x.err != nil {
			if x.err == errSkip {
				x.err = io.ErrUnexpectedEOF
			}
			return 0, x.err
		}
	}
	c := r.buf[r.i]
	r.i++
	return c, nil
}

type JournalWriter struct {
	w io.Writer
	// sequence number of current journal
	seq int
	f   flusher
	// buf[i:j] is the bytes that will become the current chunk.
	// The low bound, i, includes the chunk header.
	i, j int
	// buf[:written] has already been written to w.
	// written is 0 if no flush.
	written int
	// current block (now in buf) number, starting from 0
	blockNumber int64
	// whether the current chunk is the first chunk of a journal
	first bool
	// whether the current chunk is buffered but not yet written
	pending bool
	err     error
	buf     [blockSize]byte
}

func NewWriter(w io.Writer) *JournalWriter {
	f, _ := w.(flusher)
	return &JournalWriter{
		w: w,
		f: f,
	}
}

func (w *JournalWriter) Reset(writer io.Writer) error {
	w.seq++
	var err error
	if w.err == nil {
		w.writePending()
		err = w.err
	}
	w.w = writer
	w.f, _ = writer.(flusher)
	w.i, w.j, w.written = 0, 0, 0
	w.blockNumber = 0
	w.first = false
	w.pending = false
	w.err = nil
	return err
}

// fillHeader fills the chunk header to buf for pending chunk
func (w *JournalWriter) fillHeader(last bool) {
	if w.i+headerSize > w.j || w.j > blockSize {
		panic("granite/journal: bad writer state")
	}
	if last {
		if w.first {
			w.buf[w.i+6] = fullChunkType
		} else {
			w.buf[w.i+6] = lastChunkType
		}
	} else {
		if w.first {
			w.buf[w.i+6] = firstChunkType
		} else {
			w.buf[w.i+6] = middleChunkType
		}
	}
	binary.LittleEndian.PutUint32(w.buf[w.i+0:w.i+4], util.NewCRC(w.buf[w.i+6:w.j]).Value())
	binary.LittleEndian.PutUint16(w.buf[w.i+4:w.i+6], uint16(w.j-w.i-headerSize))
}

// writeBlock writes the buffered block to underlying writer
// and reserves space for next use
func (w *JournalWriter) writeBlock() {
	_, w.err = w.w.Write(w.buf[w.written:])
	w.i, w.j = 0, headerSize
	w.written = 0
	w.blockNumber++
}

// writePending writes the current journal and writes the buffer
// to the underlying writer
func (w *JournalWriter) writePending() {
	if w.err != nil {
		return
	}
	if w.pending {
		w.fillHeader(true)
		w.pending = false
	}
	_, w.err = w.w.Write(w.buf[w.written:w.j])
	w.written = w.j
}

// Closer finishes the current journal and closes the writer
func (w *JournalWriter) Close() error {
	w.seq++
	w.writePending()
	if w.err != nil {
		return w.err
	}
	w.err = errors.New("granite/journal: closed writer")
	return nil
}

// Flush finishes the current journal, writes to the underlying writer, and
// flushes it if that writer implements interface{ Flush() error }.
func (w *JournalWriter) Flush() error {
	w.seq++
	w.writePending()
	if w.err != nil {
		return w.err
	}
	if w.f != nil {
		w.err = w.f.Flush()
		return w.err
	}
	return nil
}

// Next returns a writer for the next journal. The writer returned
// becomes invalid after next Close, Flush or call.
func (w *JournalWriter) Next() (io.Writer, error) {
	w.seq++
	if w.err != nil {
		return nil, w.err
	}
	if w.pending {
		w.fillHeader(true)
	}
	w.i = w.j
	w.j += headerSize
	// check if there is room in the block for the header
	if w.j > blockSize {
		for k := w.i; k < blockSize; k++ {
			w.buf[k] = 0
		}
		w.writeBlock()
		if w.err != nil {
			return nil, w.err
		}
	}
	w.first = true
	w.pending = true
	return singleWriter{w, w.seq}, nil
}

func (w *JournalWriter) Size() int64 {
	if w == nil {
		return 0
	}
	return w.blockNumber*blockSize + int64(w.j)
}

type singleWriter struct {
	w   *JournalWriter
	seq int
}

func (x singleWriter) Write(p []byte) (int, error) {
	w := x.w
	if w.seq != x.seq {
		return 0, errors.New("granite/journal: stale writer")
	}
	if w.err != nil {
		return 0, w.err
	}
	n0 := len(p)
	for len(p) > 0 {
		if w.j == blockSize {
			w.fillHeader(false)
			w.writeBlock()
			if w.err != nil {
				return 0, w.err
			}
			w.first = false
		}
		n := copy(w.buf[w.j:], p)
		w.j += n
		p = p[n:]
	}
	return n0, nil
}
