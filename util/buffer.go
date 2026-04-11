package util

import (
	"bytes"
	"io"

	gerrors "github.com/DecarbonizedGlucose/granite/errors"
)

const smallBufferSize = 64
const maxInt = int(^uint(0) >> 1)

// A Buffer based on bytes.Buffer, but with more ability
// to control the buffer and avoid unnecessary copying.
type Buffer struct {
	buf []byte
	off int
}

func (b *Buffer) Bytes() []byte { return b.buf[b.off:] }

func (b *Buffer) String() string {
	if b == nil {
		// Special case, useful in debugging.
		return "<nil>"
	}
	return string(b.buf[b.off:])
}

func (b *Buffer) empty() bool { return len(b.buf) <= b.off }

func (b *Buffer) Len() int { return len(b.buf) - b.off }

func (b *Buffer) Cap() int { return cap(b.buf) }

func (b *Buffer) Truncate(n int) {
	if n == 0 {
		b.Reset()
		return
	}
	if n < 0 || n > b.Len() {
		panic("granite/util.Buffer: truncation out of range")
	}
	b.buf = b.buf[:b.off+n]
}

func (b *Buffer) Reset() {
	b.buf = b.buf[:0]
	b.off = 0
}

func (b *Buffer) tryGrowByReslice(n int) (int, bool) {
	if l := len(b.buf); n <= cap(b.buf)-l {
		b.buf = b.buf[:l+n]
		return l, true
	}
	return 0, false
}

func (b *Buffer) grow(n int) int {
	m := b.Len()
	// If buffer is empty, reset to recover space.
	if m == 0 && b.off != 0 {
		b.Reset()
	}
	// Try to grow by means of a reslice.
	if i, ok := b.tryGrowByReslice(n); ok {
		return i
	}
	if b.buf == nil && n <= smallBufferSize {
		b.buf = make([]byte, n, smallBufferSize)
		return 0
	}
	c := cap(b.buf)
	if n <= c/2-m {
		// We can slide things down instead of allocating a new
		// slice. We only need m+n <= c to slide, but
		// we instead let capacity get twice as large so we
		// don't spend all our time copying.
		copy(b.buf, b.buf[b.off:])
	} else if c > maxInt-c-n {
		panic(bytes.ErrTooLarge)
	} else {
		// Not enough space anywhere, we need to allocate.
		buf := sfMakeSlice(2*c + n)
		copy(buf, b.buf[b.off:])
		b.buf = buf
	}
	// Restore b.off and len(b.buf).
	b.off = 0
	b.buf = b.buf[:m+n]
	return m
}

// Allocate allocates n bytes at the end,
// growing the buffer if necessary.
// The caller can write to the buf directly.
func (b *Buffer) Allocate(n int) []byte {
	if n < 0 {
		panic("granite/util.Buffer: negative allocation")
	}
	l, ok := b.tryGrowByReslice(n)
	if !ok {
		l = b.grow(n)
	}
	return b.buf[l:]
}

func (b *Buffer) Grow(n int) {
	if n < 0 {
		panic("granite/util.Buffer.Grow: negative count")
	}
	m := b.grow(n)
	b.buf = b.buf[:m]
}

func (b *Buffer) Write(p []byte) (n int, err error) {
	m, ok := b.tryGrowByReslice(len(p))
	if !ok {
		m = b.grow(len(p))
	}
	return copy(b.buf[m:], p), nil
}

const MinRead = 512

func (b *Buffer) ReadFrom(r io.Reader) (n int64, err error) {
	for {
		i := b.grow(MinRead)
		b.buf = b.buf[:i]
		m, e := r.Read(b.buf[i:cap(b.buf)])
		if m < 0 {
			panic(gerrors.ErrNegativeRead)
		}

		b.buf = b.buf[:i+m]
		n += int64(m)
		if e == io.EOF {
			return n, nil // e is EOF, so return nil explicitly
		}
		if e != nil {
			return n, e
		}
	}
}

// sfMakeSlice allocates slice of n size.
// If failed, panic with known bytes.ErrTooLarge
func sfMakeSlice(n int) []byte {
	defer func() {
		if recover() != nil {
			panic(bytes.ErrTooLarge)
		}
	}()
	return make([]byte, n)
}

func (b *Buffer) WriteByte(c byte) error {
	m, ok := b.tryGrowByReslice(1)
	if !ok {
		m = b.grow(1)
	}
	b.buf[m] = c
	return nil
}

// Read reads the next len(p) bytes from the buffer or until the buffer
// is drained. The return value n is the number of bytes read. If the
// buffer has no data to return, err is [io.EOF] (unless len(p) is zero);
// otherwise it is nil.
func (b *Buffer) Read(p []byte) (n int, err error) {
	if b.empty() {
		// Buffer is empty, reset to recover space.
		b.Reset()
		if len(p) == 0 {
			return
		}
		return 0, io.EOF
	}
	n = copy(p, b.buf[b.off:])
	b.off += n
	return
}

// Next returns a slice containing the next n bytes from the buffer,
// advancing the buffer as if the bytes had been returned by Read.
// If there are fewer than n bytes in the buffer, Next returns the entire buffer.
// The slice is only valid until the next call to a read or write method.
func (b *Buffer) Next(n int) []byte {
	n = min(n, b.Len())
	data := b.buf[b.off : b.off+n]
	b.off += n
	return data
}

// ReadByte reads and returns the next byte from the buffer.
// If no byte is available, it returns error io.EOF.
func (b *Buffer) ReadByte() (c byte, err error) {
	if b.empty() {
		// Buffer is empty, reset to recover space.
		b.Reset()
		return 0, io.EOF
	}
	c = b.buf[b.off]
	b.off++
	return c, nil
}

// ReadBytes reads until the first occurrence of delim in the input,
// returning a slice containing the data up to and including the delimiter.
// If ReadBytes encounters an error before finding a delimiter,
// it returns the data read before the error and the error itself (often io.EOF).
// ReadBytes returns err != nil if and only if the returned data does not end in
// delim.
func (b *Buffer) ReadBytes(delim byte) (line []byte, err error) {
	slice, err := b.readSlice(delim)
	// return a copy of slice. The buffer's backing array may
	// be overwritten by later calls.
	line = append(line, slice...)
	return
}

// readSlice is like ReadBytes but returns a reference to internal buffer data.
func (b *Buffer) readSlice(delim byte) (line []byte, err error) {
	i := bytes.IndexByte(b.buf[b.off:], delim)
	end := b.off + i + 1
	if i < 0 {
		end = len(b.buf)
		err = io.EOF
	}
	line = b.buf[b.off:end]
	b.off = end
	return line, err
}

// NewBuffer creates and initializes a new Buffer using buf as its initial
// contents.  It is intended to prepare a Buffer to read existing data.  It
// can also be used to size the internal buffer for writing. To do that,
// buf should have the desired capacity but a length of zero.
//
// In most cases, new(Buffer) (or just declaring a Buffer variable) is
// sufficient to initialize a Buffer.
func NewBuffer(buf []byte) *Buffer { return &Buffer{buf: buf} }
