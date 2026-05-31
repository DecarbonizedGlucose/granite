package granite

import (
	"sync/atomic"

	"github.com/DecarbonizedGlucose/granite/storage"
	"github.com/DecarbonizedGlucose/granite/util"
)

type iStorageReader struct {
	storage.Reader
	c *iStorage
}

func (r *iStorageReader) Read(p []byte) (n int, err error) {
	n, err = r.Reader.Read(p)
	atomic.AddUint64(&r.c.read, uint64(n))
	return n, err
}

func (r *iStorageReader) ReadAt(p []byte, off int64) (n int, err error) {
	n, err = r.Reader.ReadAt(p, off)
	atomic.AddUint64(&r.c.read, uint64(n))
	return n, err
}

type iStorageWriter struct {
	storage.Writer
	c *iStorage
}

func (w *iStorageWriter) Write(p []byte) (n int, err error) {
	n, err = w.Writer.Write(p)
	atomic.AddUint64(&w.c.write, uint64(n))
	return n, err
}

type iStorage struct {
	storage.Storage
	read  uint64
	write uint64
}

func (c *iStorage) Open(fd util.FileDesc) (storage.Reader, error) {
	r, err := c.Storage.Open(fd)
	return &iStorageReader{Reader: r, c: c}, err
}

func (c *iStorage) Create(fd util.FileDesc) (storage.Writer, error) {
	w, err := c.Storage.Create(fd)
	return &iStorageWriter{Writer: w, c: c}, err
}

func (c *iStorage) reads() uint64 {
	return atomic.LoadUint64(&c.read)
}

func (c *iStorage) writes() uint64 {
	return atomic.LoadUint64(&c.write)
}

func newIStorage(stor storage.Storage) *iStorage {
	return &iStorage{Storage: stor, read: 0, write: 0}
}
