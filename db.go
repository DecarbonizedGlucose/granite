package granite

import (
	"sync"

	"github.com/DecarbonizedGlucose/granite/journal"
	mt "github.com/DecarbonizedGlucose/granite/memtable"
	"github.com/DecarbonizedGlucose/granite/opt"
	"github.com/DecarbonizedGlucose/granite/sstable"
)

type DB struct {
	mu sync.RWMutex
	s  *session
	o  *opt.Options

	mem  *mt.MemTable // Memory table (skip list)
	fmem *mt.MemTable // Frozen memory table
	j    *journal.Journal
	sstw *sstable.TableWriter
	sstr *sstable.TableReader
}

func (db *DB) initJournal() {

}

func (db *DB) initMemTable() {
	db.mem = mt.NewMemTable(nil, 0)
	db.fmem = mt.NewMemTable(nil, 0)
}

func newDB() *DB {
	db := &DB{}

	return db
}

func Open(o *opt.Options) (*DB, error) {
	return nil, nil
}
