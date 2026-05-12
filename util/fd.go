package util

import (
	"fmt"
)

type FileType int

const (
	TypeManifest FileType = 1 << iota
	TypeJournal
	TypeSSTable
	TypeTemp

	TypeAll = TypeManifest | TypeJournal | TypeSSTable | TypeTemp
)

func (t FileType) String() string {
	switch t {
	case TypeManifest:
		return "manifest"
	case TypeJournal:
		return "journal"
	case TypeSSTable:
		return "sstable"
	case TypeTemp:
		return "temp"
	default:
		return "unknown"
	}
}

type FileDesc struct {
	Type FileType
	Num  int64
}

func (fd FileDesc) String() string {
	switch fd.Type {
	case TypeManifest:
		return fmt.Sprintf("MANIFEST-%06d", fd.Num)
	case TypeJournal:
		return fmt.Sprintf("%06d.log", fd.Num)
	case TypeSSTable:
		return fmt.Sprintf("%06d.ldb", fd.Num)
	case TypeTemp:
		return fmt.Sprintf("%06d.tmp", fd.Num)
	default:
		return fmt.Sprintf("%#x-%d", fd.Type, fd.Num)
	}
}

func (fd FileDesc) Zero() bool {
	return fd == (FileDesc{})
}

func (fd FileDesc) Valid() bool {
	switch fd.Type {
	case TypeJournal:
	case TypeManifest:
	case TypeSSTable:
	case TypeTemp:
	default:
		return false
	}
	return fd.Num >= 0
}
