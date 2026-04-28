package granite

import (
	"encoding/binary"
	"fmt"

	gerrors "github.com/DecarbonizedGlucose/granite/errors"
	"github.com/DecarbonizedGlucose/granite/util"
)

type internalKey []byte
type keyType byte

const (
	keyTypeDel keyType = 0
	keyTypeVal keyType = 1
)

const (
	keyMaxSeq = (uint64(1) << 56) - 1
	keyMaxNum = keyMaxSeq<<8 | uint64(1)
)

var keyMaxNumBytes = func() []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, keyMaxNum)
	return buf
}()

func createInternalKey(dst, ukey []byte, seq uint64, kt keyType) internalKey {
	if seq > keyMaxSeq {
		panic("granite: invalid sequence number")
	} else if kt > keyTypeVal {
		panic("granite: invalid type")
	}

	dst = util.EnsureBuffer(dst, len(ukey)+8)
	copy(dst, ukey)
	binary.LittleEndian.PutUint64(dst[len(ukey):], (seq<<8)|uint64(kt))
	return internalKey(dst)
}

func parseInteralKey(ik []byte) (ukey []byte, seq uint64, kt keyType, err error) {
	if len(ik) < 8 {
		return nil, 0, 0, gerrors.ErrInvalidKeyLength
	}
	num := binary.LittleEndian.Uint64(ik[len(ik)-8:]) // type and seq
	seq, kt = num>>8, keyType(num&0xff)
	if kt > keyTypeVal {
		return nil, 0, 0, gerrors.ErrInvalidKeyType
	}
	ukey = ik[:len(ik)-8]
	return
}

func (k internalKey) valid() bool {
	_, _, _, err := parseInteralKey(k)
	return err == nil
}

func (k internalKey) assert() {
	if k == nil {
		panic("granite: nil internalKey")
	}
	if len(k) < 8 {
		panic(fmt.Sprintf("granite: internal key %q, len=%d: invalid length", []byte(k), len(k)))
	}
}

func (ik internalKey) ukey() []byte {
	ik.assert()
	return ik[:len(ik)-8]
}

func (ik internalKey) num() uint64 {
	ik.assert()
	return binary.LittleEndian.Uint64(ik[len(ik)-8:])
}

func (ik internalKey) parseNum() (seq uint64, kt keyType) {
	num := ik.num()
	seq, kt = num>>8, keyType(num&0xff)
	if kt > keyTypeVal {
		panic(fmt.Sprintf("granite: internal key %q, len=%d: invalid type %#x", []byte(ik), len(ik), kt))
	}
	return
}
