package granite

import (
	"github.com/DecarbonizedGlucose/granite/comparer"
)

type ikComparer struct {
	ucmp comparer.Comparer
}

func (c *ikComparer) Name() string {
	return "granite.InternalKeyComparer:" + c.ucmp.Name()
}

func (c *ikComparer) Compare(a, b []byte) int {
	x := c.ucmp.Compare(internalKey(a).ukey(), internalKey(b).ukey())
	if x != 0 {
		return x
	}
	an, bn := internalKey(a).num(), internalKey(b).num()
	if an > bn {
		return -1
	} else if an < bn {
		return 1
	}
	return 0
}

func (c *ikComparer) Separator(dst, a, b []byte) []byte {
	ua, ub := internalKey(a).ukey(), internalKey(b).ukey()
	dst = c.ucmp.Separator(dst, ua, ub)
	if dst != nil && len(dst) < len(ua) && c.ucmp.Compare(ua, dst) < 0 {
		// append earliest possible number
		return append(dst, keyMaxNumBytes...)
	}
	return nil
}

func (c *ikComparer) Successor(dst, b []byte) []byte {
	ub := internalKey(b).ukey()
	dst = c.ucmp.Successor(dst, ub)
	if dst != nil && len(dst) < len(ub) && c.ucmp.Compare(ub, dst) < 0 {
		// append earliest possible number
		return append(dst, keyMaxNumBytes...)
	}
	return nil
}
