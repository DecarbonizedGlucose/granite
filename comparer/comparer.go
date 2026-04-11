package comparer

type BasicComparer interface {
	// Compare(a, b) returns -1, 0, 1,
	// depending on some needed comparation regulation.
	Compare(a, b []byte) int
}

type Comparer interface {
	BasicComparer

	// Name returns name of the comparer,
	// which shows the function of itself.
	Name() string

	// Bwllow are advanced functions, which are used for reduce
	// the space requirement of some block.
	//
	// Strictly speaking, they are not part of the semantics of this
	// interface, but considering that the API is consistent, we
	// chose to bind these two functions to this interface.

	// Separator appends a sequence of bytes x to dst such that a <= x && x < b,
	// where 'less than' is consistent with Compare. An implementation should
	// return nil if x equal to a.
	//
	// Either contents of a or b should not by any means modified. Doing so
	// may cause corruption on the internal state.
	Separator(dst, a, b []byte) []byte

	// Successor appends a sequence of bytes x to dst such that x >= b, where
	// 'less than' is consistent with Compare. An implementation should return
	// nil if x equal to b.
	//
	// Contents of b should not by any means modified. Doing so may cause
	// corruption on the internal state.
	Successor(dst, key []byte) []byte
}
