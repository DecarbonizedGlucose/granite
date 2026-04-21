package iterator

// more like some state
type Direction int

const (
	Forward Direction = iota
	Backward

	SOI // start of iteration
	EOI // end of iteration
	Released
	DefaultInvalid
)

func (d Direction) Valid() bool {
	return d == Forward || d == Backward
}
