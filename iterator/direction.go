package iterator

type Direction int

const (
	Forward Direction = iota
	Backward

	SOI // start of iteration
	EOI // end of iteration
	Released
	DefaultInvalid
)
