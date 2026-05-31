package granite

import (
	"github.com/DecarbonizedGlucose/granite/filter"
)

type iFilter struct {
	filter.FilterPolicy
}

func (f *iFilter) MayContain(filter, key []byte) bool {
	return f.FilterPolicy.MayContain(filter, internalKey(key).ukey())
}

func (f iFilter) NewGenerator() filter.FilterGenerator {
	return &iFilterGenerator{f.FilterPolicy.NewGenerator()}
}

type iFilterGenerator struct {
	filter.FilterGenerator
}

func (g *iFilterGenerator) Add(key []byte) {
	g.FilterGenerator.Add(internalKey(key).ukey())
}
