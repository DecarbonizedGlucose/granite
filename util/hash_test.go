package util

import (
	"math/rand"
	"testing"
)

func TestHashDeterministic(t *testing.T) {
	data := []byte("hello world")
	seed := uint32(123)

	got1 := Hash(data, seed)
	got2 := Hash(data, seed)

	if got1 != got2 {
		t.Fatalf("expected deterministic hash, got %d and %d", got1, got2)
	}
}

func TestHashProducesManyDistinct(t *testing.T) {
	rnd := rand.New(rand.NewSource(1))
	seen := make(map[uint32]struct{})

	const samples = 1000
	for i := 0; i < samples; i++ {
		size := rnd.Intn(128)
		data := make([]byte, size)
		if _, err := rnd.Read(data); err != nil {
			t.Fatalf("rand read failed: %v", err)
		}
		seed := rnd.Uint32()
		seen[Hash(data, seed)] = struct{}{}
	}

	if len(seen) < 950 {
		t.Fatalf("expected many distinct hashes, got %d/%d", len(seen), samples)
	}
}
