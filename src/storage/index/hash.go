package index

import (
	"encoding/binary"
	"hash"
	"hash/fnv"
)

// DefaultHashSeed is used where a stable, process-independent seed is desired.
// Chosen as an arbitrary odd 64-bit constant (related to golden ratio).
const DefaultHashSeed uint64 = 0x9e3779b97f4a7c15

// DeterministicHasher64 wraps stdlib FNV-1a (hash/fnv) with a deterministic seed.
// The seed is written into the hasher state on Reset to perturb the mapping.
type DeterministicHasher64 struct {
	seed uint64
	h    hash.Hash64
}

// NewDeterministicHasher64 creates a hasher with the provided seed.
func NewDeterministicHasher64(seed uint64) DeterministicHasher64 {
	h := DeterministicHasher64{seed: seed}
	h.Reset()
	return h
}

// SetSeed updates the seed and resets the internal state.
func (h *DeterministicHasher64) SetSeed(seed uint64) {
	h.seed = seed
	h.Reset()
}

// Reset initializes the internal state using the seed.
func (h *DeterministicHasher64) Reset() {
	h.h = fnv.New64a()
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], h.seed)
	_, _ = h.h.Write(b[:])
}

// Write mixes the provided bytes into the hash state.
func (h *DeterministicHasher64) Write(p []byte) int {
	n, _ := h.h.Write(p)
	return n
}

// Sum64 returns the current hash value.
func (h *DeterministicHasher64) Sum64() uint64 {
	return h.h.Sum64()
}
