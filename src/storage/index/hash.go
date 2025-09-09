package index

// Deterministic seeded 64-bit hasher based on FNV-1a.
// The seed is mixed into the initial state to make the mapping deterministic
// and reproducible across process runs as long as the same seed is used.

// DefaultHashSeed is used where a stable, process-independent seed is desired.
// Chosen as an arbitrary odd 64-bit constant (related to golden ratio).
const DefaultHashSeed uint64 = 0x9e3779b97f4a7c15

const (
	fnv64Offset = 1469598103934665603
	fnv64Prime  = 1099511628211
)

// DeterministicHasher64 implements a simple seeded FNV-1a 64-bit hash.
type DeterministicHasher64 struct {
	seed  uint64
	state uint64
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
	h.state = fnv64Offset ^ h.seed
}

// Write mixes the provided bytes into the hash state.
func (h *DeterministicHasher64) Write(p []byte) int {
	for _, b := range p {
		h.state ^= uint64(b)
		h.state *= fnv64Prime
	}
	return len(p)
}

// Sum64 returns the current hash value.
func (h *DeterministicHasher64) Sum64() uint64 {
	return h.state
}
