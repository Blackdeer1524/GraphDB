package index

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeterministicHasher64_DeterminismAndSeedInfluence(t *testing.T) {
	data := []byte("the quick brown fox jumps over the lazy dog")

	// Determinism for same seed
	h1 := NewDeterministicHasher64(123456789)
	h1.Write(data)
	sum1 := h1.Sum64()

	h2 := NewDeterministicHasher64(123456789)
	h2.Write(data)
	sum2 := h2.Sum64()

	require.Equal(t, sum1, sum2, "same seed must produce same hash")

	// Different seeds should normally change the result
	h3 := NewDeterministicHasher64(987654321)
	h3.Write(data)
	sum3 := h3.Sum64()

	assert.NotEqual(t, sum1, sum3, "different seeds should normally produce different hashes")
}

func TestDeterministicHasher64_ResetAndSetSeed(t *testing.T) {
	data := []byte("hello")

	// Reset should restore initial state for the current seed
	h := NewDeterministicHasher64(0)
	h.Write([]byte("x"))
	h.Reset()
	h.Write(data)
	a := h.Sum64()

	h2 := NewDeterministicHasher64(0)
	h2.Write(data)
	b := h2.Sum64()

	require.Equal(t, a, b)

	// SetSeed should be equivalent to creating a new hasher with that seed
	h.SetSeed(42)
	h.Write(data)
	c := h.Sum64()

	h3 := NewDeterministicHasher64(42)
	h3.Write(data)
	d := h3.Sum64()

	assert.Equal(t, c, d)
}
