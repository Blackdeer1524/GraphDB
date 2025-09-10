package utils

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateUniqueInts(t *testing.T) {
	t.Run("basic functionality", func(t *testing.T) {
		result := GenerateUniqueInts[int](5, 1, 10, rand.New(rand.NewSource(42)))

		require.Len(t, result, 5, "Should return exactly 5 numbers")

		// Check that all numbers are within range
		for _, num := range result {
			assert.GreaterOrEqual(t, num, 1, "Number should be >= 1")
			assert.LessOrEqual(t, num, 10, "Number should be <= 10")
		}

		// Check uniqueness
		seen := make(map[int]bool)
		for _, num := range result {
			assert.False(
				t,
				seen[num],
				"Number %d should not be duplicated",
				num,
			)
			seen[num] = true
		}
	})

	t.Run("zero count", func(t *testing.T) {
		result := GenerateUniqueInts[int](0, 1, 10, rand.New(rand.NewSource(42)))

		assert.Empty(t, result, "Should return empty slice when count is 0")
	})

	t.Run("single number", func(t *testing.T) {
		result := GenerateUniqueInts[int](1, 5, 5, rand.New(rand.NewSource(42)))

		require.Len(t, result, 1, "Should return exactly 1 number")
		assert.Equal(
			t,
			5,
			result[0],
			"Should return the single available number",
		)
	})

	t.Run("full range", func(t *testing.T) {
		result := GenerateUniqueInts[int](3, 1, 3, rand.New(rand.NewSource(42)))

		require.Len(t, result, 3, "Should return exactly 3 numbers")

		// Should contain all numbers from 1 to 3
		expected := map[int]bool{1: true, 2: true, 3: true}
		for _, num := range result {
			assert.True(
				t,
				expected[num],
				"Number %d should be in expected set",
				num,
			)
		}
	})

	t.Run("different integer types", func(t *testing.T) {
		// Test with int64
		result64 := GenerateUniqueInts[int64](3, 1, 10, rand.New(rand.NewSource(42)))
		assert.Len(t, result64, 3, "int64 result should have length 3")

		// Test with uint64
		resultU64 := GenerateUniqueInts[uint64](3, 1, 10, rand.New(rand.NewSource(42)))
		assert.Len(t, resultU64, 3, "uint64 result should have length 3")

		// Test with int
		resultInt := GenerateUniqueInts[int](3, 1, 10, rand.New(rand.NewSource(42)))
		assert.Len(t, resultInt, 3, "int result should have length 3")
	})

	t.Run("large range", func(t *testing.T) {
		result := GenerateUniqueInts[int](5, 1000, 2000, rand.New(rand.NewSource(42)))

		require.Len(t, result, 5, "Should return exactly 5 numbers")

		// Check that all numbers are within range
		for _, num := range result {
			assert.GreaterOrEqual(t, num, 1000, "Number should be >= 1000")
			assert.LessOrEqual(t, num, 2000, "Number should be <= 2000")
		}
	})

	t.Run("negative numbers", func(t *testing.T) {
		result := GenerateUniqueInts[int](3, -5, -1, rand.New(rand.NewSource(42)))

		require.Len(t, result, 3, "Should return exactly 3 numbers")

		// Check that all numbers are within range
		for _, num := range result {
			assert.GreaterOrEqual(t, num, -5, "Number should be >= -5")
			assert.LessOrEqual(t, num, -1, "Number should be <= -1")
		}
	})
}

func TestGenerateUniqueIntsEdgeCases(t *testing.T) {
	t.Run("min equals max", func(t *testing.T) {
		result := GenerateUniqueInts[int](1, 42, 42, rand.New(rand.NewSource(42)))

		require.Len(t, result, 1, "Should return exactly 1 number")
		assert.Equal(
			t,
			42,
			result[0],
			"Should return the single available number",
		)
	})

	t.Run("range size equals count", func(t *testing.T) {
		result := GenerateUniqueInts[int](5, 1, 5, rand.New(rand.NewSource(42)))

		require.Len(t, result, 5, "Should return exactly 5 numbers")

		// Should contain all numbers from 1 to 5
		expected := map[int]bool{1: true, 2: true, 3: true, 4: true, 5: true}
		for _, num := range result {
			assert.True(
				t,
				expected[num],
				"Number %d should be in expected set",
				num,
			)
		}
	})

	t.Run("count greater than half range", func(t *testing.T) {
		// This tests the Fisher-Yates shuffle path
		result := GenerateUniqueInts[int](6, 1, 10, rand.New(rand.NewSource(42)))

		require.Len(t, result, 6, "Should return exactly 6 numbers")

		// Check that all numbers are within range
		for _, num := range result {
			assert.GreaterOrEqual(t, num, 1, "Number should be >= 1")
			assert.LessOrEqual(t, num, 10, "Number should be <= 10")
		}

		// Check uniqueness
		seen := make(map[int]bool)
		for _, num := range result {
			assert.False(
				t,
				seen[num],
				"Number %d should not be duplicated",
				num,
			)
			seen[num] = true
		}
	})
}
