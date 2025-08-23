package utils

import (
	"bytes"
	"encoding/binary"
	"math/rand"

	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
)

func Must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}

	return v
}

type Pair[T, K any] struct {
	First  T
	Second K
}

func (p Pair[T, K]) Destruct() (T, K) {
	return p.First, p.Second
}

func ToBytes[T any](v T) []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, v)
	assert.NoError(err)
	return buf.Bytes()
}

func FromBytes[T any](b []byte) T {
	var v T
	buf := bytes.NewReader(b)
	err := binary.Read(buf, binary.BigEndian, &v)
	assert.NoError(err)
	return v
}

func MergeMaps[K comparable, V any](maps ...map[K]V) map[K]V {
	result := make(map[K]V)
	for _, m := range maps {
		for k, v := range m {
			result[k] = v
		}
	}
	return result
}

type Integer interface {
	~int64 | ~uint64 | ~int
}

func GenerateUniqueInts[T Integer](n, min, max int) []T {
	assert.Assert(min <= max, "min must be less than or equal to max")

	rangeSize := max - min + 1
	assert.Assert(
		n <= rangeSize,
		"cannot generate more unique numbers than available in range",
	)

	if n == 0 {
		return []T{}
	}

	if n > rangeSize/2 {
		all := make([]T, rangeSize)
		for i := range rangeSize {
			all[i] = T(min + i)
		}

		// Fisher-Yates shuffle - only shuffle first n elements
		for i := range n {
			j := rand.Intn(rangeSize-i) + i
			all[i], all[j] = all[j], all[i]
		}

		return all[:n]
	}

	nums := make(map[T]struct{}, n)
	res := make([]T, 0, n)

	for len(res) < n {
		val := T(rand.Intn(rangeSize) + min)
		if _, exists := nums[val]; !exists {
			nums[val] = struct{}{}
			res = append(res, val)
		}
	}

	return res
}
