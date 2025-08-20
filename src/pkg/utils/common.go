package utils

import (
	"bytes"
	"encoding/binary"

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
