package fuzz

import (
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"math/rand"
)

func getRandomMapKey[K comparable, V any](m map[K]V) (K, bool) {
	var zero K

	if len(m) > 0 {
		for k := range m {
			return k, true
		}
	}

	return zero, false
}

func randomString(r *rand.Rand, n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	b := make([]byte, n)

	for i := range b {
		b[i] = letters[r.Intn(len(letters))]
	}

	return string(b)
}

func randomTableName(r *rand.Rand, existing map[string]storage.Schema, exist int) string {
	d := r.Intn(10)

	if d < exist {
		if name, ok := getRandomMapKey(existing); ok {
			return name
		}
	}

	return "tbl_" + randomString(r, 10)
}

func randomSchema() storage.Schema {
	return storage.Schema{
		"id": storage.Column{Type: "int"},
	}
}

func randomIndexNameForCreate(r *rand.Rand, existingTables map[string]storage.Schema) string {
	return "idx_" + randomString(r, 10)
}

func randomIndexNameForDrop(r *rand.Rand, existingIndexes map[string]storage.Index) string {
	return "idx_" + randomString(r, 10)
}
