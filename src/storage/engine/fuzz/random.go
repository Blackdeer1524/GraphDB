package fuzz

import (
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"math/rand"
	"strconv"
)

func getRandomMapKey[K comparable, V any](r *rand.Rand, m map[K]V) (K, bool) {
	if len(m) == 0 {
		var zero K

		return zero, false
	}

	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	return keys[r.Intn(len(keys))], true
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
		if name, ok := getRandomMapKey(r, existing); ok {
			return name
		}
	}

	return "tbl_" + randomString(r, 10)
}

func randomSchema(r *rand.Rand) storage.Schema {
	schema := make(storage.Schema)

	numCols := 1 + r.Intn(5)

	for i := 0; i < numCols; i++ {
		colName := "col" + strconv.Itoa(i)
		types := []string{"int", "string", "float", "bool"} // add your supported types
		schema[colName] = storage.Column{Type: types[r.Intn(len(types))]}
	}

	return schema
}

func randomIndexNameForCreate(r *rand.Rand, existingTables map[string]storage.Index, exist int) string {
	d := r.Intn(10)

	if d < exist {
		if name, ok := getRandomMapKey(r, existingTables); ok {
			return name
		}
	}

	return "idx_" + randomString(r, 10)
}

func randomIndexNameForDrop(r *rand.Rand, existingIndexes map[string]storage.Index, exist int) string {
	d := r.Intn(10)

	if d < exist {
		if name, ok := getRandomMapKey(r, existingIndexes); ok {
			return name
		}
	}

	return "idx_" + randomString(r, 10)
}
