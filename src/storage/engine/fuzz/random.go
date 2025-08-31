package fuzz

import (
	"cmp"
	"math/rand"
	"slices"
	"strconv"

	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/storage/engine"
)

func getRandomMapKey[K cmp.Ordered, V any](r *rand.Rand, m map[K]V) (K, bool) {
	if len(m) == 0 {
		var zero K

		return zero, false
	}

	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	orderedKeys := slices.Sorted(slices.Values(keys))
	return orderedKeys[r.Intn(len(orderedKeys))], true
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
	schema := make(storage.Schema, 0)

	numCols := 1 + r.Intn(5)

	for i := 0; i < numCols; i++ {
		colName := "col" + strconv.Itoa(i)
		types := []storage.ColumnType{
			storage.ColumnTypeInt64,
			storage.ColumnTypeUint64,
			storage.ColumnTypeFloat64,
			storage.ColumnTypeUUID,
		} // add your supported types
		schema = append(schema, storage.Column{
			Name: colName,
			Type: types[r.Intn(len(types))],
		})
	}

	return schema
}

func randomVertexIndexNameForCreate(
	r *rand.Rand,
	existingTables map[string]storage.IndexMeta,
	exist int,
) string {
	d := r.Intn(10)

	if d < exist {
		if name, ok := getRandomMapKey(r, existingTables); ok {
			return name
		}
	}

	return engine.FormVertexIndexName(randomString(r, 10))
}

func randomEdgeIndexNameForCreate(
	r *rand.Rand,
	existingTables map[string]storage.IndexMeta,
	exist int,
) string {
	d := r.Intn(10)

	if d < exist {
		if name, ok := getRandomMapKey(r, existingTables); ok {
			return name
		}
	}

	return engine.FormEdgeIndexName(randomString(r, 10))
}

func randomIndexNameForDrop(
	r *rand.Rand,
	existingIndexes map[string]storage.IndexMeta,
	exist int,
) string {
	d := r.Intn(10)

	if d < exist {
		if name, ok := getRandomMapKey(r, existingIndexes); ok {
			return name
		}
	}

	if r.Intn(2) == 0 {
		return engine.FormVertexIndexName(randomString(r, 10))
	} else {
		return engine.FormEdgeIndexName(randomString(r, 10))
	}
}
