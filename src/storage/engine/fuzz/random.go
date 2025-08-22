package fuzz

import (
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"math/rand"
)

func randomTableName(r *rand.Rand) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"

	n := 8
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[r.Intn(len(letters))]
	}

	return "tbl_" + string(b)
}

func randomIndexName(r *rand.Rand) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	n := 8
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[r.Intn(len(letters))]
	}
	return "idx_" + string(b)
}

func randomSchema(r *rand.Rand) storage.Schema {
	colCount := 1 + r.Intn(4)
	s := make(storage.Schema, colCount)

	used := map[string]struct{}{}
	for len(s) < colCount {
		name := randomColumnName(r)
		if _, ok := used[name]; ok {
			continue
		}
		used[name] = struct{}{}
		typ := "string"
		if r.Intn(2) == 0 {
			typ = "int"
		}
		s[name] = storage.Column{Name: name, Type: typ}
	}

	return s
}

func randomColumnName(r *rand.Rand) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	n := 6
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[r.Intn(len(letters))]
	}

	switch r.Intn(5) {
	case 0:
		return "id"
	case 1:
		return "name"
	default:
		return string(b)
	}
}

func schemaColumnNames(s storage.Schema) []string {
	out := make([]string, 0, len(s))

	for k := range s {
		out = append(out, k)
	}

	return out
}

func randomIndexColumns(r *rand.Rand, s storage.Schema) []string {
	cols := schemaColumnNames(s)
	if len(cols) == 0 {
		return nil
	}
	// 1..min(2,len)
	k := 1 + r.Intn(min(2, len(cols)))
	// выбираем без повторений
	r.Shuffle(len(cols), func(i, j int) { cols[i], cols[j] = cols[j], cols[i] })

	return append([]string(nil), cols[:k]...)
}

func pickRandomKey[K comparable, V any](r *rand.Rand, m map[K]V) (K, bool) {
	var zero K
	n := len(m)
	if n == 0 {
		return zero, false
	}
	i := r.Intn(n)
	for k := range m {
		if i == 0 {
			return k, true
		}
		i--
	}
	return zero, false
}

func genRandomOp(r *rand.Rand, m *Model, nextTxn *common.TxnID) Operation {
	try := r.Intn(6)

	var op Operation
	op.TxnID = *nextTxn

	switch try {
	case 0:
		op = Operation{
			Type:  OpCreateVertexTable,
			Name:  randomTableName(r),
			TxnID: *nextTxn,
		}
	case 1:
		if name, ok := pickRandomKey(r, m.VertexTables); ok {
			op = Operation{
				Type:  OpDropVertexTable,
				Name:  name,
				TxnID: *nextTxn,
			}
		} else {
			return genRandomOp(r, m, nextTxn) // fallback
		}
	case 2:
		op = Operation{
			Type:  OpCreateEdgeTable,
			Name:  randomTableName(r),
			TxnID: *nextTxn,
		}
	case 3:
		if name, ok := pickRandomKey(r, m.EdgeTables); ok {
			op = Operation{
				Type:  OpDropEdgeTable,
				Name:  name,
				TxnID: *nextTxn,
			}
		} else {
			return genRandomOp(r, m, nextTxn) // fallback
		}
	case 4:
		var kind string
		if len(m.VertexTables) == 0 && len(m.EdgeTables) == 0 {
			return genRandomOp(r, m, nextTxn) // нечего индексировать
		}
		if len(m.VertexTables) > 0 && len(m.EdgeTables) > 0 {
			if r.Intn(2) == 0 {
				kind = "vertex"
			} else {
				kind = "edge"
			}
		} else if len(m.VertexTables) > 0 {
			kind = "vertex"
		} else {
			kind = "edge"
		}

		var tblName string
		var ok bool
		var schema storage.Schema
		if kind == "vertex" {
			tblName, ok = pickRandomKey(r, m.VertexTables)
			if ok {
				schema = m.VertexTables[tblName]
			}
		} else {
			tblName, ok = pickRandomKey(r, m.EdgeTables)
			if ok {
				schema = m.EdgeTables[tblName]
			}
		}

		if !ok || len(schema) == 0 {
			return genRandomOp(r, m, nextTxn) // fallback
		}

		op = Operation{
			Type:      OpCreateIndex,
			Name:      randomIndexName(r),
			Table:     tblName,
			TableKind: kind,
			Columns:   randomIndexColumns(r, schema),
			TxnID:     *nextTxn,
		}
	case 5:
		if name, ok := pickRandomKey(r, m.Indexes); ok {
			op = Operation{
				Type:  OpDropIndex,
				Name:  name,
				TxnID: *nextTxn,
			}
		} else {
			return genRandomOp(r, m, nextTxn) // fallback
		}
	}

	*nextTxn++

	return op
}
