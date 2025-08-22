package fuzz

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/storage/engine"
	"github.com/Blackdeer1524/GraphDB/src/storage/systemcatalog"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
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

// ======== Генерация операции с учётом текущей модели ========

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

func compareModelWithEngineFS(t *testing.T, baseDir string, se *engine.StorageEngine, m *Model) {
	for tbl := range m.VertexTables {
		_, err := os.Stat(engine.GetVertexTableFilePath(baseDir, tbl))
		require.NoError(t, err, "vertex table file is missing: %s", tbl)
	}

	for tbl := range m.EdgeTables {
		_, err := os.Stat(engine.GetEdgeTableFilePath(baseDir, tbl))
		require.NoError(t, err, "edge table file is missing: %s", tbl)
	}

	for idx := range m.Indexes {
		_, err := os.Stat(engine.GetIndexFilePath(baseDir, idx))
		require.NoError(t, err, "index file is missing: %s", idx)
	}

	for tbl, sch := range m.VertexTables {
		err := se.CreateVertexTable(0, tbl, sch)
		require.Error(t, err, "expected error on duplicate CreateVertexTable(%s)", tbl)
	}

	for tbl, sch := range m.EdgeTables {
		err := se.CreateEdgesTable(0, tbl, sch)
		require.Error(t, err, "expected error on duplicate CreateEdgesTable(%s)", tbl)
	}

	for idx, meta := range m.Indexes {
		err := se.CreateIndex(0, idx, meta.TableName, meta.TableKind, meta.Columns, 8)
		require.Error(t, err, "expected error on duplicate CreateIndex(%s)", idx)
	}
}

func TestFuzz_SingleThreaded(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed=%d", seed)
	r := rand.New(rand.NewSource(seed))

	baseDir := t.TempDir()

	err := systemcatalog.InitSystemCatalog(baseDir, afero.NewOsFs())
	require.NoError(t, err)

	lockMgr := newMockRWMutexLockManager()
	se, err := engine.New(baseDir, uint64(200), afero.NewOsFs(), lockMgr)
	require.NoError(t, err)

	model := newModel()
	var nextTxn common.TxnID = 1

	const opsCount = 600
	for i := 0; i < opsCount; i++ {
		op := genRandomOp(r, model, &nextTxn)
		res := OpResult{Op: op}

		switch op.Type {
		case OpCreateVertexTable:
			// сгенерируем схему и сразу положим её в модель (до вызова create),
			// чтобы индексы могли ссылаться на корректные колонки.
			if _, exists := model.VertexTables[op.Name]; !exists {
				model.VertexTables[op.Name] = randomSchema(r)
			}
			err := se.CreateVertexTable(op.TxnID, op.Name, model.VertexTables[op.Name])
			if err == nil {
				res.Success = true
			} else {
				res.ErrText = err.Error()
			}

		case OpDropVertexTable:
			err := se.DropVertexTable(op.TxnID, op.Name)
			if err == nil {
				res.Success = true
			} else {
				res.ErrText = err.Error()
			}

		case OpCreateEdgeTable:
			if _, exists := model.EdgeTables[op.Name]; !exists {
				model.EdgeTables[op.Name] = randomSchema(r)
			}
			err := se.CreateEdgesTable(op.TxnID, op.Name, model.EdgeTables[op.Name])
			if err == nil {
				res.Success = true
			} else {
				res.ErrText = err.Error()
			}

		case OpDropEdgeTable:
			err := se.DropEdgesTable(op.TxnID, op.Name)
			if err == nil {
				res.Success = true
			} else {
				res.ErrText = err.Error()
			}

		case OpCreateIndex:
			// гарантируем, что таблица и колонки существуют в модели
			var schema storage.Schema
			if op.TableKind == "vertex" {
				schema = model.VertexTables[op.Table]
			} else {
				schema = model.EdgeTables[op.Table]
			}
			if len(op.Columns) == 0 {
				// fallback — защитимся, если генератор дал пустой список
				op.Columns = randomIndexColumns(r, schema)
			}
			err := se.CreateIndex(op.TxnID, op.Name, op.Table, op.TableKind, op.Columns, 8)
			if err == nil {
				res.Success = true
			} else {
				res.ErrText = err.Error()
			}

		case OpDropIndex:
			err := se.DropIndex(op.TxnID, op.Name)
			if err == nil {
				res.Success = true
			} else {
				res.ErrText = err.Error()
			}

		default:
			require.FailNow(t, "unknown op type")
		}

		model.apply(op, res)

		if i%25 == 0 {
			t.Logf("validate invariants at step=%d", i)
			compareModelWithEngineFS(t, baseDir, se, model)
		}
	}

	compareModelWithEngineFS(t, baseDir, se, model)

	t.Logf("fuzz ok: seed=%d, ops=%d, lastTxn=%d", seed, opsCount, nextTxn-1)
}
