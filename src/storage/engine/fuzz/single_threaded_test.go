package fuzz

import (
	"math/rand"
	"testing"
	"time"

	"github.com/Blackdeer1524/GraphDB/src/storage/engine"
	"github.com/Blackdeer1524/GraphDB/src/storage/systemcatalog"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

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

	model := newEngineSimulator()

	const opsCount = 600

	operations := NewOpsGenerator(r, opsCount).Gen()

	i := 0

	for op := range operations {
		res := OpResult{Op: op}

		switch op.Type {
		case OpCreateVertexTable:
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
			model.compareWithEngineFS(t, baseDir, se)
		}

		i += 1
	}

	model.compareWithEngineFS(t, baseDir, se)

	t.Logf("fuzz ok: seed=%d, ops=%d", seed, opsCount)
}
