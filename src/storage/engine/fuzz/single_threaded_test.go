package fuzz

import (
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Blackdeer1524/GraphDB/src/storage/engine"
	"github.com/Blackdeer1524/GraphDB/src/storage/systemcatalog"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func applyOp(se *engine.StorageEngine, op Operation, model *engineSimulator) OpResult {
	res := OpResult{Op: op}
	var err error

	switch op.Type {
	case OpCreateVertexTable:
		err = se.CreateVertexTable(op.TxnID, op.Name, model.VertexTables[op.Name])
	case OpDropVertexTable:
		err = se.DropVertexTable(op.TxnID, op.Name)
	case OpCreateEdgeTable:
		err = se.CreateEdgesTable(op.TxnID, op.Name, model.EdgeTables[op.Name])
	case OpDropEdgeTable:
		err = se.DropEdgesTable(op.TxnID, op.Name)
	case OpCreateIndex:
		err = se.CreateIndex(op.TxnID, op.Name, op.Table, op.TableKind, op.Columns, 8)
	case OpDropIndex:
		err = se.DropIndex(op.TxnID, op.Name)
	default:
		panic("unknown op type")
	}

	if err == nil {
		res.Success = true
	} else {
		res.ErrText = err.Error()
	}

	return res
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

	model := newEngineSimulator()

	const opsCount = 500

	operations := NewOpsGenerator(r, opsCount).Gen()

	i := 0

	for op := range operations {
		res := applyOp(se, op, model)

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

func TestFuzz_MultiThreaded(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed=%d", seed)
	//r := rand.New(rand.NewSource(seed))

	baseDir := t.TempDir()

	err := systemcatalog.InitSystemCatalog(baseDir, afero.NewOsFs())
	require.NoError(t, err)

	lockMgr := newMockRWMutexLockManager()
	se, err := engine.New(baseDir, uint64(200), afero.NewOsFs(), lockMgr)
	require.NoError(t, err)

	model := newEngineSimulator()

	const numThreads = 10
	const opsPerThread = 50

	type AppliedOp struct {
		op        Operation
		res       OpResult
		completed time.Time
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var applied []AppliedOp

	for thread := 0; thread < numThreads; thread++ {
		wg.Add(1)
		go func(thread int) {
			defer wg.Done()
			rThread := rand.New(rand.NewSource(seed + int64(thread)))
			operations := NewOpsGenerator(rThread, opsPerThread).Gen()

			for op := range operations {
				res := applyOp(se, op, model)
				mu.Lock()
				applied = append(applied, AppliedOp{op: op, res: res, completed: time.Now()})
				mu.Unlock()
			}
		}(thread)
	}

	wg.Wait()

	sort.Slice(applied, func(i, j int) bool {
		return applied[i].completed.Before(applied[j].completed)
	})

	for _, a := range applied {
		model.apply(a.op, a.res)
	}

	model.compareWithEngineFS(t, baseDir, se)

	t.Logf("fuzz ok: seed=%d, threads=%d, ops=%d", seed, numThreads, numThreads*opsPerThread)
}
