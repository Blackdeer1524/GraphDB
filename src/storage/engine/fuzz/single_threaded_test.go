package fuzz

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage/systemcatalog"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

// applyOp is a convenient wrapper to apply an operation to the
// It uses the model as a schema provider for create operations.
func applyOp(
	se *StorageEngine,
	op Operation,
	baseDir string,
	logger common.ITxnLoggerWithContext,
) OpResult {
	res := OpResult{Op: op}
	var err error

	switch op.Type {
	case OpCreateVertexTable:
		err = se.CreateVertexTable(op.TxnID, op.Name, nil, logger)
	case OpDropVertexTable:
		err = se.DropVertexTable(op.TxnID, op.Name, logger)
		if err == nil {
			filePath := GetVertexTableFilePath(baseDir, op.Name)
			errRemove := os.Remove(filePath)
			if errRemove != nil {
				err = fmt.Errorf("failed to remove vertex table file: %w", errRemove)
			}
		}
	case OpCreateEdgeTable:
		err = se.CreateEdgesTable(op.TxnID, op.Name, nil, logger)
	case OpDropEdgeTable:
		err = se.DropEdgesTable(op.TxnID, op.Name, logger)
		if err == nil {
			filePath := GetEdgeTableFilePath(baseDir, op.Name)
			errRemove := os.Remove(filePath)
			if errRemove != nil {
				err = fmt.Errorf("failed to remove edge table file: %w", errRemove)
			}
		}
	case OpCreateIndex:
		err = se.CreateIndex(
			op.TxnID,
			op.Name,
			op.Table,
			op.TableKind,
			op.Columns,
			8,
			logger,
		)
	case OpDropIndex:
		err = se.DropIndex(op.TxnID, op.Name, logger)
		if err == nil {
			filePath := GetIndexFilePath(baseDir, op.Name)
			errRemove := os.Remove(filePath)
			if errRemove != nil {
				err = fmt.Errorf("failed to remove index file: %w", errRemove)
			}
		}
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

	lockMgr := txns.NewLockManager()
	se, err := New(baseDir, uint64(200), lockMgr, afero.NewOsFs())
	require.NoError(t, err)

	model := newEngineSimulator()

	const opsCount = 500

	operations := NewOpsGenerator(r, opsCount).Gen()

	i := 0
	for op := range operations {
		res := applyOp(se, op, baseDir, common.NoLogs())

		model.apply(op, res)
		lockMgr.Unlock(op.TxnID)

		if i%25 == 0 {
			t.Logf("validate invariants at step=%d", i)
			model.compareWithEngineFS(t, baseDir, se, common.NoLogs())
		}

		i += 1
	}

	model.compareWithEngineFS(t, baseDir, se, common.NoLogs())

	t.Logf("fuzz ok: seed=%d, ops=%d", seed, opsCount)
}

func TestFuzz_MultiThreaded(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed=%d", seed)
	r := rand.New(rand.NewSource(seed))

	baseDir := t.TempDir()

	err := systemcatalog.InitSystemCatalog(baseDir, afero.NewOsFs())
	require.NoError(t, err)

	lockMgr := txns.NewLockManager()
	se, err := New(baseDir, uint64(200), lockMgr, afero.NewOsFs())
	require.NoError(t, err)

	model := newEngineSimulator()

	const numThreads = 20
	const opsPerThread = 50
	const totalOps = numThreads * opsPerThread

	operations := NewOpsGenerator(r, totalOps).Gen()

	type AppliedOp struct {
		op       Operation
		res      OpResult
		sequence int64
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var applied []AppliedOp

	var sequence int64

	wg.Add(numThreads)

	for thread := 0; thread < numThreads; thread++ {
		go func() {
			defer wg.Done()

			for {
				op, ok := <-operations
				if !ok {
					return
				}

				res := applyOp(se, op, baseDir, common.NoLogs())

				lockMgr.Unlock(op.TxnID)
				if res.Success {
					mu.Lock()
					sequence++
					applied = append(applied, AppliedOp{
						op:       op,
						res:      res,
						sequence: sequence,
					})
					mu.Unlock()
				}
			}
		}()
	}

	wg.Wait()

	sort.Slice(applied, func(i, j int) bool {
		return applied[i].sequence < applied[j].sequence
	})

	for _, a := range applied {
		model.apply(a.op, a.res)
	}

	model.compareWithEngineFS(t, baseDir, se, common.NoLogs())

	t.Logf("fuzz ok: seed=%d, threads=%d, ops=%d", seed, numThreads, totalOps)
}
