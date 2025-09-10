package query

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/recovery"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/storage/disk"
	"github.com/Blackdeer1524/GraphDB/src/storage/engine"
	"github.com/Blackdeer1524/GraphDB/src/storage/index"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
	"github.com/Blackdeer1524/GraphDB/src/storage/systemcatalog"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

func setupExecutor(poolPageCount uint64) (*Executor, common.ITxnLogger, error) {
	catalogBasePath := "/tmp/graphdb_test"
	fs := afero.NewMemMapFs()
	err := systemcatalog.InitSystemCatalog(catalogBasePath, fs)
	if err != nil {
		return nil, nil, err
	}

	err = systemcatalog.CreateLogFileIfDoesntExist(catalogBasePath, fs)
	if err != nil {
		return nil, nil, err
	}

	versionFilePath := systemcatalog.GetSystemCatalogVersionFilePath(catalogBasePath)
	logFilePath := systemcatalog.GetLogFilePath(catalogBasePath)
	diskMgr := disk.New(
		func(fileID common.FileID, pageID common.PageID) *page.SlottedPage {
			return page.NewSlottedPage()
		},
		fs,
	)
	diskMgr.InsertToFileMap(systemcatalog.CatalogVersionFileID, versionFilePath)
	diskMgr.InsertToFileMap(systemcatalog.LogFileID, logFilePath)

	pool := bufferpool.New(poolPageCount, bufferpool.NewLRUReplacer(), diskMgr)
	logger := recovery.NewTxnLogger(pool, systemcatalog.LogFileID)

	debugPool := bufferpool.NewDebugBufferPool(pool)
	sysCat, err := systemcatalog.New(catalogBasePath, fs, debugPool)
	if err != nil {
		return nil, nil, err
	}

	debugPool.MarkPageAsLeaking(systemcatalog.CatalogVersionPageIdent())
	debugPool.MarkPageAsLeaking(recovery.GetMasterPageIdent(systemcatalog.LogFileID))

	diskMgr.UpdateFileMap(sysCat.GetFileIDToPathMap())

	locker := txns.NewLockManager()
	indexLoader := func(
		indexMeta storage.IndexMeta,
		pool bufferpool.BufferPool,
		locker *txns.LockManager,
		logger common.ITxnLoggerWithContext,
	) (storage.Index, error) {
		return index.NewLinearProbingIndex(indexMeta, pool, locker, logger)
	}

	se := engine.New(
		sysCat,
		debugPool,
		diskMgr.InsertToFileMap,
		diskMgr.GetLastFilePage,
		diskMgr.GetEmptyPage,
		locker,
		fs,
		indexLoader,
	)
	executor := New(se, locker)
	return executor, logger, nil
}

var ErrRollback = errors.New("rollback")

func Execute(
	ticker *atomic.Uint64,
	executor *Executor,
	logger common.ITxnLogger,
	fn Task,
) (err error) {
	txnID := common.TxnID(ticker.Add(1))
	defer executor.locker.Unlock(txnID)
	ctxLogger := logger.WithContext(txnID)

	if err := ctxLogger.AppendBegin(); err != nil {
		return fmt.Errorf("failed to append begin: %w", err)
	}

	defer func() {
		if err != nil {
			assert.NoError(ctxLogger.AppendAbort())
			ctxLogger.Rollback()
			if err == ErrRollback {
				err = nil
				return
			}
			return
		}
		if err = ctxLogger.AppendCommit(); err != nil {
			err = fmt.Errorf("failed to append commit: %w", err)
		} else if err = ctxLogger.AppendTxnEnd(); err != nil {
			err = fmt.Errorf("failed to append txn end: %w", err)
		}
	}()

	return fn(txnID, executor, ctxLogger)
}

func TestCreateVertexType(t *testing.T) {
	e, logger, err := setupExecutor(10)
	require.NoError(t, err)

	ticker := atomic.Uint64{}
	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			tableName := "test"
			schema := storage.Schema{
				{Name: "money", Type: storage.ColumnTypeInt64},
			}
			err = e.CreateVertexType(txnID, tableName, schema, logger)
			require.NoError(t, err)
			return nil
		},
	)

	require.NoError(t, err)
}

func TestCreateVertexSimpleInsert(t *testing.T) {
	e, logger, err := setupExecutor(10)
	require.NoError(t, err)

	ticker := atomic.Uint64{}

	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			tableName := "test"
			schema := storage.Schema{
				{Name: "money", Type: storage.ColumnTypeInt64},
			}
			err = e.CreateVertexType(txnID, tableName, schema, logger)
			require.NoError(t, err)

			data := map[string]any{
				"money": int64(100),
			}
			vID, err := e.InsertVertex(txnID, tableName, data, logger)
			require.NoError(t, err)

			v, err := e.SelectVertex(txnID, tableName, vID, logger)
			require.NoError(t, err)
			require.Equal(t, v.Data["money"], int64(100))
			return nil
		},
	)

	require.NoError(t, err)
}

func TestVertexTableInserts(t *testing.T) {
	e, logger, err := setupExecutor(10)
	require.NoError(t, err)

	tableName := "test"
	ticker := atomic.Uint64{}
	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			schema := storage.Schema{
				{Name: "money", Type: storage.ColumnTypeInt64},
			}
			err = e.CreateVertexType(txnID, tableName, schema, logger)
			require.NoError(t, err)
			return nil
		},
	)
	require.NoError(t, err)

	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			N := 1000
			vertices := make(map[storage.VertexSystemID]int64, N)
			for i := range N {
				val := int64(i)
				data := map[string]any{
					"money": val,
				}
				vID, err := e.InsertVertex(txnID, tableName, data, logger)
				require.NoError(t, err)

				vertices[vID] = val
			}

			for vID, val := range vertices {
				v, err := e.SelectVertex(txnID, tableName, vID, logger)
				require.NoError(t, err)
				require.Equal(t, v.Data["money"], val)
			}
			return nil
		},
	)

	require.NoError(t, err)
}

func TestCreateVertexRollback(t *testing.T) {
	e, logger, err := setupExecutor(10)
	require.NoError(t, err)

	ticker := atomic.Uint64{}
	tableName := "test"
	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			schema := storage.Schema{
				{Name: "money", Type: storage.ColumnTypeInt64},
			}
			err = e.CreateVertexType(txnID, tableName, schema, logger)
			require.NoError(t, err)
			return ErrRollback
		},
	)
	require.NoError(t, err)

	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			schema := storage.Schema{
				{Name: "money2", Type: storage.ColumnTypeInt64},
			}
			err = e.CreateVertexType(txnID, tableName, schema, logger)
			require.NoError(t, err)
			return nil
		},
	)
	require.NoError(t, err)
}

func TestVertexTableInsertsRollback(t *testing.T) {
	e, logger, err := setupExecutor(10)
	require.NoError(t, err)

	tableName := "test"
	ticker := atomic.Uint64{}
	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			schema := storage.Schema{
				{Name: "money", Type: storage.ColumnTypeInt64},
			}
			err = e.CreateVertexType(txnID, tableName, schema, logger)
			require.NoError(t, err)
			return nil
		},
	)
	require.NoError(t, err)

	N := 300
	vertices := make(map[storage.VertexSystemID]int64, N)
	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			for i := range N {
				val := int64(i)
				data := map[string]any{
					"money": val,
				}
				vID, err := e.InsertVertex(txnID, tableName, data, logger)
				require.NoError(t, err)

				vertices[vID] = val
			}

			for vID, val := range vertices {
				v, err := e.SelectVertex(txnID, tableName, vID, logger)
				require.NoError(t, err)
				require.Equal(t, v.Data["money"], val)
			}
			return ErrRollback
		},
	)

	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			for vID := range vertices {
				_, err := e.SelectVertex(txnID, tableName, vID, logger)
				require.Error(t, err)
			}
			return nil
		},
	)

	require.NoError(t, err)
}

func TestDropVertexTable(t *testing.T) {
	e, logger, err := setupExecutor(10)
	require.NoError(t, err)

	tableName := "test"
	ticker := atomic.Uint64{}

	N := 1000
	vertices := make(map[storage.VertexSystemID]int64, N)

	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			schema := storage.Schema{
				{Name: "money", Type: storage.ColumnTypeInt64},
			}
			err = e.CreateVertexType(txnID, tableName, schema, logger)
			require.NoError(t, err)

			for i := range N {
				val := int64(i) + 42
				data := map[string]any{
					"money": val,
				}
				vID, err := e.InsertVertex(txnID, tableName, data, logger)
				require.NoError(t, err)
				vertices[vID] = val
			}
			return nil
		},
	)
	require.NoError(t, err)

	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			err = e.DropVertexTable(txnID, tableName, logger)
			require.NoError(t, err)
			return nil
		},
	)
	require.NoError(t, err)

	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			schema := storage.Schema{
				{Name: "money", Type: storage.ColumnTypeInt64},
			}
			err = e.CreateVertexType(txnID, tableName, schema, logger)
			require.NoError(t, err)

			for vID := range vertices {
				v, err := e.SelectVertex(txnID, tableName, vID, logger)
				require.ErrorIs(
					t,
					err,
					storage.ErrKeyNotFound,
					"vertex with ID %v should have been deleted. found: ID: %v, Data: %v",
					vID,
					v.ID,
					v.Data,
				)
			}
			return nil
		},
	)
	require.NoError(t, err)
}

func TestCreateEdgeTable(t *testing.T) {
	e, logger, err := setupExecutor(10)
	require.NoError(t, err)

	vertTableName := "person"
	edgeTableName := "indepted_to"
	ticker := atomic.Uint64{}

	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			schema := storage.Schema{
				{Name: "money", Type: storage.ColumnTypeInt64},
			}
			err = e.CreateVertexType(txnID, vertTableName, schema, logger)
			require.NoError(t, err)

			edgeSchema := storage.Schema{
				{Name: "debt_amount", Type: storage.ColumnTypeInt64},
			}
			err = e.CreateEdgeType(txnID, edgeTableName, edgeSchema, "person", "person", logger)
			require.NoError(t, err)

			v1, err := e.InsertVertex(txnID, vertTableName, map[string]any{
				"money": int64(100),
			}, logger)
			require.NoError(t, err)

			v2, err := e.InsertVertex(txnID, vertTableName, map[string]any{
				"money": int64(200),
			}, logger)
			require.NoError(t, err)

			_, err = e.InsertEdge(txnID, edgeTableName, v1, v2, map[string]any{
				"debt_amount": int64(100),
			}, logger)
			require.NoError(t, err)
			return nil
		},
	)
	require.NoError(t, err)
}
