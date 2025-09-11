package query

import (
	"errors"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	myassert "github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
	"github.com/Blackdeer1524/GraphDB/src/recovery"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/storage/disk"
	"github.com/Blackdeer1524/GraphDB/src/storage/engine"
	"github.com/Blackdeer1524/GraphDB/src/storage/index"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
	"github.com/Blackdeer1524/GraphDB/src/storage/systemcatalog"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

func setupExecutor(
	poolPageCount uint64,
) (*Executor, *bufferpool.DebugBufferPool, common.ITxnLogger, error) {
	catalogBasePath := "/tmp/graphdb_test"
	fs := afero.NewMemMapFs()
	err := systemcatalog.InitSystemCatalog(catalogBasePath, fs)
	if err != nil {
		return nil, nil, nil, err
	}

	err = systemcatalog.CreateLogFileIfDoesntExist(catalogBasePath, fs)
	if err != nil {
		return nil, nil, nil, err
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
		return nil, nil, nil, err
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
	return executor, debugPool, logger, nil
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
			myassert.NoError(ctxLogger.AppendAbort())
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
	e, pool, logger, err := setupExecutor(10)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

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
	e, pool, logger, err := setupExecutor(10)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

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
			vIDs, err := e.InsertVertices(txnID, tableName, []map[string]any{data}, logger)
			require.NoError(t, err)

			v, err := e.SelectVertex(txnID, tableName, vIDs[0], logger)
			require.NoError(t, err)
			require.Equal(t, v.Data["money"], int64(100))
			return nil
		},
	)

	require.NoError(t, err)
}

func TestVertexTableInserts(t *testing.T) {
	e, pool, logger, err := setupExecutor(10)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

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

			vRecords := make([]map[string]any, N)
			for i := range N {
				vRecords[i] = map[string]any{
					"money": int64(i),
				}
			}

			vIDs, err := e.InsertVertices(txnID, tableName, vRecords, logger)
			require.NoError(t, err)

			for i, vID := range vIDs {
				vertices[vID] = int64(i)
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
	e, pool, logger, err := setupExecutor(10)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

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
	e, pool, logger, err := setupExecutor(10)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

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
			vRecords := make([]map[string]any, N)
			for i := range N {
				vRecords[i] = map[string]any{
					"money": int64(i),
				}
			}

			vIDs, err := e.InsertVertices(txnID, tableName, vRecords, logger)
			require.NoError(t, err)

			for i, vID := range vIDs {
				vertices[vID] = int64(i)
			}

			for vID, val := range vertices {
				v, err := e.SelectVertex(txnID, tableName, vID, logger)
				require.NoError(t, err)
				require.Equal(t, v.Data["money"], val)
			}
			return ErrRollback
		},
	)
	require.NoError(t, err)

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
	e, pool, logger, err := setupExecutor(10)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

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

			vRecords := make([]map[string]any, N)
			for i := range N {
				vRecords[i] = map[string]any{
					"money": int64(i) + 42,
				}
			}
			vIDs, err := e.InsertVertices(txnID, tableName, vRecords, logger)
			require.NoError(t, err)

			for i, vID := range vIDs {
				vertices[vID] = int64(i) + 42
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
	e, pool, logger, err := setupExecutor(10)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	vertTableName := "person"
	edgeTableName := "indepted_to"
	ticker := atomic.Uint64{}

	var v1 storage.VertexSystemID
	var v2 storage.VertexSystemID
	var edgeID storage.EdgeSystemID
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

			v1Record := map[string]any{
				"money": int64(100),
			}
			v1, err = e.InsertVertex(txnID, vertTableName, v1Record, logger)
			require.NoError(t, err)

			v2Record := map[string]any{
				"money": int64(200),
			}
			v2, err = e.InsertVertex(txnID, vertTableName, v2Record, logger)
			require.NoError(t, err)

			edgeRecord := EdgeInfo{
				SrcVertexID: v1,
				DstVertexID: v2,
				Data: map[string]any{
					"debt_amount": int64(40),
				},
			}
			edgeID, err = e.InsertEdge(txnID, edgeTableName, edgeRecord, logger)
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
			edge, err := e.SelectEdge(txnID, edgeTableName, edgeID, logger)
			require.NoError(t, err)
			require.Equal(t, edge.Data["debt_amount"], int64(40))
			return nil
		},
	)
	require.NoError(t, err)

	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			res, err := e.GetVertexesOnDepth(txnID, vertTableName, v1, 1, logger)
			require.NoError(t, err)
			require.Equal(t, len(res), 1)
			require.Equal(t, res[0].V, v2)
			return nil
		},
	)
	require.NoError(t, err)
}

func setupTables(
	t *testing.T,
	e *Executor,
	ticker *atomic.Uint64,
	vertTableName string,
	vertFieldName string,
	edgeFieldName string,
	edgeTableName string,
	logger common.ITxnLogger,
) {
	err := Execute(
		ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			schema := storage.Schema{
				{Name: vertFieldName, Type: storage.ColumnTypeInt64},
			}
			err = e.CreateVertexType(txnID, vertTableName, schema, logger)
			require.NoError(t, err)

			edgeSchema := storage.Schema{
				{Name: edgeFieldName, Type: storage.ColumnTypeInt64},
			}
			err = e.CreateEdgeType(
				txnID,
				edgeTableName,
				edgeSchema,
				vertTableName,
				vertTableName,
				logger,
			)
			require.NoError(t, err)
			return nil
		},
	)
	require.NoError(t, err)
}

func TestVertexAndEdgeTableDrop(t *testing.T) {
	e, pool, logger, err := setupExecutor(10)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	vertTableName := "person"
	edgeTableName := "indepted_to"
	vertFieldName := "money"
	edgeFieldName := "debt_amount"
	ticker := atomic.Uint64{}
	setupTables(t, e, &ticker, vertTableName, vertFieldName, edgeFieldName, edgeTableName, logger)

	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			err = e.DropVertexTable(txnID, vertTableName, logger)
			require.NoError(t, err)

			err = e.DropEdgeTable(txnID, edgeTableName, logger)
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
			err = e.DropVertexTable(txnID, vertTableName, logger)
			require.NoError(t, err)

			err = e.DropEdgeTable(txnID, edgeTableName, logger)
			require.NoError(t, err)
			return nil
		},
	)
	require.NoError(t, err)
}

func TestSnowflakeNeighbours(t *testing.T) {
	e, pool, logger, err := setupExecutor(10)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	vertTableName := "person"
	edgeTableName := "indepted_to"
	vertFieldName := "money"
	edgeFieldName := "debt_amount"
	ticker := atomic.Uint64{}
	setupTables(t, e, &ticker, vertTableName, vertFieldName, edgeFieldName, edgeTableName, logger)

	N := 1000
	var vCenterID storage.VertexSystemID
	neighbors := make([]storage.VertexSystemID, 0, N)
	edgeIDs := make([]storage.EdgeSystemID, 0, N)
	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			centerData := map[string]any{
				vertFieldName: int64(33),
			}
			vCenterID, err = e.InsertVertex(
				txnID,
				vertTableName,
				centerData,
				logger,
			)
			require.NoError(t, err)

			neighborRecords := make([]map[string]any, N)
			for i := range N {
				neighborRecords[i] = map[string]any{
					vertFieldName: int64(i) + 42,
				}
			}

			neighbors, err = e.InsertVertices(txnID, vertTableName, neighborRecords, logger)
			require.NoError(t, err)

			edgeRecords := make([]EdgeInfo, N)
			for i := range N {
				edgeRecords[i] = EdgeInfo{
					SrcVertexID: vCenterID,
					DstVertexID: neighbors[i],
					Data: map[string]any{
						edgeFieldName: int64(i) + 100,
					},
				}
			}

			edgeIDs, err = e.InsertEdges(txnID, edgeTableName, edgeRecords, logger)
			require.NoError(t, err)

			return nil
		},
	)
	require.NoError(t, err)

	t.Run("AssertEdgesInserted", func(t *testing.T) {
		err = Execute(
			&ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
				for i := range N {
					edge, err := e.SelectEdge(txnID, edgeTableName, edgeIDs[i], logger)
					require.NoError(t, err)
					require.Equal(t, edge.Data[edgeFieldName], int64(i)+100)
				}
				return nil
			},
		)
		require.NoError(t, err)
	})

	t.Run("GetNeighborsOfCenterVertex_Depth=1", func(t *testing.T) {
		err = Execute(
			&ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
				recordedNeighbors, err := e.GetVertexesOnDepth(
					txnID,
					vertTableName,
					vCenterID,
					1,
					logger,
				)
				require.NoError(t, err)
				require.Equal(t, len(recordedNeighbors), N)

				neighborsIDS := make([]storage.VertexSystemID, 0, N)
				for _, neighbor := range recordedNeighbors {
					neighborsIDS = append(neighborsIDS, neighbor.V)
				}

				require.ElementsMatch(t, neighborsIDS, neighbors)

				for _, noEdgesNeighbor := range neighborsIDS {
					ns, err := e.GetVertexesOnDepth(
						txnID,
						vertTableName,
						noEdgesNeighbor,
						1,
						logger,
					)
					require.NoError(t, err)
					require.Equal(t, len(ns), 0)
				}
				return nil
			},
		)
		require.NoError(t, err)
	})

	t.Run("GetNeighborsOfCenterVertex_Depth=2", func(t *testing.T) {
		err = Execute(
			&ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
				recordedNeighbors, err := e.GetVertexesOnDepth(
					txnID,
					vertTableName,
					vCenterID,
					2,
					logger,
				)
				require.NoError(t, err)
				require.ElementsMatch(t, recordedNeighbors, []storage.VertexSystemIDWithRID{})
				return nil
			},
		)
		require.NoError(t, err)
	})

	t.Run("GetNeighborsOfSnowflakeEdges_Depth=1", func(t *testing.T) {
		err = Execute(
			&ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
				for _, noEdgesNeighbor := range neighbors {
					ns, err := e.GetVertexesOnDepth(
						txnID,
						vertTableName,
						noEdgesNeighbor,
						1,
						logger,
					)
					require.NoError(t, err)
					require.Equal(t, len(ns), 0)
				}
				return nil
			},
		)
		require.NoError(t, err)
	})
}

func BuildGraph(
	t *testing.T,
	ticker *atomic.Uint64,
	vertTableName string,
	edgeTableName string,
	e *Executor,
	logger common.ITxnLogger,

	g map[int][]int,

	edgesFieldName string,
	edgesInfo map[utils.Pair[int, int]]int64,

	verticesFieldName string,
	verticesInfo map[int]int64,
) (map[int]storage.VertexSystemID, map[utils.Pair[int, int]]storage.EdgeSystemID) {
	intVertID2systemID := make(map[int]storage.VertexSystemID)
	edgesSystemInfo := make(map[utils.Pair[int, int]]storage.EdgeSystemID)

	err := Execute(
		ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			vRecords := make([]map[string]any, 0, len(verticesInfo))

			vIntIDs := make([]int, 0, len(verticesInfo))
			for vertIntID, val := range verticesInfo {
				record := map[string]any{
					verticesFieldName: val,
				}
				vRecords = append(vRecords, record)
				vIntIDs = append(vIntIDs, vertIntID)
			}
			vSystemIDs, err := e.InsertVertices(txnID, vertTableName, vRecords, logger)
			require.NoError(t, err)

			for i, vSystemID := range vSystemIDs {
				intVertID2systemID[vIntIDs[i]] = vSystemID
			}

			edgeRecords := make([]EdgeInfo, 0, len(edgesInfo))
			edgeInsertionOrder := make([]utils.Pair[int, int], 0, len(edgesInfo))
			for srcIntID, neighbors := range g {
				srcSystemID, srcExists := intVertID2systemID[srcIntID]
				require.True(t, srcExists)
				for _, dstIntID := range neighbors {
					dstSystemID, dstExists := intVertID2systemID[dstIntID]
					require.True(t, dstExists)

					edgeInfo, ok := edgesInfo[utils.Pair[int, int]{First: srcIntID, Second: dstIntID}]
					require.True(t, ok)

					edgeRecords = append(edgeRecords, EdgeInfo{
						SrcVertexID: srcSystemID,
						DstVertexID: dstSystemID,
						Data: map[string]any{
							edgesFieldName: edgeInfo,
						},
					})

					edgeInsertionOrder = append(
						edgeInsertionOrder,
						utils.Pair[int, int]{First: srcIntID, Second: dstIntID},
					)
				}
			}

			edgeSystemIDs, err := e.InsertEdges(txnID, edgeTableName, edgeRecords, logger)
			require.NoError(t, err)
			require.Equal(t, len(edgeSystemIDs), len(edgeRecords))
			require.Equal(t, len(edgeSystemIDs), len(edgeInsertionOrder))

			for i, pair := range edgeInsertionOrder {
				edgesSystemInfo[pair] = edgeSystemIDs[i]
			}
			return nil
		},
	)
	require.NoError(t, err)
	return intVertID2systemID, edgesSystemInfo
}

func getVerticesOnDepth(g map[int][]int, depth int) map[int][]int {
	result := make(map[int][]int)
	for startVertex := range g {
		visited := make(map[int]bool)
		currentLevel := []int{startVertex}
		visited[startVertex] = true

		for d := 0; d < depth && len(currentLevel) > 0; d++ {
			nextLevel := []int{}
			for _, vertex := range currentLevel {
				for _, neighbor := range g[vertex] {
					if !visited[neighbor] {
						visited[neighbor] = true
						nextLevel = append(nextLevel, neighbor)
					}
				}
			}
			currentLevel = nextLevel
		}

		result[startVertex] = currentLevel
	}

	return result
}

type GraphInfo struct {
	g            map[int][]int
	verticesInfo map[int]int64
	edgesInfo    map[utils.Pair[int, int]]int64
}

func assertDBGraph(
	t *testing.T,
	ticker *atomic.Uint64,
	e *Executor,
	logger common.ITxnLogger,
	graphInfo GraphInfo,
	vertTableName string,
	verticesFieldName string,
	edgesFieldName string,
	edgeTableName string,
	intToVertSystemID map[int]storage.VertexSystemID,
	edgesSystemInfo map[utils.Pair[int, int]]storage.EdgeSystemID,
	maxDepthAssertion int,
) {
	t.Run("AssertVerticesInserted", func(t *testing.T) {
		err := Execute(
			ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
				for vertIntID, vertSystemID := range intToVertSystemID {
					vert, err := e.SelectVertex(txnID, vertTableName, vertSystemID, logger)
					require.NoError(t, err)
					require.Equal(
						t,
						vert.Data[verticesFieldName],
						graphInfo.verticesInfo[vertIntID],
					)
				}
				return nil
			},
		)
		require.NoError(t, err)
	})

	t.Run("AssertEdgesInserted", func(t *testing.T) {
		err := Execute(
			ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
				for srcIntID, neighbors := range graphInfo.g {
					for _, nIntID := range neighbors {
						edgeSystemID, ok := edgesSystemInfo[utils.Pair[int, int]{First: srcIntID, Second: nIntID}]
						require.True(t, ok)

						edge, err := e.SelectEdge(
							txnID,
							edgeTableName,
							edgeSystemID,
							logger,
						)
						require.NoError(t, err)
						require.Equal(
							t,
							edge.Data[edgesFieldName],
							graphInfo.edgesInfo[utils.Pair[int, int]{First: srcIntID, Second: nIntID}],
						)
					}
				}
				return nil
			},
		)
		require.NoError(t, err)
	})

	t.Run("AssertVerticesOnDepth", func(t *testing.T) {
		err := Execute(
			ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
				for depth := 1; depth <= maxDepthAssertion; depth++ {
					for startIntID, depthNeighbours := range getVerticesOnDepth(graphInfo.g, depth) {
						startSystemID := intToVertSystemID[startIntID]
						expectedNeighborIDS := make(
							[]storage.VertexSystemID,
							0,
							len(depthNeighbours),
						)

						for _, nIntID := range depthNeighbours {
							nSystemID, ok := intToVertSystemID[nIntID]
							require.True(t, ok)

							expectedNeighborIDS = append(expectedNeighborIDS, nSystemID)
						}
						neighboursIDWithRID, err := e.GetVertexesOnDepth(
							txnID,
							vertTableName,
							startSystemID,
							uint32(depth),
							logger,
						)
						require.NoError(t, err)

						actualNeighbours := make([]storage.VertexSystemID, 0)
						for _, storedNeighbour := range neighboursIDWithRID {
							actualNeighbours = append(actualNeighbours, storedNeighbour.V)
						}
						require.ElementsMatch(t, expectedNeighborIDS, actualNeighbours)
					}
				}
				return nil
			},
		)
		require.NoError(t, err)
	})
}

func TestBuildGraph(t *testing.T) {
	e, pool, logger, err := setupExecutor(10)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	ticker := atomic.Uint64{}

	vertTableName := "person"
	edgeTableName := "indepted_to"

	verticesFieldName := "money"
	edgesFieldName := "debt_amount"

	tests := []struct {
		graphInfo GraphInfo
		name      string
	}{
		{
			name: "simple",
			graphInfo: GraphInfo{
				g: map[int][]int{
					1: {2, 3},
					2: {4, 5},
				},
				verticesInfo: map[int]int64{
					1: 100,
					2: 200,
					3: 300,
					4: 400,
					5: 500,
				},
				edgesInfo: map[utils.Pair[int, int]]int64{
					{First: 1, Second: 2}: 100,
					{First: 1, Second: 3}: 200,
					{First: 2, Second: 4}: 300,
					{First: 2, Second: 5}: 400,
				},
			},
		},
		{
			name: "medium",
			graphInfo: GraphInfo{
				g: map[int][]int{
					2:  {3, 4},
					3:  {5, 6},
					4:  {3},
					6:  {7},
					7:  {8},
					9:  {10},
					10: {2},
				},
				verticesInfo: map[int]int64{
					2:  100,
					3:  200,
					4:  300,
					5:  400,
					6:  500,
					7:  600,
					8:  700,
					9:  800,
					10: 900,
					11: 1000,
				},
				edgesInfo: map[utils.Pair[int, int]]int64{
					{First: 2, Second: 3}:  100,
					{First: 2, Second: 4}:  200,
					{First: 3, Second: 5}:  300,
					{First: 3, Second: 6}:  400,
					{First: 4, Second: 3}:  500,
					{First: 6, Second: 7}:  600,
					{First: 7, Second: 8}:  700,
					{First: 9, Second: 10}: 800,
					{First: 10, Second: 2}: 900,
				},
			},
		},
	}

	for _, test := range tests {
		graphInfo := test.graphInfo
		t.Run(test.name, func(t *testing.T) {
			setupTables(
				t,
				e,
				&ticker,
				vertTableName,
				verticesFieldName,
				edgesFieldName,
				edgeTableName,
				logger,
			)

			intToVertSystemID, edgesSystemInfo := BuildGraph(
				t,
				&ticker,
				vertTableName,
				edgeTableName,
				e,
				logger,
				graphInfo.g,
				edgesFieldName,
				graphInfo.edgesInfo,
				verticesFieldName,
				graphInfo.verticesInfo,
			)

			assert.Equal(t, len(graphInfo.verticesInfo), len(intToVertSystemID))
			assert.Equal(t, len(graphInfo.edgesInfo), len(edgesSystemInfo))

			assertDBGraph(
				t,
				&ticker,
				e,
				logger,
				graphInfo,
				vertTableName,
				verticesFieldName,
				edgesFieldName,
				edgeTableName,
				intToVertSystemID,
				edgesSystemInfo,
				5,
			)

			err := Execute(
				&ticker,
				e,
				logger,
				func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
					err = e.DropVertexTable(txnID, vertTableName, logger)
					require.NoError(t, err)

					err = e.DropEdgeTable(txnID, edgeTableName, logger)
					require.NoError(t, err)
					return nil
				},
			)
			require.NoError(t, err)
		})
	}
}

func generateRandomGraph(n int, connectivity float32, r *rand.Rand) GraphInfo {
	myassert.Assert(connectivity >= 0.0 && connectivity <= 1.0)

	graphInfo := GraphInfo{
		g:            make(map[int][]int),
		verticesInfo: make(map[int]int64),
		edgesInfo:    make(map[utils.Pair[int, int]]int64),
	}

	for i := 0; i < n; i++ {
		graphInfo.verticesInfo[i] = r.Int63()
		graphInfo.g[i] = []int{}
	}

	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if r.Float32() <= connectivity {
				graphInfo.g[i] = append(graphInfo.g[i], j)
				edgePair := utils.Pair[int, int]{First: i, Second: j}
				graphInfo.edgesInfo[edgePair] = r.Int63()
			}
		}
	}

	return graphInfo
}

func TestRandomizedBuildGraph(t *testing.T) {
	e, pool, logger, err := setupExecutor(100)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	ticker := atomic.Uint64{}

	vertTableName := "person"
	edgeTableName := "indepted_to"

	verticesFieldName := "money"
	edgesFieldName := "debt_amount"

	nTries := 1

	tests := []struct {
		vertexCount  int
		connectivity float32
	}{
		{
			vertexCount:  10,
			connectivity: 0.3,
		},
		{
			vertexCount:  10,
			connectivity: 0.5,
		},
		{
			vertexCount:  50,
			connectivity: 1.0,
		},
		{
			vertexCount:  100,
			connectivity: 0.3,
		},
		{
			vertexCount:  100,
			connectivity: 0.5,
		},
	}

	r := rand.New(rand.NewSource(42))

	for _, test := range tests {
		for range nTries {
			graphInfo := generateRandomGraph(test.vertexCount, test.connectivity, r)
			t.Run(
				fmt.Sprintf("vertexCount=%d,connectivity=%f", test.vertexCount, test.connectivity),
				func(t *testing.T) {
					setupTables(
						t,
						e,
						&ticker,
						vertTableName,
						verticesFieldName,
						edgesFieldName,
						edgeTableName,
						logger,
					)

					intToVertSystemID, edgesSystemInfo := BuildGraph(
						t,
						&ticker,
						vertTableName,
						edgeTableName,
						e,
						logger,
						graphInfo.g,
						edgesFieldName,
						graphInfo.edgesInfo,
						verticesFieldName,
						graphInfo.verticesInfo,
					)

					assert.Equal(t, len(graphInfo.verticesInfo), len(intToVertSystemID))
					assert.Equal(t, len(graphInfo.edgesInfo), len(edgesSystemInfo))

					assertDBGraph(
						t,
						&ticker,
						e,
						logger,
						graphInfo,
						vertTableName,
						verticesFieldName,
						edgesFieldName,
						edgeTableName,
						intToVertSystemID,
						edgesSystemInfo,
						3,
					)

					err := Execute(
						&ticker,
						e,
						logger,
						func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
							err = e.DropVertexTable(txnID, vertTableName, logger)
							require.NoError(t, err)

							err = e.DropEdgeTable(txnID, edgeTableName, logger)
							require.NoError(t, err)
							return nil
						},
					)
					require.NoError(t, err)
				},
			)
		}
	}
}

func TestBigRandomGraph(t *testing.T) {
	e, pool, logger, err := setupExecutor(100)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	ticker := atomic.Uint64{}

	vertTableName := "person"
	edgeTableName := "indepted_to"

	verticesFieldName := "money"
	edgesFieldName := "debt_amount"

	graphInfo := generateRandomGraph(10_000, 0.0005, rand.New(rand.NewSource(42)))

	setupTables(
		t,
		e,
		&ticker,
		vertTableName,
		verticesFieldName,
		edgesFieldName,
		edgeTableName,
		logger,
	)

	intToVertSystemID, edgesSystemInfo := BuildGraph(
		t,
		&ticker,
		vertTableName,
		edgeTableName,
		e,
		logger,
		graphInfo.g,
		edgesFieldName,
		graphInfo.edgesInfo,
		verticesFieldName,
		graphInfo.verticesInfo,
	)

	assertDBGraph(
		t,
		&ticker,
		e,
		logger,
		graphInfo,
		vertTableName,
		verticesFieldName,
		edgesFieldName,
		edgeTableName,
		intToVertSystemID,
		edgesSystemInfo,
		1,
	)
}

func TestNeighboursMultipleTables(t *testing.T) {
	e, pool, logger, err := setupExecutor(100)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	ticker := atomic.Uint64{}

	firstVTableName := "person"
	secondVTableName := "workplace"
	edgeTableName := "employs"

	firstVFieldName := "some"
	secondVFieldName := "another"
	edgesFieldName := "salary"

	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			firstVTableShema := storage.Schema{
				{Name: firstVFieldName, Type: storage.ColumnTypeInt64},
			}
			err = e.CreateVertexType(txnID, firstVTableName, firstVTableShema, logger)
			require.NoError(t, err)

			secondVTableShema := storage.Schema{
				{Name: secondVFieldName, Type: storage.ColumnTypeInt64},
			}
			err = e.CreateVertexType(txnID, secondVTableName, secondVTableShema, logger)
			require.NoError(t, err)

			edgeTableShema := storage.Schema{
				{Name: edgesFieldName, Type: storage.ColumnTypeInt64},
			}
			err = e.CreateEdgeType(
				txnID,
				edgeTableName,
				edgeTableShema,
				firstVTableName,
				secondVTableName,
				logger,
			)

			require.NoError(t, err)
			return nil
		},
	)
	require.NoError(t, err)

	var firstVID storage.VertexSystemID
	var secondVID storage.VertexSystemID
	var edgeID storage.EdgeSystemID
	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			firstVRecord := map[string]any{
				firstVFieldName: int64(1),
			}
			firstVID, err = e.InsertVertex(txnID, firstVTableName, firstVRecord, logger)
			require.NoError(t, err)
			secondVRecord := map[string]any{
				secondVFieldName: int64(1),
			}
			secondVID, err = e.InsertVertex(txnID, secondVTableName, secondVRecord, logger)
			require.NoError(t, err)
			edgeRecord := EdgeInfo{
				SrcVertexID: firstVID,
				DstVertexID: secondVID,
				Data: map[string]any{
					edgesFieldName: int64(1),
				},
			}
			edgeID, err = e.InsertEdge(txnID, edgeTableName, edgeRecord, logger)
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
			edge, err := e.SelectEdge(txnID, edgeTableName, edgeID, logger)
			require.NoError(t, err)
			require.Equal(t, edge.Data[edgesFieldName], int64(1))

			neighbors, err := e.GetVertexesOnDepth(txnID, firstVTableName, firstVID, 1, logger)
			require.NoError(t, err)
			require.Equal(t, len(neighbors), 1)
			require.Equal(t, neighbors[0].V, secondVID)

			neighbors, err = e.GetVertexesOnDepth(txnID, secondVTableName, secondVID, 1, logger)
			require.NoError(t, err)
			require.Equal(t, len(neighbors), 0)
			return nil
		},
	)
	require.NoError(t, err)
}
