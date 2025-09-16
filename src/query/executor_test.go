package query

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/panjf2000/ants"
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
	fs afero.Fs,
	catalogBasePath string,
	poolPageCount uint64,
	debugMode bool,
) (*Executor, *bufferpool.DebugBufferPool, *txns.LockManager, *recovery.TxnLogger, error) {
	err := systemcatalog.InitSystemCatalog(catalogBasePath, fs)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	err = systemcatalog.CreateLogFileIfDoesntExist(catalogBasePath, fs)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	diskMgr := disk.New(
		catalogBasePath,
		func(fileID common.FileID, pageID common.PageID) page.SlottedPage {
			return page.NewSlottedPage()
		},
		fs,
	)

	pool := bufferpool.New(poolPageCount, bufferpool.NewLRUReplacer(), diskMgr)
	debugPool := bufferpool.NewDebugBufferPool(pool)

	debugPool.MarkPageAsLeaking(systemcatalog.CatalogVersionPageIdent())
	debugPool.MarkPageAsLeaking(recovery.GetMasterPageIdent(systemcatalog.LogFileID))

	logger := recovery.NewTxnLogger(pool, systemcatalog.LogFileID)
	sysCat, err := systemcatalog.New(catalogBasePath, fs, debugPool)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	locker := txns.NewLockManager()
	indexLoader := func(
		indexMeta storage.IndexMeta,
		pool bufferpool.BufferPool,
		locker *txns.LockManager,
		logger common.ITxnLoggerWithContext,
	) (storage.Index, error) {
		return index.NewLinearProbingIndex(
			indexMeta,
			pool,
			locker,
			logger,
			debugMode,
			42,
		)
	}

	se := engine.New(
		sysCat,
		debugPool,
		diskMgr.GetLastFilePage,
		diskMgr.GetEmptyPage,
		locker,
		fs,
		indexLoader,
		debugMode,
	)
	executor := New(se, locker)
	return executor, debugPool, locker, logger, nil
}

var ErrRollback = errors.New("rollback")

func Execute(
	ticker *atomic.Uint64,
	executor *Executor,
	logger common.ITxnLogger,
	fn Task,
	isReadOnly bool, // не будет писать логов -> немного получше скорость
) (err error) {
	txnID := common.TxnID(ticker.Add(1))
	defer executor.Locker.Unlock(txnID)

	ctxLogger := logger.WithContext(txnID)
	if !isReadOnly {
		if err := ctxLogger.AppendBegin(); err != nil {
			return fmt.Errorf("failed to append begin: %w", err)
		}
	}

	defer func() {
		if isReadOnly {
			return
		}
		if err != nil {
			myassert.NoError(ctxLogger.AppendAbort())
			ctxLogger.Rollback()
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
	fs := afero.NewMemMapFs()
	catalogBasePath := "/tmp/graphdb_test"
	e, pool, locker, logger, err := setupExecutor(fs, catalogBasePath, 10, true)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()
	defer func() { require.True(t, locker.AreAllQueuesEmpty()) }()

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
		false,
	)

	require.NoError(t, err)
}

func TestCreateVertexSimpleInsert(t *testing.T) {
	fs := afero.NewMemMapFs()
	catalogBasePath := "/tmp/graphdb_test"
	e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, 10, true)
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

			vInfo := storage.VertexInfo{
				SystemID: storage.VertexSystemID(uuid.New()),
				Data: map[string]any{
					"money": int64(100),
				},
			}

			err = e.InsertVertices(txnID, tableName, []storage.VertexInfo{vInfo}, logger)
			require.NoError(t, err)

			v, err := e.SelectVertex(txnID, tableName, vInfo.SystemID, logger)
			require.NoError(t, err)
			require.Equal(t, v.Data["money"], int64(100))
			return nil
		},
		false,
	)

	require.NoError(t, err)
}

func TestVertexTableInserts(t *testing.T) {
	fs := afero.NewMemMapFs()
	catalogBasePath := "/tmp/graphdb_test"
	e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, 10, true)
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
		false,
	)
	require.NoError(t, err)

	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			N := 1000
			vertices := make(map[storage.VertexSystemID]int64, N)

			vRecords := make([]storage.VertexInfo, N)
			for i := range N {
				vRecords[i] = storage.VertexInfo{
					SystemID: storage.VertexSystemID(uuid.New()),
					Data: map[string]any{
						"money": int64(i),
					},
				}
			}

			err = e.InsertVertices(txnID, tableName, vRecords, logger)
			require.NoError(t, err)

			for i, vID := range vRecords {
				vertices[vID.SystemID] = int64(i)
			}

			for vID, val := range vertices {
				v, err := e.SelectVertex(txnID, tableName, vID, logger)
				require.NoError(t, err)
				require.Equal(t, v.Data["money"], val)
			}
			return nil
		},
		false,
	)

	require.NoError(t, err)
}

func TestCreateVertexRollback(t *testing.T) {
	fs := afero.NewMemMapFs()
	catalogBasePath := "/tmp/graphdb_test"
	e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, 10, true)
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
		false,
	)
	require.ErrorIs(t, err, ErrRollback)

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
		false,
	)
	require.NoError(t, err)
}

func TestVertexTableInsertsRollback(t *testing.T) {
	fs := afero.NewMemMapFs()
	catalogBasePath := "/tmp/graphdb_test"
	e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, 10, true)
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
		false,
	)
	require.NoError(t, err)

	N := 300
	vertices := make(map[storage.VertexSystemID]int64, N)
	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			vRecords := make([]storage.VertexInfo, N)
			for i := range N {
				vRecords[i] = storage.VertexInfo{
					SystemID: storage.VertexSystemID(uuid.New()),
					Data: map[string]any{
						"money": int64(i),
					},
				}
			}

			err = e.InsertVertices(txnID, tableName, vRecords, logger)
			require.NoError(t, err)

			for i, vID := range vRecords {
				vertices[vID.SystemID] = int64(i)
			}

			for vID, val := range vertices {
				v, err := e.SelectVertex(txnID, tableName, vID, logger)
				require.NoError(t, err)
				require.Equal(t, v.Data["money"], val)
			}
			return ErrRollback
		},
		false,
	)
	require.ErrorIs(t, err, ErrRollback)

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
		false,
	)

	require.NoError(t, err)
}

func TestDropVertexTable(t *testing.T) {
	fs := afero.NewMemMapFs()
	catalogBasePath := "/tmp/graphdb_test"
	e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, 10, true)
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

			vRecords := make([]storage.VertexInfo, N)
			for i := range N {
				vRecords[i] = storage.VertexInfo{
					SystemID: storage.VertexSystemID(uuid.New()),
					Data: map[string]any{
						"money": int64(i) + 42,
					},
				}
			}
			err = e.InsertVertices(txnID, tableName, vRecords, logger)
			require.NoError(t, err)

			for i, vID := range vRecords {
				vertices[vID.SystemID] = int64(i) + 42
			}
			return nil
		},
		false,
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
		false,
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
		false,
	)
	require.NoError(t, err)
}

func TestCreateEdgeTable(t *testing.T) {
	fs := afero.NewMemMapFs()
	catalogBasePath := "/tmp/graphdb_test"
	e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, 10, true)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	vertTableName := "person"
	edgeTableName := "indepted_to"
	ticker := atomic.Uint64{}

	vertSchema := storage.Schema{
		{Name: "money", Type: storage.ColumnTypeInt64},
	}
	edgeSchema := storage.Schema{
		{Name: "debt_amount", Type: storage.ColumnTypeInt64},
	}
	v1Record := storage.VertexInfo{
		SystemID: storage.VertexSystemID(uuid.New()),
		Data: map[string]any{
			"money": int64(100),
		},
	}
	v2Record := storage.VertexInfo{
		SystemID: storage.VertexSystemID(uuid.New()),
		Data: map[string]any{
			"money": int64(200),
		},
	}
	edgeRecord := storage.EdgeInfo{
		SystemID:    storage.EdgeSystemID(uuid.New()),
		SrcVertexID: v1Record.SystemID,
		DstVertexID: v2Record.SystemID,
		Data: map[string]any{
			"debt_amount": int64(40),
		},
	}

	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			err = e.CreateVertexType(txnID, vertTableName, vertSchema, logger)
			require.NoError(t, err)

			err = e.CreateEdgeType(txnID, edgeTableName, edgeSchema, "person", "person", logger)
			require.NoError(t, err)

			err = e.InsertVertex(txnID, vertTableName, v1Record, logger)
			require.NoError(t, err)

			err = e.InsertVertex(txnID, vertTableName, v2Record, logger)
			require.NoError(t, err)

			err = e.InsertEdge(txnID, edgeTableName, edgeRecord, logger)
			require.NoError(t, err)
			return nil
		},
		false,
	)
	require.NoError(t, err)

	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			edge, err := e.SelectEdge(txnID, edgeTableName, edgeRecord.SystemID, logger)
			require.NoError(t, err)
			require.Equal(t, edge.Data["debt_amount"], int64(40))
			return nil
		},
		false,
	)
	require.NoError(t, err)

	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			res, err := e.GetVerticesOnDepth(
				txnID,
				vertTableName,
				v1Record.SystemID,
				1,
				storage.AllowAllVerticesFilter,
				logger,
			)
			require.NoError(t, err)
			require.Equal(t, len(res), 1)
			require.Equal(t, res[0].V, v2Record.SystemID)
			return nil
		},
		false,
	)
	require.NoError(t, err)
}

func setupTables(
	t testing.TB,
	e *Executor,
	ticker *atomic.Uint64,
	vertTableName string,
	vertFieldName string,
	edgeTableName string,
	edgeFieldName string,
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
		false,
	)
	require.NoError(t, err)
}

func TestVertexAndEdgeTableDrop(t *testing.T) {
	fs := afero.NewMemMapFs()
	catalogBasePath := "/tmp/graphdb_test"
	e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, 10, true)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	vertTableName := "person"
	edgeTableName := "indepted_to"
	vertFieldName := "money"
	edgeFieldName := "debt_amount"
	ticker := atomic.Uint64{}
	setupTables(t, e, &ticker, vertTableName, vertFieldName, edgeTableName, edgeFieldName, logger)

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
		false,
	)
	require.ErrorIs(t, err, ErrRollback)

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
		false,
	)
	require.NoError(t, err)
}

func TestSnowflakeNeighbours(t *testing.T) {
	fs := afero.NewMemMapFs()
	catalogBasePath := "/tmp/graphdb_test"
	e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, 10, true)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	vertTableName := "person"
	edgeTableName := "indepted_to"
	vertFieldName := "money"
	edgeFieldName := "debt_amount"
	ticker := atomic.Uint64{}
	setupTables(t, e, &ticker, vertTableName, vertFieldName, edgeTableName, edgeFieldName, logger)

	N := 1000
	centerData := storage.VertexInfo{
		SystemID: storage.VertexSystemID(uuid.New()),
		Data: map[string]any{
			vertFieldName: int64(33),
		},
	}
	neighborRecords := make([]storage.VertexInfo, N)
	for i := range N {
		neighborRecords[i] = storage.VertexInfo{
			SystemID: storage.VertexSystemID(uuid.New()),
			Data: map[string]any{
				vertFieldName: int64(i) + 42,
			},
		}
	}
	edgeRecords := make([]storage.EdgeInfo, N)
	for i := range N {
		edgeRecords[i] = storage.EdgeInfo{
			SystemID:    storage.EdgeSystemID(uuid.New()),
			SrcVertexID: centerData.SystemID,
			DstVertexID: neighborRecords[i].SystemID,
			Data: map[string]any{
				edgeFieldName: int64(i) + 100,
			},
		}
	}

	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			err = e.InsertVertex(
				txnID,
				vertTableName,
				centerData,
				logger,
			)
			require.NoError(t, err)

			err = e.InsertVertices(txnID, vertTableName, neighborRecords, logger)
			require.NoError(t, err)

			err = e.InsertEdges(txnID, edgeTableName, edgeRecords, logger)
			require.NoError(t, err)

			return nil
		},
		false,
	)
	require.NoError(t, err)

	t.Run("AssertEdgesInserted", func(t *testing.T) {
		err = Execute(
			&ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
				for i := range N {
					edge, err := e.SelectEdge(txnID, edgeTableName, edgeRecords[i].SystemID, logger)
					require.NoError(t, err)
					require.Equal(t, edge.Data[edgeFieldName], int64(i)+100)
				}
				return nil
			},
			true,
		)
		require.NoError(t, err)
	})

	t.Run("GetNeighborsOfCenterVertex_Depth=1", func(t *testing.T) {
		err = Execute(
			&ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
				recordedNeighbors, err := e.GetVerticesOnDepth(
					txnID,
					vertTableName,
					centerData.SystemID,
					1,
					storage.AllowAllVerticesFilter,
					logger,
				)
				require.NoError(t, err)
				require.Equal(t, len(recordedNeighbors), N)

				actualNeighborsIDS := make([]storage.VertexSystemID, 0, N)
				for _, neighbor := range recordedNeighbors {
					actualNeighborsIDS = append(actualNeighborsIDS, neighbor.V)
				}

				expectedNeighborIDs := make([]storage.VertexSystemID, 0, N)
				for _, neighbor := range neighborRecords {
					expectedNeighborIDs = append(expectedNeighborIDs, neighbor.SystemID)
				}

				require.ElementsMatch(t, expectedNeighborIDs, actualNeighborsIDS)

				for _, noEdgesNeighbor := range actualNeighborsIDS {
					ns, err := e.GetVerticesOnDepth(
						txnID,
						vertTableName,
						noEdgesNeighbor,
						1,
						storage.AllowAllVerticesFilter,
						logger,
					)
					require.NoError(t, err)
					require.Equal(t, len(ns), 0)
				}
				return nil
			},
			true,
		)
		require.NoError(t, err)
	})

	t.Run("GetNeighborsOfCenterVertex_Depth=2", func(t *testing.T) {
		err = Execute(
			&ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
				recordedNeighbors, err := e.GetVerticesOnDepth(
					txnID,
					vertTableName,
					centerData.SystemID,
					2,
					storage.AllowAllVerticesFilter,
					logger,
				)
				require.NoError(t, err)
				require.ElementsMatch(t, recordedNeighbors, []storage.VertexSystemIDWithRID{})
				return nil
			},
			true,
		)
		require.NoError(t, err)
	})

	t.Run("GetNeighborsOfSnowflakeEdges_Depth=1", func(t *testing.T) {
		err = Execute(
			&ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
				for _, noEdgesNeighbor := range neighborRecords {
					ns, err := e.GetVerticesOnDepth(
						txnID,
						vertTableName,
						noEdgesNeighbor.SystemID,
						1,
						storage.AllowAllVerticesFilter,
						logger,
					)
					require.NoError(t, err)
					require.Equal(t, len(ns), 0)
				}
				return nil
			},
			true,
		)
		require.NoError(t, err)
	})
}

func instantiateGraph(
	t testing.TB,
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
			vRecords := make([]storage.VertexInfo, 0, len(verticesInfo))

			vIntIDs := make([]int, 0, len(verticesInfo))
			for vertIntID, val := range verticesInfo {
				record := storage.VertexInfo{
					SystemID: storage.VertexSystemID(uuid.New()),
					Data: map[string]any{
						verticesFieldName: val,
					},
				}
				vRecords = append(vRecords, record)
				vIntIDs = append(vIntIDs, vertIntID)
			}
			err = e.InsertVertices(txnID, vertTableName, vRecords, logger)
			require.NoError(t, err)

			for i, vSystemID := range vRecords {
				intVertID2systemID[vIntIDs[i]] = vSystemID.SystemID
			}

			edgeRecords := make([]storage.EdgeInfo, 0, len(edgesInfo))
			edgeInsertionOrder := make([]utils.Pair[int, int], 0, len(edgesInfo))
			for srcIntID, neighbors := range g {
				srcSystemID, srcExists := intVertID2systemID[srcIntID]
				require.True(t, srcExists)
				for _, dstIntID := range neighbors {
					dstSystemID, dstExists := intVertID2systemID[dstIntID]
					require.True(t, dstExists)

					edgeInfo, ok := edgesInfo[utils.Pair[int, int]{First: srcIntID, Second: dstIntID}]
					require.True(t, ok)

					edgeRecords = append(edgeRecords, storage.EdgeInfo{
						SystemID:    storage.EdgeSystemID(uuid.New()),
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

			err = e.InsertEdges(txnID, edgeTableName, edgeRecords, logger)
			require.NoError(t, err)
			require.Equal(t, len(edgeRecords), len(edgeRecords))
			require.Equal(t, len(edgeRecords), len(edgeInsertionOrder))

			for i, pair := range edgeInsertionOrder {
				edgesSystemInfo[pair] = edgeRecords[i].SystemID
			}
			return nil
		},
		false,
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

func (g *GraphInfo) GraphVizRepr() string {
	var result strings.Builder

	result.WriteString("digraph GraphInfo {\n")
	result.WriteString("\trankdir=LR;\n")
	result.WriteString("\tnode [shape=circle];\n")
	result.WriteString("\n")

	// Add vertices with their labels and weights
	for vertexID, weight := range g.verticesInfo {
		result.WriteString(
			fmt.Sprintf("\t\"v_%d\" [label=\"%d (w:%d)\"];\n", vertexID, vertexID, weight),
		)
	}
	result.WriteString("\n")

	// Add edges with their weights
	for fromVertex, neighbors := range g.g {
		for _, toVertex := range neighbors {
			edgePair := utils.Pair[int, int]{First: fromVertex, Second: toVertex}
			edgeWeight, hasWeight := g.edgesInfo[edgePair]

			if hasWeight {
				result.WriteString(
					fmt.Sprintf(
						"\t\"v_%d\" -> \"v_%d\" [label=\"%d\"];\n",
						fromVertex,
						toVertex,
						edgeWeight,
					),
				)
			} else {
				result.WriteString(
					fmt.Sprintf("\t\"v_%d\" -> \"v_%d\";\n", fromVertex, toVertex),
				)
			}
		}
	}

	result.WriteString("}\n")
	return result.String()
}

func assertDBGraph(
	t testing.TB,
	ticker *atomic.Uint64,
	e *Executor,
	logger common.ITxnLogger,
	graphInfo GraphInfo,
	vertTableName string,
	verticesFieldName string,
	edgeTableName string,
	edgesFieldName string,
	intToVertSystemID map[int]storage.VertexSystemID,
	edgesSystemInfo map[utils.Pair[int, int]]storage.EdgeSystemID,
	maxDepthAssertion int,
	workerPoolSize int,
) {
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
		true,
	)
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	pool, perr := ants.NewPool(workerPoolSize)
	require.NoError(t, perr)
	defer pool.Release()
	for srcIntID, neighbors := range graphInfo.g {
		for _, nIntID := range neighbors {
			esid, ok := edgesSystemInfo[utils.Pair[int, int]{First: srcIntID, Second: nIntID}]
			require.True(t, ok)
			startID := srcIntID
			targetID := nIntID
			edgeSystemID := esid
			wg.Add(1)
			job := func() {
				defer wg.Done()
				err := Execute(
					ticker,
					e,
					logger,
					func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
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
							graphInfo.edgesInfo[utils.Pair[int, int]{First: startID, Second: targetID}],
						)
						return nil
					},
					true,
				)
				require.NoError(t, err)
			}
			require.NoError(t, pool.Submit(job))
		}
	}
	wg.Wait()

	for depth := 1; depth <= maxDepthAssertion; depth++ {
		for startIntID, depthNeighbours := range getVerticesOnDepth(graphInfo.g, depth) {
			d := depth
			sID := startIntID
			ds := depthNeighbours
			wg.Add(1)
			job := func() {
				defer wg.Done()
				startSystemID := intToVertSystemID[sID]
				expectedNeighborIDS := make([]storage.VertexSystemID, 0, len(ds))

				for _, nIntID := range ds {
					nSystemID, ok := intToVertSystemID[nIntID]
					require.True(t, ok)
					expectedNeighborIDS = append(expectedNeighborIDS, nSystemID)
				}

				err := Execute(
					ticker,
					e,
					logger,
					func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
						neighboursIDWithRID, err := e.GetVerticesOnDepth(
							txnID,
							vertTableName,
							startSystemID,
							uint32(d),
							storage.AllowAllVerticesFilter,
							logger,
						)
						require.NoError(t, err)

						actualNeighbours := make([]storage.VertexSystemID, 0)
						for _, storedNeighbour := range neighboursIDWithRID {
							actualNeighbours = append(actualNeighbours, storedNeighbour.V)
						}
						require.ElementsMatch(t, expectedNeighborIDS, actualNeighbours)
						return nil
					},
					true,
				)
				require.NoError(t, err)
			}
			require.NoError(t, pool.Submit(job))
		}
	}
	wg.Wait()
}

func TestBuildGraph(t *testing.T) {
	catalogBasePath := "/tmp/graphdb_test"
	fs := afero.NewMemMapFs()
	e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, 10, true)
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
				edgeTableName,
				edgesFieldName,
				logger,
			)

			intToVertSystemID, edgesSystemInfo := instantiateGraph(
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
				edgeTableName,
				edgesFieldName,
				intToVertSystemID,
				edgesSystemInfo,
				5,
				1,
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
				false,
			)
			require.NoError(t, err)
		})
	}
}

func generateRandomGraph(
	t testing.TB,
	vertexCount int,
	connectivity float32,
	r *rand.Rand,
	bidirectional bool,
) GraphInfo {
	myassert.Assert(connectivity >= 0.0 && connectivity <= 1.0)

	graphInfo := GraphInfo{
		g:            make(map[int][]int),
		verticesInfo: make(map[int]int64),
		edgesInfo:    make(map[utils.Pair[int, int]]int64),
	}

	for i := 0; i < vertexCount; i++ {
		graphInfo.verticesInfo[i] = r.Int63() % 100
		graphInfo.g[i] = []int{}
	}

	if bidirectional {
		for i := 0; i < vertexCount; i++ {
			for j := i; j < vertexCount; j++ {
				if r.Float32() <= connectivity {
					graphInfo.g[i] = append(graphInfo.g[i], j)

					edgePair := utils.Pair[int, int]{First: i, Second: j}
					edgeWeight := r.Int63() % 100
					graphInfo.edgesInfo[edgePair] = edgeWeight

					graphInfo.g[j] = append(graphInfo.g[j], i)
					edgePair = utils.Pair[int, int]{First: j, Second: i}
					graphInfo.edgesInfo[edgePair] = edgeWeight
				}
			}
		}
	} else {
		for i := 0; i < vertexCount; i++ {
			for j := 0; j < vertexCount; j++ {
				if r.Float32() <= connectivity {
					graphInfo.g[i] = append(graphInfo.g[i], j)
					edgePair := utils.Pair[int, int]{First: i, Second: j}
					edgeWeight := r.Int63() % 100
					graphInfo.edgesInfo[edgePair] = edgeWeight
				}
			}
		}
	}

	edgeCount := len(graphInfo.edgesInfo)
	t.Logf("|V| = %d, |E| = %d", vertexCount, edgeCount)

	return graphInfo
}

func TestRandomizedBuildGraph(t *testing.T) {
	fs := afero.NewMemMapFs()
	catalogBasePath := "/tmp/graphdb_test"

	e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, 50, false)
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
		// {
		// 	vertexCount:  10,
		// 	connectivity: 0.5,
		// },
		// {
		// 	vertexCount:  25,
		// 	connectivity: 1.0,
		// },
		{
			vertexCount:  50,
			connectivity: 0.3,
		},
	}

	r := rand.New(rand.NewSource(42))

	for _, test := range tests {
		for range nTries {
			graphInfo := generateRandomGraph(t, test.vertexCount, test.connectivity, r, false)
			t.Run(
				fmt.Sprintf("vertexCount=%d,connectivity=%f", test.vertexCount, test.connectivity),
				func(t *testing.T) {
					setupTables(
						t,
						e,
						&ticker,
						vertTableName,
						verticesFieldName,
						edgeTableName,
						edgesFieldName,
						logger,
					)

					intToVertSystemID, edgesSystemInfo := instantiateGraph(
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
						edgeTableName,
						edgesFieldName,
						intToVertSystemID,
						edgesSystemInfo,
						3,
						1,
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
						false,
					)
					require.NoError(t, err)
				},
			)
		}
	}
}

func TestBigRandomGraph(t *testing.T) {
	fs := afero.NewOsFs()
	catalogBasePath := t.TempDir()
	e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, 100, false)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	ticker := atomic.Uint64{}

	vertTableName := "person"
	edgeTableName := "indepted_to"

	verticesFieldName := "money"
	edgesFieldName := "debt_amount"

	graphInfo := generateRandomGraph(t, 500, 0.01, rand.New(rand.NewSource(42)), false)

	setupTables(
		t,
		e,
		&ticker,
		vertTableName,
		verticesFieldName,
		edgeTableName,
		edgesFieldName,
		logger,
	)

	intToVertSystemID, edgesSystemInfo := instantiateGraph(
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

	t.Log("asserting a graph...")
	assertDBGraph(
		t,
		&ticker,
		e,
		logger,
		graphInfo,
		vertTableName,
		verticesFieldName,
		edgeTableName,
		edgesFieldName,
		intToVertSystemID,
		edgesSystemInfo,
		1,
		1,
	)
}

func TestNeighboursMultipleTables(t *testing.T) {
	fs := afero.NewMemMapFs()
	catalogBasePath := "/tmp/graphdb_test"
	e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, 10, true)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	ticker := atomic.Uint64{}

	personVTableName := "person"
	personFieldName := "some"

	workplaceVTableName := "workplace"
	workplaceFieldName := "aaaa"

	employsETableName := "employs"
	employsFieldName := "salary"

	friendETableName := "friend"
	friendFieldName := "how_long"

	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			personVTableShema := storage.Schema{
				{Name: personFieldName, Type: storage.ColumnTypeInt64},
			}
			err = e.CreateVertexType(txnID, personVTableName, personVTableShema, logger)
			require.NoError(t, err)

			workplaceVTableShema := storage.Schema{
				{Name: workplaceFieldName, Type: storage.ColumnTypeInt64},
			}
			err = e.CreateVertexType(txnID, workplaceVTableName, workplaceVTableShema, logger)
			require.NoError(t, err)

			employsETableShema := storage.Schema{
				{Name: employsFieldName, Type: storage.ColumnTypeInt64},
			}
			err = e.CreateEdgeType(
				txnID,
				employsETableName,
				employsETableShema,
				personVTableName,
				workplaceVTableName,
				logger,
			)
			require.NoError(t, err)

			friendETableSchema := storage.Schema{
				{Name: friendFieldName, Type: storage.ColumnTypeInt64},
			}
			err = e.CreateEdgeType(
				txnID,
				friendETableName,
				friendETableSchema,
				personVTableName,
				personVTableName,
				logger,
			)
			require.NoError(t, err)
			return nil
		},
		false,
	)
	require.NoError(t, err)

	firstPersonVRecord := storage.VertexInfo{
		SystemID: storage.VertexSystemID(uuid.New()),
		Data: map[string]any{
			personFieldName: int64(1),
		},
	}
	secondPersonVRecord := storage.VertexInfo{
		SystemID: storage.VertexSystemID(uuid.New()),
		Data: map[string]any{
			personFieldName: int64(2),
		},
	}
	workplaceVRecord := storage.VertexInfo{
		SystemID: storage.VertexSystemID(uuid.New()),
		Data: map[string]any{
			workplaceFieldName: int64(3),
		},
	}

	employsEdgeRecord := storage.EdgeInfo{
		SystemID:    storage.EdgeSystemID(uuid.New()),
		SrcVertexID: firstPersonVRecord.SystemID,
		DstVertexID: workplaceVRecord.SystemID,
		Data: map[string]any{
			employsFieldName: int64(4),
		},
	}

	friendEdgeRecord := storage.EdgeInfo{
		SystemID:    storage.EdgeSystemID(uuid.New()),
		SrcVertexID: firstPersonVRecord.SystemID,
		DstVertexID: secondPersonVRecord.SystemID,
		Data: map[string]any{
			friendFieldName: int64(5),
		},
	}
	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			err = e.InsertVertex(
				txnID,
				personVTableName,
				firstPersonVRecord,
				logger,
			)
			require.NoError(t, err)

			err = e.InsertVertex(
				txnID,
				personVTableName,
				secondPersonVRecord,
				logger,
			)
			require.NoError(t, err)

			err = e.InsertVertex(txnID, workplaceVTableName, workplaceVRecord, logger)
			require.NoError(t, err)

			err = e.InsertEdge(txnID, employsETableName, employsEdgeRecord, logger)
			require.NoError(t, err)

			err = e.InsertEdge(txnID, friendETableName, friendEdgeRecord, logger)
			require.NoError(t, err)
			return nil
		},
		false,
	)
	require.NoError(t, err)

	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			employsEdge, err := e.SelectEdge(
				txnID,
				employsETableName,
				employsEdgeRecord.SystemID,
				logger,
			)
			require.NoError(t, err)
			require.Equal(t, int64(4), employsEdge.Data[employsFieldName])

			friendEdge, err := e.SelectEdge(
				txnID,
				friendETableName,
				friendEdgeRecord.SystemID,
				logger,
			)
			require.NoError(t, err)
			require.Equal(t, int64(5), friendEdge.Data[friendFieldName])

			neighbors, err := e.GetVerticesOnDepth(
				txnID,
				personVTableName,
				firstPersonVRecord.SystemID,
				1,
				storage.AllowAllVerticesFilter,
				logger,
			)
			require.NoError(t, err)
			require.Equal(t, len(neighbors), 2)
			require.ElementsMatch(
				t,
				[]storage.VertexSystemID{neighbors[1].V, neighbors[0].V},
				[]storage.VertexSystemID{secondPersonVRecord.SystemID, workplaceVRecord.SystemID},
			)

			neighbors, err = e.GetVerticesOnDepth(
				txnID,
				workplaceVTableName,
				workplaceVRecord.SystemID,
				1,
				storage.AllowAllVerticesFilter,
				logger,
			)
			require.NoError(t, err)
			require.Equal(t, len(neighbors), 0)

			firstPersonV, err := e.SelectVertex(
				txnID,
				personVTableName,
				firstPersonVRecord.SystemID,
				logger,
			)
			require.NoError(t, err)
			require.Equal(t, firstPersonV.Data[personFieldName], int64(1))

			secondPersonV, err := e.SelectVertex(
				txnID,
				personVTableName,
				secondPersonVRecord.SystemID,
				logger,
			)
			require.NoError(t, err)
			require.Equal(t, secondPersonV.Data[personFieldName], int64(2))

			workplaceV, err := e.SelectVertex(
				txnID,
				workplaceVTableName,
				workplaceVRecord.SystemID,
				logger,
			)
			require.NoError(t, err)
			require.Equal(t, workplaceV.Data[workplaceFieldName], int64(3))
			return nil
		},
		false,
	)
	require.NoError(t, err)
}

func TestSelectVerticesWithValues(t *testing.T) {
	fs := afero.NewMemMapFs()
	catalogBasePath := "/tmp/graphdb_test"
	e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, 10, true)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	ticker := atomic.Uint64{}

	vertTableName := "person"
	vertFieldName := "money"

	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			schema := storage.Schema{
				{Name: vertFieldName, Type: storage.ColumnTypeInt64},
			}
			err = e.CreateVertexType(txnID, vertTableName, schema, logger)
			require.NoError(t, err)
			return nil
		},
		false,
	)
	require.NoError(t, err)

	N := 10
	offset := 42
	vRecords := make([]storage.VertexInfo, 0)
	for i := range N {
		for j := range N {
			vRecords = append(vRecords, storage.VertexInfo{
				SystemID: storage.VertexSystemID(uuid.New()),
				Data: map[string]any{
					vertFieldName: int64(i*N + j + offset),
				},
			})
		}
		vRecords = append(vRecords, storage.VertexInfo{
			SystemID: storage.VertexSystemID(uuid.New()),
			Data: map[string]any{
				vertFieldName: int64(offset - 1),
			},
		})
	}

	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			err = e.InsertVertices(txnID, vertTableName, vRecords, logger)
			require.NoError(t, err)
			return nil
		},
		false,
	)
	require.NoError(t, err)

	c := 0
	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			for _, vID := range vRecords {
				v, err := e.SelectVertex(txnID, vertTableName, vID.SystemID, logger)
				require.NoError(t, err)
				if v.Data[vertFieldName].(int64) == int64(offset-1) {
					c++
				}
			}
			return nil
		},
		false,
	)
	require.NoError(t, err)
	require.Equal(t, c, N)

	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			vertices, err := e.GetAllVertexesWithFieldValue(
				txnID,
				vertTableName,
				vertFieldName,
				utils.ToBytes(int64(offset-1)),
				logger,
			)
			require.NoError(t, err)
			require.Equal(t, len(vertices), N)
			return nil
		},
		false,
	)
	require.NoError(t, err)

	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			vertices, err := e.GetAllVertexesWithFieldValue(
				txnID,
				vertTableName,
				vertFieldName,
				utils.ToBytes(int64(0)),
				logger,
			)
			require.NoError(t, err)
			require.Equal(t, len(vertices), 0)
			return nil
		},
		false,
	)
	require.NoError(t, err)
}

func TestGetAllTriangles(t *testing.T) {
	fs := afero.NewMemMapFs()
	catalogBasePath := "/tmp/graphdb_test"
	e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, 10, true)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	ticker := atomic.Uint64{}

	vertTableName := "person"
	vertFieldName := "money"

	edgeTableName := "indepted_to"
	edgeFieldName := "debt_amount"

	tests := []struct {
		graphInfo GraphInfo
		name      string
		expected  uint64
	}{
		{
			name: "empty graph",
			graphInfo: GraphInfo{
				g:            map[int][]int{},
				edgesInfo:    map[utils.Pair[int, int]]int64{},
				verticesInfo: map[int]int64{},
			},
			expected: 0,
		},
		{
			name: "one triangle",
			graphInfo: GraphInfo{
				g: map[int][]int{
					1: {2, 3},
					2: {1, 3},
					3: {1, 2},
				},
				edgesInfo: map[utils.Pair[int, int]]int64{
					{First: 1, Second: 2}: 100,
					{First: 1, Second: 3}: 200,
					{First: 2, Second: 3}: 300,
					{First: 2, Second: 1}: 400,
					{First: 3, Second: 1}: 500,
					{First: 3, Second: 2}: 600,
				},
				verticesInfo: map[int]int64{
					1: 100,
					2: 200,
					3: 300,
				},
			},
			expected: 1,
		},
		{
			name: "two triangle",
			graphInfo: GraphInfo{
				g: map[int][]int{
					1: {2, 3},
					2: {1, 3, 4},
					3: {1, 2, 4},
					4: {2, 3},
				},
				edgesInfo: map[utils.Pair[int, int]]int64{
					{First: 1, Second: 2}: 100,
					{First: 1, Second: 3}: 200,
					{First: 2, Second: 1}: 300,
					{First: 2, Second: 3}: 400,
					{First: 2, Second: 4}: 500,
					{First: 3, Second: 1}: 600,
					{First: 3, Second: 2}: 700,
					{First: 3, Second: 4}: 800,
					{First: 4, Second: 2}: 900,
					{First: 4, Second: 3}: 1000,
				},
				verticesInfo: map[int]int64{
					1: 100,
					2: 200,
					3: 300,
					4: 400,
				},
			},
			expected: 2,
		},
	}

	for _, test := range tests {
		setupTables(
			t,
			e,
			&ticker,
			vertTableName,
			vertFieldName,
			edgeTableName,
			edgeFieldName,
			logger,
		)
		instantiateGraph(
			t,
			&ticker,
			vertTableName,
			edgeTableName,
			e,
			logger,
			test.graphInfo.g,
			edgeFieldName,
			test.graphInfo.edgesInfo,
			vertFieldName,
			test.graphInfo.verticesInfo,
		)

		err = Execute(
			&ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
				triangles, err := e.GetAllTriangles(txnID, vertTableName, logger)
				require.NoError(t, err)
				require.Equal(t, triangles, test.expected)
				return nil
			},
			false,
		)
		require.NoError(t, err)

		err = Execute(
			&ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
				require.NoError(t, e.DropVertexTable(txnID, vertTableName, logger))
				require.NoError(t, e.DropEdgeTable(txnID, edgeTableName, logger))
				return nil
			},
			false,
		)
		require.NoError(t, err)
	}
}

func graphCountTriangles(g map[int][]int) [][]int {
	// For an oriented graph, three vertices V1, V2, V3 form a triangle if there exist
	// all 6 directed edges: V1->V2, V2->V1, V1->V3, V3->V1, V2->V3, V3->V2

	triangles := make([][]int, 0)

	// Get all vertices
	vertices := make([]int, 0, len(g))
	for v := range g {
		vertices = append(vertices, v)
	}

	// Check all possible triplets of vertices
	for i := 0; i < len(vertices); i++ {
		for j := i + 1; j < len(vertices); j++ {
			for k := j + 1; k < len(vertices); k++ {
				v1, v2, v3 := vertices[i], vertices[j], vertices[k]

				// Check if all 6 directed edges exist
				if hasDirectedEdge(g, v1, v2) && hasDirectedEdge(g, v2, v1) &&
					hasDirectedEdge(g, v1, v3) && hasDirectedEdge(g, v3, v1) &&
					hasDirectedEdge(g, v2, v3) && hasDirectedEdge(g, v3, v2) {
					triangles = append(triangles, []int{v1, v2, v3})
				}
			}
		}
	}

	return triangles
}

// hasDirectedEdge checks if there's a directed edge from src to dst
func hasDirectedEdge(g map[int][]int, src, dst int) bool {
	neighbors, exists := g[src]
	if !exists {
		return false
	}

	for _, neighbor := range neighbors {
		if neighbor == dst {
			return true
		}
	}
	return false
}

func TestGraphCountTriangles(t *testing.T) {
	tests := []struct {
		name     string
		graph    map[int][]int
		expected uint64
	}{
		{
			name:     "empty graph",
			graph:    map[int][]int{},
			expected: 0,
		},
		{
			name: "single vertex",
			graph: map[int][]int{
				1: {},
			},
			expected: 0,
		},
		{
			name: "two vertices, no edges",
			graph: map[int][]int{
				1: {},
				2: {},
			},
			expected: 0,
		},
		{
			name: "two vertices, one edge",
			graph: map[int][]int{
				1: {2},
				2: {},
			},
			expected: 0,
		},
		{
			name: "two vertices, bidirectional edge",
			graph: map[int][]int{
				1: {2},
				2: {1},
			},
			expected: 0,
		},
		{
			name: "three vertices, no triangle",
			graph: map[int][]int{
				1: {2},
				2: {1},
				3: {},
			},
			expected: 0,
		},
		{
			name: "three vertices, partial triangle (missing some edges)",
			graph: map[int][]int{
				1: {2, 3},
				2: {1},
				3: {1},
			},
			expected: 0,
		},
		{
			name: "one complete oriented triangle",
			graph: map[int][]int{
				1: {2, 3},
				2: {1, 3},
				3: {1, 2},
			},
			expected: 1,
		},
		{
			name: "two complete oriented triangles",
			graph: map[int][]int{
				1: {2, 3},
				2: {1, 3, 4},
				3: {1, 2, 4},
				4: {2, 3},
			},
			expected: 2,
		},
		{
			name: "three complete oriented triangles",
			graph: map[int][]int{
				1: {2, 3, 4},
				2: {1, 3, 4},
				3: {1, 2, 4},
				4: {1, 2, 3},
			},
			expected: 4, // C(4,3) = 4 triangles
		},
		{
			name: "complex graph with mixed triangles",
			graph: map[int][]int{
				1: {2, 3},
				2: {1, 3, 4},
				3: {1, 2, 4, 5},
				4: {2, 3, 5},
				5: {3, 4},
			},
			expected: 3, // (1,2,3), (2,3,4), and (3,4,5) form complete oriented triangles
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := graphCountTriangles(test.graph)
			require.Equal(t, test.expected, uint64(len(result)),
				"Expected %d triangles but got %d for graph %v",
				test.expected, result, test.graph)
		})
	}
}

func TestRandomizedGetAllTriangles(t *testing.T) {
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
	}

	fs := afero.NewMemMapFs()
	catalogBasePath := "/tmp/graphdb_test"
	e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, 10, true)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	ticker := atomic.Uint64{}
	vertTableName := "person"
	verticesFieldName := "money"
	edgeTableName := "indepted_to"
	edgesFieldName := "debt_amount"

	for _, test := range tests {
		t.Run(
			fmt.Sprintf("vertexCount=%d,connectivity=%f", test.vertexCount, test.connectivity),
			func(t *testing.T) {
				setupTables(
					t,
					e,
					&ticker,
					vertTableName,
					verticesFieldName,
					edgeTableName,
					edgesFieldName,
					logger,
				)

				graphInfo := generateRandomGraph(
					t,
					test.vertexCount,
					test.connectivity,
					rand.New(rand.NewSource(42)),
					true,
				)
				expectedTriangles := graphCountTriangles(graphInfo.g)

				intToVertSystemID, edgesSystemInfo := instantiateGraph(
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
					edgeTableName,
					edgesFieldName,
					intToVertSystemID,
					edgesSystemInfo,
					1,
					1,
				)

				err = Execute(
					&ticker,
					e,
					logger,
					func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
						triangles, err := e.GetAllTriangles(txnID, vertTableName, logger)
						require.NoError(t, err)
						if !assert.Equal(t, uint64(len(expectedTriangles)), triangles) {
							t.Logf(
								"Graph: \n%s\nExpected triangles %v",
								graphInfo.GraphVizRepr(),
								expectedTriangles,
							)
							t.FailNow()
						}
						return nil
					},
					false,
				)
				require.NoError(t, err)

				err = Execute(
					&ticker,
					e,
					logger,
					func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
						require.NoError(t, e.DropVertexTable(txnID, vertTableName, logger))
						require.NoError(t, e.DropEdgeTable(txnID, edgeTableName, logger))
						return nil
					},
					false,
				)
				require.NoError(t, err)
			},
		)
	}
}

func TestPhantomRead(t *testing.T) {
	fs := afero.NewMemMapFs()
	catalogBasePath := "/tmp/graphdb_test"
	e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, 10, false)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	ticker := atomic.Uint64{}
	vertTableName := "person"
	verticesFieldName := "money"
	edgeTableName := "indepted_to"
	edgesFieldName := "debt_amount"

	setupTables(
		t,
		e,
		&ticker,
		vertTableName,
		verticesFieldName,
		edgeTableName,
		edgesFieldName,
		logger,
	)

	vertices := make([]storage.VertexInfo, 0, 100)
	for i := 0; i < 100; i++ {
		vertices = append(vertices, storage.VertexInfo{
			SystemID: storage.VertexSystemID(uuid.New()),
			Data: map[string]any{
				verticesFieldName: int64(i),
			},
		})
	}

	signaller := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := Execute(
			&ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
				err = e.InsertVertices(txnID, vertTableName, vertices, logger)
				require.NoError(t, err)
				signaller <- struct{}{}
				time.Sleep(time.Second * 3)
				return nil
			},
			false,
		)
		require.NoError(t, err)
	}()

	time.Sleep(time.Second * 1)

	wg.Add(1)
	go func() {
		defer wg.Done()

		err := Execute(
			&ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
				<-signaller
				for _, vert := range vertices {
					_, err := e.SelectVertex(txnID, vertTableName, vert.SystemID, logger)
					require.ErrorIs(t, err, txns.ErrDeadlockPrevention)
					break
				}
				return nil
			},
			false,
		)
		require.NoError(t, err)
	}()
	wg.Wait()
}

func TestRecovery(t *testing.T) {
	fs := afero.NewMemMapFs()
	catalogBasePath := "/tmp/graphdb_test"

	ticker := atomic.Uint64{}
	vertTableName := "person"
	verticesFieldName := "money"
	edgeTableName := "indepted_to"
	edgesFieldName := "debt_amount"

	graphInfo := GraphInfo{
		g: map[int][]int{
			1: {2},
		},
		edgesInfo: map[utils.Pair[int, int]]int64{
			{First: 1, Second: 2}: 100,
		},
		verticesInfo: map[int]int64{
			1: 100,
			2: 200,
		},
	}

	var intToVertSystemID map[int]storage.VertexSystemID
	var edgesSystemInfo map[utils.Pair[int, int]]storage.EdgeSystemID
	func() {
		e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, 10, false)
		require.NoError(t, err)
		defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

		setupTables(
			t,
			e,
			&ticker,
			vertTableName,
			verticesFieldName,
			edgeTableName,
			edgesFieldName,
			logger,
		)

		intToVertSystemID, edgesSystemInfo = instantiateGraph(
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

		t.Log("asserting a graph...")
		assertDBGraph(
			t,
			&ticker,
			e,
			logger,
			graphInfo,
			vertTableName,
			verticesFieldName,
			edgeTableName,
			edgesFieldName,
			intToVertSystemID,
			edgesSystemInfo,
			1,
			1,
		)
		require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked())
	}()

	func() {
		t.Log("recovering a graph...")
		e, _, _, logger, err := setupExecutor(fs, catalogBasePath, 10, false)

		b, err := logger.Dump(
			common.FileLocation{PageID: common.CheckpointInfoPageID + 1, SlotNum: 0},
		)
		require.NoError(t, err)
		t.Logf("Log file:\n%s", b)

		require.NoError(t, err)
		t.Log("asserting a graph after recovery...")
		assertDBGraph(
			t,
			&ticker,
			e,
			logger,
			graphInfo,
			vertTableName,
			verticesFieldName,
			edgeTableName,
			edgesFieldName,
			intToVertSystemID,
			edgesSystemInfo,
			1,
			1,
		)
	}()
}

func TestRecoveryRandomized(t *testing.T) {
	fs := afero.NewOsFs()
	catalogBasePath := t.TempDir()

	ticker := atomic.Uint64{}
	vertTableName := "person"
	verticesFieldName := "money"
	edgeTableName := "indepted_to"
	edgesFieldName := "debt_amount"

	graphInfo := generateRandomGraph(t, 100, 0.05, rand.New(rand.NewSource(42)), false)

	var intToVertSystemID map[int]storage.VertexSystemID
	var edgesSystemInfo map[utils.Pair[int, int]]storage.EdgeSystemID
	func() {
		e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, 10, false)
		require.NoError(t, err)
		defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

		setupTables(
			t,
			e,
			&ticker,
			vertTableName,
			verticesFieldName,
			edgeTableName,
			edgesFieldName,
			logger,
		)

		intToVertSystemID, edgesSystemInfo = instantiateGraph(
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

		t.Log("asserting a graph...")
		assertDBGraph(
			t,
			&ticker,
			e,
			logger,
			graphInfo,
			vertTableName,
			verticesFieldName,
			edgeTableName,
			edgesFieldName,
			intToVertSystemID,
			edgesSystemInfo,
			1,
			1,
		)
		require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked())
	}()

	func() {
		t.Log("recovering a graph...")
		e, _, _, logger, err := setupExecutor(fs, catalogBasePath, 10, false)

		require.NoError(t, err)
		t.Log("asserting a graph after recovery...")
		assertDBGraph(
			t,
			&ticker,
			e,
			logger,
			graphInfo,
			vertTableName,
			verticesFieldName,
			edgeTableName,
			edgesFieldName,
			intToVertSystemID,
			edgesSystemInfo,
			1,
			1,
		)

		secondVertTableName := "person2"
		secondVerticesFieldName := "money2"
		secondEdgeTableName := "indepted_to2"
		secondEdgesFieldName := "debt_amount2"
		secondGraphInfo := generateRandomGraph(t, 100, 0.05, rand.New(rand.NewSource(42)), false)

		setupTables(
			t,
			e,
			&ticker,
			secondVertTableName,
			secondVerticesFieldName,
			secondEdgeTableName,
			secondEdgesFieldName,
			logger,
		)

		secondIntToVertSystemID, secondEdgesSystemInfo := instantiateGraph(
			t,
			&ticker,
			secondVertTableName,
			secondEdgeTableName,
			e,
			logger,
			secondGraphInfo.g,
			secondEdgesFieldName,
			secondGraphInfo.edgesInfo,
			secondVerticesFieldName,
			secondGraphInfo.verticesInfo,
		)
		assertDBGraph(
			t,
			&ticker,
			e,
			logger,
			secondGraphInfo,
			secondVertTableName,
			secondVerticesFieldName,
			secondEdgeTableName,
			secondEdgesFieldName,
			secondIntToVertSystemID,
			secondEdgesSystemInfo,
			1,
			1,
		)
		assertDBGraph(
			t,
			&ticker,
			e,
			logger,
			graphInfo,
			vertTableName,
			verticesFieldName,
			edgeTableName,
			edgesFieldName,
			intToVertSystemID,
			edgesSystemInfo,
			1,
			1,
		)
	}()
}

func TestRecoveryCheckpoint(t *testing.T) {
	catalogBasePath := "/tmp/graphdb_test"
	fs := afero.NewMemMapFs()

	ticker := atomic.Uint64{}
	vertTableName := "person"
	vertFieldName := "money"
	edgeTableName := "indepted_to"
	edgeFieldName := "debt_amount"

	graphInfo := generateRandomGraph(t, 100, 0.05, rand.New(rand.NewSource(42)), false)
	var intToVertSystemID map[int]storage.VertexSystemID
	var edgesSystemInfo map[utils.Pair[int, int]]storage.EdgeSystemID

	func() {
		e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, 10, false)
		require.NoError(t, err)
		defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()
		setupTables(
			t,
			e,
			&ticker,
			vertTableName,
			vertFieldName,
			edgeTableName,
			edgeFieldName,
			logger,
		)
		intToVertSystemID, edgesSystemInfo = instantiateGraph(
			t,
			&ticker,
			vertTableName,
			edgeTableName,
			e,
			logger,
			graphInfo.g,
			edgeFieldName,
			graphInfo.edgesInfo,
			vertFieldName,
			graphInfo.verticesInfo,
		)

		require.NoError(t, pool.FlushAllPages())
		assertDBGraph(
			t,
			&ticker,
			e,
			logger,
			graphInfo,
			vertTableName,
			vertFieldName,
			edgeTableName,
			edgeFieldName,
			intToVertSystemID,
			edgesSystemInfo,
			1,
			1,
		)
	}()

	secondVertTableName := "person2"
	secondVerticesFieldName := "money2"
	secondEdgeTableName := "indepted_to2"
	secondEdgesFieldName := "debt_amount2"

	secondGraphInfo := generateRandomGraph(t, 100, 0.05, rand.New(rand.NewSource(42)), false)

	var secondIntToVertSystemID map[int]storage.VertexSystemID
	var secondEdgesSystemInfo map[utils.Pair[int, int]]storage.EdgeSystemID
	func() {
		e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, 10, false)
		require.NoError(t, err)
		require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked())

		assertDBGraph(
			t,
			&ticker,
			e,
			logger,
			graphInfo,
			vertTableName,
			vertFieldName,
			edgeTableName,
			edgeFieldName,
			intToVertSystemID,
			edgesSystemInfo,
			1,
			1,
		)

		setupTables(
			t,
			e,
			&ticker,
			secondVertTableName,
			secondVerticesFieldName,
			secondEdgeTableName,
			secondEdgesFieldName,
			logger,
		)

		secondIntToVertSystemID, secondEdgesSystemInfo = instantiateGraph(
			t,
			&ticker,
			secondVertTableName,
			secondEdgeTableName,
			e,
			logger,
			secondGraphInfo.g,
			secondEdgesFieldName,
			secondGraphInfo.edgesInfo,
			secondVerticesFieldName,
			secondGraphInfo.verticesInfo,
		)

		assertDBGraph(
			t,
			&ticker,
			e,
			logger,
			secondGraphInfo,
			secondVertTableName,
			secondVerticesFieldName,
			secondEdgeTableName,
			secondEdgesFieldName,
			secondIntToVertSystemID,
			secondEdgesSystemInfo,
			1,
			1,
		)
	}()

	func() {
		e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, 10, false)
		require.NoError(t, err)
		require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked())

		assertDBGraph(
			t,
			&ticker,
			e,
			logger,
			graphInfo,
			vertTableName,
			vertFieldName,
			edgeTableName,
			edgeFieldName,
			intToVertSystemID,
			edgesSystemInfo,
			1,
			1,
		)
		assertDBGraph(
			t,
			&ticker,
			e,
			logger,
			secondGraphInfo,
			secondVertTableName,
			secondVerticesFieldName,
			secondEdgeTableName,
			secondEdgesFieldName,
			secondIntToVertSystemID,
			secondEdgesSystemInfo,
			1,
			1,
		)
	}()
}

func TestSimpleUnfinishedTxnRecovery(t *testing.T) {
	catalogBasePath := "/tmp/graphdb_test"
	fs := afero.NewMemMapFs()

	const (
		nSuccess = 100
		nFailed  = 100
	)

	ticker := atomic.Uint64{}
	vertTableName := "person"
	vertFieldName := "money"
	schema := storage.Schema{
		{Name: vertFieldName, Type: storage.ColumnTypeInt64},
	}

	vertices := make([]storage.VertexInfo, nSuccess)
	for i := range nSuccess {
		vertices[i] = storage.VertexInfo{
			SystemID: storage.VertexSystemID(uuid.New()),
			Data: map[string]any{
				vertFieldName: int64(i),
			},
		}
	}

	failedVertices := make([]storage.VertexInfo, nFailed)
	for i := range nFailed {
		failedVertices[i] = storage.VertexInfo{
			SystemID: storage.VertexSystemID(uuid.New()),
			Data: map[string]any{
				vertFieldName: int64(i + nSuccess),
			},
		}
	}

	func() {
		e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, 23, false)
		require.NoError(t, err)
		defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

		err = Execute(
			&ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
				err = e.CreateVertexType(txnID, vertTableName, schema, logger)
				require.NoError(t, err)
				return nil
			},
			false,
		)
		require.NoError(t, err)

		err = Execute(
			&ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
				err = e.InsertVertices(txnID, vertTableName, vertices, logger)
				require.NoError(t, err)
				return nil
			},
			false,
		)
		require.NoError(t, err)

		failedTxnID := common.TxnID(ticker.Add(1))
		ctxLogger := logger.WithContext(failedTxnID)
		require.NoError(t, ctxLogger.AppendBegin())
		for i := range nFailed {
			err := e.InsertVertex(failedTxnID, vertTableName, failedVertices[i], ctxLogger)
			require.NoError(t, err)
		}
	}()

	func() {
		e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, 11, false)
		require.NoError(t, err)
		defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()
		err = Execute(
			&ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
				for i := range nSuccess {
					vert, err := e.SelectVertex(txnID, vertTableName, vertices[i].SystemID, logger)
					require.NoError(t, err)
					require.Equal(t, vert.Data[vertFieldName], vertices[i].Data[vertFieldName])
				}
				for i := range nFailed {
					_, err := e.SelectVertex(
						txnID,
						vertTableName,
						failedVertices[i].SystemID,
						logger,
					)
					require.ErrorIs(t, err, storage.ErrKeyNotFound)
				}
				return nil
			},
			false,
		)
		require.NoError(t, err)
	}()
}

// TestGetVertexesOnDepthConcurrent tests that GetVertexesOnDepth can be called concurrently
// from multiple goroutines safely since it's a read-only operation.
func TestGetVertexesOnDepthConcurrent(t *testing.T) {
	fs := afero.NewMemMapFs()
	catalogBasePath := "/tmp/graphdb_concurrent_test"
	poolPageCount := uint64(100)
	debugMode := true

	e, debugPool, _, logger, err := setupExecutor(fs, catalogBasePath, poolPageCount, debugMode)
	require.NoError(t, err)
	defer func() { require.NoError(t, debugPool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	// Setup test data
	vertTableName := "person"
	edgeTableName := "friend"
	vertSchema := storage.Schema{
		{Name: "id", Type: storage.ColumnTypeInt64},
	}
	edgeSchema := storage.Schema{
		{Name: "weight", Type: storage.ColumnTypeInt64},
	}

	var ticker atomic.Uint64

	// Create a simple graph: A -> B -> C, A -> D
	// This creates vertices at different depths from A
	vertices := []storage.VertexInfo{
		{SystemID: storage.VertexSystemID(uuid.New()), Data: map[string]any{"id": int64(1)}}, // A
		{SystemID: storage.VertexSystemID(uuid.New()), Data: map[string]any{"id": int64(2)}}, // B
		{SystemID: storage.VertexSystemID(uuid.New()), Data: map[string]any{"id": int64(3)}}, // C
		{SystemID: storage.VertexSystemID(uuid.New()), Data: map[string]any{"id": int64(4)}}, // D
	}

	edges := []storage.EdgeInfo{
		{
			SystemID:    storage.EdgeSystemID(uuid.New()),
			SrcVertexID: vertices[0].SystemID,
			DstVertexID: vertices[1].SystemID,
			Data:        map[string]any{"weight": int64(1)},
		}, // A -> B
		{
			SystemID:    storage.EdgeSystemID(uuid.New()),
			SrcVertexID: vertices[1].SystemID,
			DstVertexID: vertices[2].SystemID,
			Data:        map[string]any{"weight": int64(2)},
		}, // B -> C
		{
			SystemID:    storage.EdgeSystemID(uuid.New()),
			SrcVertexID: vertices[0].SystemID,
			DstVertexID: vertices[3].SystemID,
			Data:        map[string]any{"weight": int64(3)},
		}, // A -> D
	}

	// Setup the graph
	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			err = e.CreateVertexType(txnID, vertTableName, vertSchema, logger)
			require.NoError(t, err)

			err = e.CreateEdgeType(txnID, edgeTableName, edgeSchema, "person", "person", logger)
			require.NoError(t, err)

			for _, vertex := range vertices {
				err = e.InsertVertex(txnID, vertTableName, vertex, logger)
				require.NoError(t, err)
			}

			for _, edge := range edges {
				err = e.InsertEdge(txnID, edgeTableName, edge, logger)
				require.NoError(t, err)
			}
			return nil
		},
		false,
	)
	require.NoError(t, err)

	// Test concurrent reads
	const numGoroutines = 10
	const numIterations = 5

	var wg sync.WaitGroup
	results := make([][]storage.VertexSystemIDWithRID, numGoroutines*numIterations)
	errors := make([]error, numGoroutines*numIterations)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < numIterations; j++ {
				index := goroutineID*numIterations + j

				err := Execute(
					&ticker,
					e,
					logger,
					func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
						// Test depth 1 from vertex A (should return B and D)
						result, err := e.GetVerticesOnDepth(
							txnID,
							vertTableName,
							vertices[0].SystemID, // A
							1,
							storage.AllowAllVerticesFilter,
							logger,
						)
						if err != nil {
							errors[index] = err
							return err
						}

						results[index] = result
						return nil
					},
					false,
				)
				if err != nil {
					errors[index] = err
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify all operations succeeded
	for i, err := range errors {
		require.NoError(t, err, "Goroutine %d failed", i)
	}

	// Verify all results are consistent
	expectedVertices := []storage.VertexSystemID{
		vertices[1].SystemID,
		vertices[3].SystemID,
	} // B and D
	for i, result := range results {
		require.NotNil(t, result, "Result %d is nil", i)
		require.Len(t, result, 2, "Result %d should have 2 vertices", i)

		actualVertices := make([]storage.VertexSystemID, len(result))
		for j, v := range result {
			actualVertices[j] = v.V
		}
		require.ElementsMatch(
			t,
			expectedVertices,
			actualVertices,
			"Result %d vertices don't match expected",
			i,
		)
	}
}

// TestGetVertexesOnDepthConcurrentWithDifferentDepths tests concurrent queries with different depth
// parameters
func TestGetVertexesOnDepthConcurrentWithDifferentDepths(t *testing.T) {
	fs := afero.NewMemMapFs()
	catalogBasePath := "/tmp/graphdb_concurrent_depths_test"
	poolPageCount := uint64(100)
	debugMode := true

	e, debugPool, _, logger, err := setupExecutor(fs, catalogBasePath, poolPageCount, debugMode)
	require.NoError(t, err)
	defer func() { require.NoError(t, debugPool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	// Setup test data - create a deeper graph: A -> B -> C -> D
	vertTableName := "person"
	edgeTableName := "friend"
	vertSchema := storage.Schema{
		{Name: "id", Type: storage.ColumnTypeInt64},
	}
	edgeSchema := storage.Schema{
		{Name: "weight", Type: storage.ColumnTypeInt64},
	}

	var ticker atomic.Uint64

	vertices := []storage.VertexInfo{
		{SystemID: storage.VertexSystemID(uuid.New()), Data: map[string]any{"id": int64(1)}}, // A
		{SystemID: storage.VertexSystemID(uuid.New()), Data: map[string]any{"id": int64(2)}}, // B
		{SystemID: storage.VertexSystemID(uuid.New()), Data: map[string]any{"id": int64(3)}}, // C
		{SystemID: storage.VertexSystemID(uuid.New()), Data: map[string]any{"id": int64(4)}}, // D
	}

	edges := []storage.EdgeInfo{
		{
			SystemID:    storage.EdgeSystemID(uuid.New()),
			SrcVertexID: vertices[0].SystemID,
			DstVertexID: vertices[1].SystemID,
			Data:        map[string]any{"weight": int64(1)},
		}, // A -> B
		{
			SystemID:    storage.EdgeSystemID(uuid.New()),
			SrcVertexID: vertices[1].SystemID,
			DstVertexID: vertices[2].SystemID,
			Data:        map[string]any{"weight": int64(2)},
		}, // B -> C
		{
			SystemID:    storage.EdgeSystemID(uuid.New()),
			SrcVertexID: vertices[2].SystemID,
			DstVertexID: vertices[3].SystemID,
			Data:        map[string]any{"weight": int64(3)},
		}, // C -> D
	}

	// Setup the graph
	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			err = e.CreateVertexType(txnID, vertTableName, vertSchema, logger)
			require.NoError(t, err)

			err = e.CreateEdgeType(txnID, edgeTableName, edgeSchema, "person", "person", logger)
			require.NoError(t, err)

			for _, vertex := range vertices {
				err = e.InsertVertex(txnID, vertTableName, vertex, logger)
				require.NoError(t, err)
			}

			for _, edge := range edges {
				err = e.InsertEdge(txnID, edgeTableName, edge, logger)
				require.NoError(t, err)
			}
			return nil
		},
		false,
	)
	require.NoError(t, err)

	// Test concurrent reads with different depths
	const numGoroutines = 8
	depths := []uint32{1, 2, 3}
	expectedResults := [][]storage.VertexSystemID{
		{vertices[1].SystemID}, // depth 1: B
		{vertices[2].SystemID}, // depth 2: C
		{vertices[3].SystemID}, // depth 3: D
	}

	var wg sync.WaitGroup
	results := make([][]storage.VertexSystemIDWithRID, numGoroutines*len(depths))
	errors := make([]error, numGoroutines*len(depths))

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for j, depth := range depths {
				index := goroutineID*len(depths) + j

				err := Execute(
					&ticker,
					e,
					logger,
					func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
						result, err := e.GetVerticesOnDepth(
							txnID,
							vertTableName,
							vertices[0].SystemID, // A
							depth,
							storage.AllowAllVerticesFilter,
							logger,
						)
						if err != nil {
							errors[index] = err
							return err
						}

						results[index] = result
						return nil
					},
					false,
				)
				if err != nil {
					errors[index] = err
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify all operations succeeded
	for i, err := range errors {
		require.NoError(t, err, "Goroutine %d failed", i)
	}

	// Verify results are consistent for each depth
	for i := 0; i < numGoroutines; i++ {
		for j, depth := range depths {
			index := i*len(depths) + j
			result := results[index]
			expected := expectedResults[j]

			require.NotNil(t, result, "Result for depth %d, goroutine %d is nil", depth, i)
			require.Len(
				t,
				result,
				len(expected),
				"Result for depth %d, goroutine %d should have %d vertices",
				depth,
				i,
				len(expected),
			)

			actualVertices := make([]storage.VertexSystemID, len(result))
			for k, v := range result {
				actualVertices[k] = v.V
			}
			require.ElementsMatch(
				t,
				expected,
				actualVertices,
				"Result for depth %d, goroutine %d vertices don't match expected",
				depth,
				i,
			)
		}
	}
}

// TestGetVertexesOnDepthConcurrentWithDifferentStartVertices tests concurrent queries with
// different starting vertices
func TestGetVertexesOnDepthConcurrentWithDifferentStartVertices(t *testing.T) {
	fs := afero.NewMemMapFs()
	catalogBasePath := "/tmp/graphdb_concurrent_starts_test"
	poolPageCount := uint64(100)
	debugMode := true

	e, debugPool, _, logger, err := setupExecutor(fs, catalogBasePath, poolPageCount, debugMode)
	require.NoError(t, err)
	defer func() { require.NoError(t, debugPool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	// Setup test data - create a graph with multiple starting points
	vertTableName := "person"
	edgeTableName := "friend"
	vertSchema := storage.Schema{
		{Name: "id", Type: storage.ColumnTypeInt64},
	}
	edgeSchema := storage.Schema{
		{Name: "weight", Type: storage.ColumnTypeInt64},
	}

	var ticker atomic.Uint64

	// Create graph: A -> B -> C, D -> E, F (isolated)
	vertices := []storage.VertexInfo{
		{SystemID: storage.VertexSystemID(uuid.New()), Data: map[string]any{"id": int64(1)}}, // A
		{SystemID: storage.VertexSystemID(uuid.New()), Data: map[string]any{"id": int64(2)}}, // B
		{SystemID: storage.VertexSystemID(uuid.New()), Data: map[string]any{"id": int64(3)}}, // C
		{SystemID: storage.VertexSystemID(uuid.New()), Data: map[string]any{"id": int64(4)}}, // D
		{SystemID: storage.VertexSystemID(uuid.New()), Data: map[string]any{"id": int64(5)}}, // E
		{SystemID: storage.VertexSystemID(uuid.New()), Data: map[string]any{"id": int64(6)}}, // F
	}

	edges := []storage.EdgeInfo{
		{
			SystemID:    storage.EdgeSystemID(uuid.New()),
			SrcVertexID: vertices[0].SystemID,
			DstVertexID: vertices[1].SystemID,
			Data:        map[string]any{"weight": int64(1)},
		}, // A -> B
		{
			SystemID:    storage.EdgeSystemID(uuid.New()),
			SrcVertexID: vertices[1].SystemID,
			DstVertexID: vertices[2].SystemID,
			Data:        map[string]any{"weight": int64(2)},
		}, // B -> C
		{
			SystemID:    storage.EdgeSystemID(uuid.New()),
			SrcVertexID: vertices[3].SystemID,
			DstVertexID: vertices[4].SystemID,
			Data:        map[string]any{"weight": int64(3)},
		}, // D -> E
	}

	// Setup the graph
	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			err = e.CreateVertexType(txnID, vertTableName, vertSchema, logger)
			require.NoError(t, err)

			err = e.CreateEdgeType(txnID, edgeTableName, edgeSchema, "person", "person", logger)
			require.NoError(t, err)

			for _, vertex := range vertices {
				err = e.InsertVertex(txnID, vertTableName, vertex, logger)
				require.NoError(t, err)
			}

			for _, edge := range edges {
				err = e.InsertEdge(txnID, edgeTableName, edge, logger)
				require.NoError(t, err)
			}
			return nil
		},
		false,
	)
	require.NoError(t, err)

	// Test concurrent reads from different starting vertices
	const numGoroutines = 6
	startVertices := []storage.VertexSystemID{
		vertices[0].SystemID,
		vertices[3].SystemID,
		vertices[5].SystemID,
	} // A, D, F
	expectedResults := [][]storage.VertexSystemID{
		{vertices[1].SystemID}, // A -> B at depth 1
		{vertices[4].SystemID}, // D -> E at depth 1
		{},                     // F has no neighbors
	}

	var wg sync.WaitGroup
	results := make([][]storage.VertexSystemIDWithRID, numGoroutines*len(startVertices))
	errors := make([]error, numGoroutines*len(startVertices))

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for j, startVertex := range startVertices {
				index := goroutineID*len(startVertices) + j

				err := Execute(
					&ticker,
					e,
					logger,
					func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
						result, err := e.GetVerticesOnDepth(
							txnID,
							vertTableName,
							startVertex,
							1, // depth 1
							storage.AllowAllVerticesFilter,
							logger,
						)
						if err != nil {
							errors[index] = err
							return err
						}

						results[index] = result
						return nil
					},
					false,
				)
				if err != nil {
					errors[index] = err
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify all operations succeeded
	for i, err := range errors {
		require.NoError(t, err, "Goroutine %d failed", i)
	}

	// Verify results are consistent for each starting vertex
	for i := 0; i < numGoroutines; i++ {
		for j := range startVertices {
			index := i*len(startVertices) + j
			result := results[index]
			expected := expectedResults[j]

			require.NotNil(t, result, "Result for start vertex %d, goroutine %d is nil", j, i)
			require.Len(
				t,
				result,
				len(expected),
				"Result for start vertex %d, goroutine %d should have %d vertices",
				j,
				i,
				len(expected),
			)

			actualVertices := make([]storage.VertexSystemID, len(result))
			for k, v := range result {
				actualVertices[k] = v.V
			}
			require.ElementsMatch(
				t,
				expected,
				actualVertices,
				"Result for start vertex %d, goroutine %d vertices don't match expected",
				j,
				i,
			)
		}
	}
}

func TestGetVertexesOnDepthConcurrentWithDifferentStartVerticesAndDifferentDepths(t *testing.T) {
	fs := afero.NewOsFs()
	catalogBasePath := t.TempDir()
	poolPageCount := uint64(100)
	debugMode := false

	e, debugPool, _, logger, err := setupExecutor(fs, catalogBasePath, poolPageCount, debugMode)
	require.NoError(t, err)
	defer func() { require.NoError(t, debugPool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	var ticker atomic.Uint64

	vertTableName := "person"
	vertFieldName := "id"
	edgeTableName := "friend"
	edgesFieldName := "weight"

	setupTables(
		t,
		e,
		&ticker,
		vertTableName,
		vertFieldName,
		edgeTableName,
		edgesFieldName,
		logger,
	)

	graphInfo := generateRandomGraph(t, 300, 0.05, rand.New(rand.NewSource(42)), false)
	intToVertSystemID, edgesSystemInfo := instantiateGraph(
		t,
		&ticker,
		vertTableName,
		edgeTableName,
		e,
		logger,
		graphInfo.g,
		edgesFieldName,
		graphInfo.edgesInfo,
		vertFieldName,
		graphInfo.verticesInfo,
	)
	t.Log("[one thread] asserting a graph...")
	assertDBGraph(
		t,
		&ticker,
		e,
		logger,
		graphInfo,
		vertTableName,
		vertFieldName,
		edgeTableName,
		edgesFieldName,
		intToVertSystemID,
		edgesSystemInfo,
		2,
		1,
	)

	t.Log("[concurrent] asserting a graph...")
	assertDBGraph(
		t,
		&ticker,
		e,
		logger,
		graphInfo,
		vertTableName,
		vertFieldName,
		edgeTableName,
		edgesFieldName,
		intToVertSystemID,
		edgesSystemInfo,
		2,
		128,
	)
}

func BenchmarkGetVertexesOnDepthSingleThreadedWithDifferentStartVerticesAndDifferentDepths(
	b *testing.B,
) {
	fs := afero.NewOsFs()
	catalogBasePath := b.TempDir()
	poolPageCount := uint64(300_000)
	debugMode := false

	e, debugPool, _, logger, err := setupExecutor(fs, catalogBasePath, poolPageCount, debugMode)
	require.NoError(b, err)
	defer func() { require.NoError(b, debugPool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	var ticker atomic.Uint64

	vertTableName := "person"
	vertFieldName := "id"
	edgeTableName := "friend"
	edgesFieldName := "weight"

	setupTables(
		b,
		e,
		&ticker,
		vertTableName,
		vertFieldName,
		edgeTableName,
		edgesFieldName,
		logger,
	)

	targetEdgeCountPerNode := 7
	connectivity := min(max(float32(targetEdgeCountPerNode)/float32(b.N), 0), 1)
	graphInfo := generateRandomGraph(b, b.N, connectivity, rand.New(rand.NewSource(42)), false)
	intToVertSystemID, edgesSystemInfo := instantiateGraph(
		b,
		&ticker,
		vertTableName,
		edgeTableName,
		e,
		logger,
		graphInfo.g,
		edgesFieldName,
		graphInfo.edgesInfo,
		vertFieldName,
		graphInfo.verticesInfo,
	)

	b.ResetTimer()
	assertDBGraph(
		b,
		&ticker,
		e,
		logger,
		graphInfo,
		vertTableName,
		vertFieldName,
		edgeTableName,
		edgesFieldName,
		intToVertSystemID,
		edgesSystemInfo,
		2,
		1,
	)
}

func BenchmarkGetVertexesOnDepthConcurrentWithDifferentStartVerticesAndDifferentDepths(
	b *testing.B,
) {
	fs := afero.NewOsFs()
	catalogBasePath := b.TempDir()
	poolPageCount := uint64(300_000)
	debugMode := false

	e, debugPool, _, logger, err := setupExecutor(fs, catalogBasePath, poolPageCount, debugMode)
	require.NoError(b, err)
	defer func() { require.NoError(b, debugPool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	var ticker atomic.Uint64

	vertTableName := "person"
	vertFieldName := "id"
	edgeTableName := "friend"
	edgesFieldName := "weight"

	setupTables(
		b,
		e,
		&ticker,
		vertTableName,
		vertFieldName,
		edgeTableName,
		edgesFieldName,
		logger,
	)

	targetEdgeCountPerNode := 7
	connectivity := min(max(float32(targetEdgeCountPerNode)/float32(b.N), 0), 1)
	graphInfo := generateRandomGraph(b, b.N, connectivity, rand.New(rand.NewSource(42)), false)
	intToVertSystemID, edgesSystemInfo := instantiateGraph(
		b,
		&ticker,
		vertTableName,
		edgeTableName,
		e,
		logger,
		graphInfo.g,
		edgesFieldName,
		graphInfo.edgesInfo,
		vertFieldName,
		graphInfo.verticesInfo,
	)

	b.ResetTimer()
	assertDBGraph(
		b,
		&ticker,
		e,
		logger,
		graphInfo,
		vertTableName,
		vertFieldName,
		edgeTableName,
		edgesFieldName,
		intToVertSystemID,
		edgesSystemInfo,
		2,
		10_000,
	)
}

func insertVertexWithRetry(
	t testing.TB,
	ticker *atomic.Uint64,
	e *Executor,
	logger common.ITxnLogger,
	tableName string,
	vertex storage.VertexInfo,
) {
	err := ExecuteWithRetry(
		ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			return e.InsertVertex(txnID, tableName, vertex, logger)
		},
	)
	if err != nil {
		require.ErrorIs(t, err, ErrRollback)
	}
}

func TestConcurrentVertexInsertsSameTable(t *testing.T) {
	fs := afero.NewMemMapFs()
	catalogBasePath := "/tmp/graphdb_concurrent_inserts_same_table"
	poolPageCount := uint64(50)
	debugMode := false

	e, debugPool, _, logger, err := setupExecutor(fs, catalogBasePath, poolPageCount, debugMode)
	require.NoError(t, err)
	defer func() { require.NoError(t, debugPool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	var ticker atomic.Uint64
	vertTableName := "person"
	vertFieldName := "id"

	// Create the vertex table
	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			schema := storage.Schema{{Name: vertFieldName, Type: storage.ColumnTypeInt64}}
			return e.CreateVertexType(txnID, vertTableName, schema, logger)
		},
		false,
	)
	require.NoError(t, err)

	// Prepare vertices
	const goroutines = 8
	const perGoroutine = 50
	total := goroutines * perGoroutine
	vertices := make([]storage.VertexInfo, total)
	for i := 0; i < total; i++ {
		vertices[i] = storage.VertexInfo{
			SystemID: storage.VertexSystemID(uuid.New()),
			Data: map[string]any{
				vertFieldName: int64(i + 1),
			},
		}
	}

	// Concurrent inserts with retry on deadlock
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		start := g * perGoroutine
		end := start + perGoroutine
		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			for i := start; i < end; i++ {
				insertVertexWithRetry(t, &ticker, e, logger, vertTableName, vertices[i])
			}
		}(start, end)
	}
	wg.Wait()

	// Verify all vertices are present
	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			for i := 0; i < total; i++ {
				v, err := e.SelectVertex(txnID, vertTableName, vertices[i].SystemID, logger)
				require.NoError(t, err)
				require.Equal(t, int64(i+1), v.Data[vertFieldName])
			}
			return nil
		},
		false,
	)
	require.NoError(t, err)
}

func TestConcurrentVertexInsertsMultipleTables(t *testing.T) {
	fs := afero.NewMemMapFs()
	catalogBasePath := "/tmp/graphdb_concurrent_inserts_multiple_tables"
	pools := uint64(80)
	debug := false

	e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, pools, debug)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	var ticker atomic.Uint64
	leftTable := "person_left"
	rightTable := "person_right"
	field := "id"

	// Create two vertex tables
	createTable := func(name string) {
		require.NoError(t, Execute(
			&ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
				schema := storage.Schema{{Name: field, Type: storage.ColumnTypeInt64}}
				return e.CreateVertexType(txnID, name, schema, logger)
			},
			false,
		))
	}
	createTable(leftTable)
	createTable(rightTable)

	const g = 6
	const per = 40
	total := g * per
	leftVertices := make([]storage.VertexInfo, total)
	rightVertices := make([]storage.VertexInfo, total)
	for i := 0; i < total; i++ {
		leftVertices[i] = storage.VertexInfo{
			SystemID: storage.VertexSystemID(uuid.New()),
			Data:     map[string]any{field: int64(1000 + i)},
		}
		rightVertices[i] = storage.VertexInfo{
			SystemID: storage.VertexSystemID(uuid.New()),
			Data:     map[string]any{field: int64(2000 + i)},
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < g; i++ {
		start := i * per
		end := start + per
		wg.Add(2)
		go func(s, eidx int) {
			defer wg.Done()
			for j := s; j < eidx; j++ {
				insertVertexWithRetry(t, &ticker, e, logger, leftTable, leftVertices[j])
			}
		}(start, end)
		go func(s, eidx int) {
			defer wg.Done()
			for j := s; j < eidx; j++ {
				insertVertexWithRetry(t, &ticker, e, logger, rightTable, rightVertices[j])
			}
		}(start, end)
	}
	wg.Wait()

	// Verify both tables
	verify := func(name string, verts []storage.VertexInfo, base int64) {
		require.NoError(t, Execute(
			&ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
				for i := 0; i < total; i++ {
					v, err := e.SelectVertex(txnID, name, verts[i].SystemID, logger)
					require.NoError(t, err)
					require.Equal(t, base+int64(i), v.Data[field])
				}
				return nil
			},
			false,
		))
	}
	verify(leftTable, leftVertices, 1000)
	verify(rightTable, rightVertices, 2000)
}

func TestConcurrentVertexInsertsHighContention(t *testing.T) {
	fs := afero.NewMemMapFs()
	catalogBasePath := "/tmp/graphdb_concurrent_inserts_high_contention"
	// Intentionally small pool to increase page contention
	pools := uint64(20)
	debug := false

	e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, pools, debug)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	var ticker atomic.Uint64
	table := "hot_vertices"
	field := "id"

	// Create the vertex table
	require.NoError(t, Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			schema := storage.Schema{{Name: field, Type: storage.ColumnTypeInt64}}
			return e.CreateVertexType(txnID, table, schema, logger)
		},
		false,
	))

	const writers = 12
	const insertsPerWriter = 75
	total := writers * insertsPerWriter
	verts := make([]storage.VertexInfo, total)
	for i := 0; i < total; i++ {
		verts[i] = storage.VertexInfo{
			SystemID: storage.VertexSystemID(uuid.New()),
			Data:     map[string]any{field: int64(i + 10)},
		}
	}

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		start := w * insertsPerWriter
		end := start + insertsPerWriter
		wg.Add(1)
		go func(s, eidx int) {
			defer wg.Done()
			for j := s; j < eidx; j++ {
				insertVertexWithRetry(t, &ticker, e, logger, table, verts[j])
			}
		}(start, end)
	}
	wg.Wait()

	// Verify all inserts are present after high contention
	require.NoError(t, Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			for i := 0; i < total; i++ {
				v, err := e.SelectVertex(txnID, table, verts[i].SystemID, logger)
				require.NoError(t, err)
				require.Equal(t, int64(i+10), v.Data[field])
			}
			return nil
		},
		false,
	))

	{
		e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, pools, debug)
		require.NoError(t, err)
		defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

		require.NoError(t, Execute(
			&ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
				for i := 0; i < total; i++ {
					v, err := e.SelectVertex(txnID, table, verts[i].SystemID, logger)
					require.NoError(t, err)
					require.Equal(t, int64(i+10), v.Data[field])
				}
				return nil
			},
			false,
		))
	}
}

func TestForbidSelectOnInsertedVertex(t *testing.T) {
	fs := afero.NewOsFs()
	catalogBasePath := t.TempDir()
	pools := uint64(20)
	debug := false

	e, _, _, logger, err := setupExecutor(fs, catalogBasePath, pools, debug)
	require.NoError(t, err)

	ticker := atomic.Uint64{}
	vertTableName := "person"
	vertFieldName := "money"
	edgeTableName := "friend"
	edgeFieldName := "how_long"
	setupTables(
		t,
		e,
		&ticker,
		vertTableName,
		vertFieldName,
		edgeTableName,
		edgeFieldName,
		logger,
	)

	srcVert := storage.VertexInfo{
		SystemID: storage.VertexSystemID(uuid.New()),
		Data: map[string]any{
			vertFieldName: int64(100),
		},
	}
	dstVert := storage.VertexInfo{
		SystemID: storage.VertexSystemID(uuid.New()),
		Data: map[string]any{
			vertFieldName: int64(200),
		},
	}
	edge := storage.EdgeInfo{
		SystemID:    storage.EdgeSystemID(uuid.New()),
		SrcVertexID: srcVert.SystemID,
		DstVertexID: dstVert.SystemID,
		Data: map[string]any{
			edgeFieldName: int64(300),
		},
	}

	insertDoneCh := make(chan struct{})
	go func() {
		require.NoError(t, Execute(
			&ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) error {
				err := e.InsertVertex(txnID, vertTableName, srcVert, logger)
				require.NoError(t, err)

				err = e.InsertVertex(txnID, vertTableName, dstVert, logger)
				require.NoError(t, err)

				t.Logf("inserted vertices: %s, %s", srcVert.SystemID, dstVert.SystemID)
				close(insertDoneCh)
				time.Sleep(time.Hour)
				return nil
			},
			false,
		))
	}()

	time.Sleep(time.Second * 1)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, Execute(
			&ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) error {
				<-insertDoneCh
				t.Logf("selecting src vertex: %s", srcVert.SystemID)
				_, err := e.SelectVertex(txnID, vertTableName, srcVert.SystemID, logger)
				require.ErrorIs(t, err, txns.ErrDeadlockPrevention)
				return nil
			},
			false,
		))
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, Execute(
			&ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) error {
				<-insertDoneCh
				t.Logf("selecting dst vertex: %s", dstVert.SystemID)
				_, err := e.SelectVertex(txnID, vertTableName, dstVert.SystemID, logger)
				require.ErrorIs(t, err, txns.ErrDeadlockPrevention)
				return nil
			},
			false,
		))
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, Execute(
			&ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) error {
				<-insertDoneCh
				t.Logf("inserting edge: %s", edge.SystemID)
				err := e.InsertEdge(txnID, edgeTableName, edge, logger)
				require.ErrorIs(t, err, txns.ErrDeadlockPrevention)
				return nil
			},
			false,
		))
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, Execute(
			&ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) error {
				<-insertDoneCh
				t.Logf("getting vertices on depth: %s", edge.SrcVertexID)
				_, err := e.GetVerticesOnDepth(
					txnID,
					edgeTableName,
					edge.SrcVertexID,
					1,
					storage.AllowAllVerticesFilter,
					logger,
				)
				require.ErrorIs(t, err, txns.ErrDeadlockPrevention)
				return nil
			},
			false,
		))
	}()

	wg.Wait()
}

func TestOlderWaitsAndSucceedsOnEdgeInsert(t *testing.T) {
	fs := afero.NewOsFs()
	catalogBasePath := t.TempDir()
	pools := uint64(20)
	debug := false

	e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, pools, debug)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	var ticker atomic.Uint64
	vertTableName := "person"
	vertFieldName := "money"
	edgeTableName := "friend"
	edgeFieldName := "how_long"
	setupTables(t, e, &ticker, vertTableName, vertFieldName, edgeTableName, edgeFieldName, logger)

	srcVert := storage.VertexInfo{
		SystemID: storage.VertexSystemID(uuid.New()),
		Data:     map[string]any{vertFieldName: int64(10)},
	}
	dstVert := storage.VertexInfo{
		SystemID: storage.VertexSystemID(uuid.New()),
		Data:     map[string]any{vertFieldName: int64(20)},
	}
	require.NoError(t, Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) error {
			require.NoError(t, e.InsertVertex(txnID, vertTableName, srcVert, logger))
			require.NoError(t, e.InsertVertex(txnID, vertTableName, dstVert, logger))
			return nil
		},
		false,
	))

	edgeToInsert := storage.EdgeInfo{
		SystemID:    storage.EdgeSystemID(uuid.New()),
		SrcVertexID: srcVert.SystemID,
		DstVertexID: dstVert.SystemID,
		Data:        map[string]any{edgeFieldName: int64(300)},
	}

	youngerReady := make(chan struct{})
	olderEntered := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(2)

	// Older txn: obtain txnID first, then wait for younger's S-locks and insert edge
	go func() {
		defer wg.Done()
		require.NoError(t, Execute(
			&ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) error {
				// Signal older entered and has smaller txnID
				olderEntered <- struct{}{}
				<-youngerReady
				// Allowed to wait (older waits on younger) and then succeed
				require.NoError(t, e.InsertEdge(txnID, edgeTableName, edgeToInsert, logger))
				return nil
			},
			false,
		))
	}()

	// Ensure older obtained txnID before starting younger
	<-olderEntered

	// Younger txn: acquire S-locks on the edge index (via a SelectEdge) and finish shortly after
	go func() {
		defer wg.Done()
		require.NoError(t, Execute(
			&ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) error {
				_, _ = e.SelectEdge(
					txnID,
					edgeTableName,
					storage.EdgeSystemID(uuid.New()),
					logger,
				) // ensure S-locks on index pages
				close(youngerReady)
				time.Sleep(200 * time.Millisecond)
				return nil
			},
			true,
		))
	}()

	wg.Wait()

	// Verify edge was inserted
	require.NoError(t, Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) error {
			edge, err := e.SelectEdge(txnID, edgeTableName, edgeToInsert.SystemID, logger)
			require.NoError(t, err)
			require.Equal(t, int64(300), edge.Data[edgeFieldName])
			return nil
		},
		true,
	))
}

func TestYoungerInsertEdgeForbiddenByOlderRead(t *testing.T) {
	fs := afero.NewOsFs()
	catalogBasePath := t.TempDir()
	pools := uint64(20)
	debug := false

	e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, pools, debug)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	var ticker atomic.Uint64
	vertTableName := "person"
	vertFieldName := "money"
	edgeTableName := "friend"
	edgeFieldName := "how_long"
	setupTables(t, e, &ticker, vertTableName, vertFieldName, edgeTableName, edgeFieldName, logger)

	srcVert := storage.VertexInfo{
		SystemID: storage.VertexSystemID(uuid.New()),
		Data:     map[string]any{vertFieldName: int64(10)},
	}
	dstVert := storage.VertexInfo{
		SystemID: storage.VertexSystemID(uuid.New()),
		Data:     map[string]any{vertFieldName: int64(20)},
	}
	require.NoError(t, Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) error {
			require.NoError(t, e.InsertVertex(txnID, vertTableName, srcVert, logger))
			require.NoError(t, e.InsertVertex(txnID, vertTableName, dstVert, logger))
			return nil
		},
		false,
	))

	edgeToInsert := storage.EdgeInfo{
		SystemID:    storage.EdgeSystemID(uuid.New()),
		SrcVertexID: srcVert.SystemID,
		DstVertexID: dstVert.SystemID,
		Data:        map[string]any{edgeFieldName: int64(1)},
	}

	olderReady := make(chan struct{})
	holdOlder := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(2)

	// Older txn: take shared locks by scanning neighbors and hold the txn
	go func() {
		defer wg.Done()
		require.NoError(t, Execute(
			&ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) error {
				_, err := e.GetVerticesOnDepth(
					txnID,
					vertTableName,
					srcVert.SystemID,
					1,
					storage.AllowAllVerticesFilter,
					logger,
				)
				require.NoError(t, err)
				close(olderReady)
				<-holdOlder // keep S-locks until the younger tries to insert
				return nil
			},
			false,
		))
	}()

	// Younger txn: try to insert edge; should be aborted due to wait-die
	go func() {
		defer wg.Done()
		<-olderReady
		require.NoError(t, Execute(
			&ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) error {
				err := e.InsertEdge(txnID, edgeTableName, edgeToInsert, logger)
				require.ErrorIs(t, err, txns.ErrDeadlockPrevention)
				return nil
			},
			false,
		))
		close(holdOlder)
	}()

	wg.Wait()

	// Verify that the edge was not inserted
	require.NoError(t, Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) error {
			_, err := e.SelectEdge(txnID, edgeTableName, edgeToInsert.SystemID, logger)
			require.ErrorIs(t, err, storage.ErrKeyNotFound)
			return nil
		},
		true,
	))
}

func TestYoungerSelectEdgeForbiddenByOlderInsert(t *testing.T) {
	fs := afero.NewOsFs()
	catalogBasePath := t.TempDir()
	pools := uint64(20)
	debug := false

	e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, pools, debug)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	var ticker atomic.Uint64
	vertTableName := "person"
	vertFieldName := "money"
	edgeTableName := "friend"
	edgeFieldName := "how_long"
	setupTables(t, e, &ticker, vertTableName, vertFieldName, edgeTableName, edgeFieldName, logger)

	srcVert := storage.VertexInfo{
		SystemID: storage.VertexSystemID(uuid.New()),
		Data:     map[string]any{vertFieldName: int64(10)},
	}
	dstVert := storage.VertexInfo{
		SystemID: storage.VertexSystemID(uuid.New()),
		Data:     map[string]any{vertFieldName: int64(20)},
	}
	require.NoError(t, Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) error {
			require.NoError(t, e.InsertVertex(txnID, vertTableName, srcVert, logger))
			require.NoError(t, e.InsertVertex(txnID, vertTableName, dstVert, logger))
			return nil
		},
		false,
	))

	edgeToInsert := storage.EdgeInfo{
		SystemID:    storage.EdgeSystemID(uuid.New()),
		SrcVertexID: srcVert.SystemID,
		DstVertexID: dstVert.SystemID,
		Data:        map[string]any{edgeFieldName: int64(111)},
	}

	olderHold := make(chan struct{})
	olderReady := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(2)

	// Older txn: insert edge and hold the txn to keep X-locks on index master page
	go func() {
		defer wg.Done()
		require.NoError(t, Execute(
			&ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) error {
				require.NoError(t, e.InsertEdge(txnID, edgeTableName, edgeToInsert, logger))
				close(olderReady)
				<-olderHold
				return nil
			},
			false,
		))
	}()

	// Younger txn: try to select while older holds X-lock; should be aborted (wait-die)
	go func() {
		defer wg.Done()
		<-olderReady
		require.NoError(t, Execute(
			&ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) error {
				_, err := e.SelectEdge(
					txnID,
					edgeTableName,
					storage.EdgeSystemID(uuid.New()),
					logger,
				)
				require.ErrorIs(t, err, txns.ErrDeadlockPrevention)
				return nil
			},
			true,
		))
		close(olderHold)
	}()

	wg.Wait()

	// Verify that the inserted edge is present
	require.NoError(t, Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) error {
			edge, err := e.SelectEdge(txnID, edgeTableName, edgeToInsert.SystemID, logger)
			require.NoError(t, err)
			require.Equal(t, int64(111), edge.Data[edgeFieldName])
			return nil
		},
		true,
	))
}

func TestConcurrentCheckpoint(t *testing.T) {
	fs := afero.NewOsFs()
	catalogBasePath := t.TempDir()
	pools := uint64(200)
	debug := false

	vertTableName := "person"
	vertFieldName := "money"
	edgeTableName := "friend"
	edgeFieldName := "how_long"

	const (
		workersCount = 100
		insertsCount = 100_000
		edgesCount   = 10
	)

	vertMu := sync.RWMutex{}
	insertedVertexIDs := make(map[storage.VertexSystemID]storage.VertexInfo)

	failedVertexIDsMu := sync.RWMutex{}
	failedVertexIDs := make([]storage.VertexSystemID, 0)

	edgesMu := sync.RWMutex{}
	insertedEdgeIDs := make(map[storage.VertexSystemID][]storage.EdgeInfo)

	failedEdgeIDsMu := sync.RWMutex{}
	failedEdgeIDs := make([]storage.EdgeSystemID, 0)

	ensureConsistentDB := func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) error {
		for vertSysID, vertInfo := range insertedVertexIDs {
			storedVert, err := e.SelectVertex(txnID, vertTableName, vertSysID, logger)
			require.NoError(t, err)

			expectedFieldVal := vertInfo.Data[vertFieldName].(int64)
			require.Equal(t, expectedFieldVal, storedVert.Data[vertFieldName].(int64))

			for _, edge := range insertedEdgeIDs[vertSysID] {
				storedEdge, err := e.SelectEdge(txnID, edgeTableName, edge.SystemID, logger)
				require.NoError(t, err)
				require.Equal(
					t,
					edge.Data[edgeFieldName].(int64),
					storedEdge.Data[edgeFieldName].(int64),
				)
			}
		}

		for _, vert := range failedVertexIDs {
			_, err := e.SelectVertex(txnID, vertTableName, vert, logger)
			require.ErrorIs(t, err, storage.ErrKeyNotFound)
		}

		for _, edge := range failedEdgeIDs {
			_, err := e.SelectEdge(txnID, edgeTableName, edge, logger)
			require.ErrorIs(t, err, storage.ErrKeyNotFound)
		}
		return nil
	}

	func() {
		var ticker atomic.Uint64
		e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, pools, debug)
		require.NoError(t, err)
		defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

		setupTables(
			t,
			e,
			&ticker,
			vertTableName,
			vertFieldName,
			edgeTableName,
			edgeFieldName,
			logger,
		)

		workerPool, err := ants.NewPool(workersCount)
		require.NoError(t, err)
		defer workerPool.Release()

		err = Execute(
			&ticker,
			e,
			logger,
			func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) error {
				for i := range edgesCount {
					vert := storage.VertexInfo{
						SystemID: storage.VertexSystemID(uuid.New()),
						Data:     map[string]any{vertFieldName: int64(i)},
					}
					require.NoError(t, e.InsertVertex(txnID, vertTableName, vert, logger))
					insertedVertexIDs[vert.SystemID] = vert
				}
				return nil
			},
			false,
		)
		require.NoError(t, err)

		wg := sync.WaitGroup{}
		for i := edgesCount; i < insertsCount+edgesCount; i++ {
			wg.Add(1)
			j := i
			require.NoError(t, workerPool.Submit(func() {
				defer wg.Done()
				vertex := storage.VertexInfo{
					SystemID: storage.VertexSystemID(uuid.New()),
					Data:     map[string]any{vertFieldName: int64(j)},
				}

				edgeIDs := make([]storage.EdgeSystemID, 0, edgesCount)
				for range edgesCount {
					edgeIDs = append(edgeIDs, storage.EdgeSystemID(uuid.New()))
				}
				insertedEdges := make([]storage.EdgeInfo, 0, edgesCount)

				err := Execute(
					&ticker,
					e,
					logger,
					func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) error {
						err := e.InsertVertex(txnID, vertTableName, vertex, logger)
						if err != nil {
							return err
						}

						c := 0
						vertMu.RLock()
						for dst := range insertedVertexIDs {
							edgeInfo := storage.EdgeInfo{
								SystemID:    edgeIDs[c],
								SrcVertexID: dst,
								DstVertexID: vertex.SystemID,
								Data:        map[string]any{edgeFieldName: int64(j + c)},
							}
							insertedEdges = append(insertedEdges, edgeInfo)
							c++
							if c == edgesCount {
								break
							}
						}
						vertMu.RUnlock()
						require.Equal(t, edgesCount, c)

						err = e.InsertEdges(txnID, edgeTableName, insertedEdges, logger)
						return err
					},
					false,
				)
				if errors.Is(err, txns.ErrDeadlockPrevention) {
					failedEdgeIDsMu.Lock()
					failedEdgeIDs = append(failedEdgeIDs, edgeIDs...)
					failedEdgeIDsMu.Unlock()

					failedVertexIDsMu.Lock()
					failedVertexIDs = append(failedVertexIDs, vertex.SystemID)
					failedVertexIDsMu.Unlock()
					return
				}
				require.NoError(t, err)

				vertMu.Lock()
				insertedVertexIDs[vertex.SystemID] = vertex
				vertMu.Unlock()

				edgesMu.Lock()
				insertedEdgeIDs[vertex.SystemID] = insertedEdges
				edgesMu.Unlock()
			}))

			if i == insertsCount/2 {
				wg.Add(1)
				require.NoError(t, workerPool.Submit(func() {
					defer wg.Done()
					require.NoError(t, pool.FlushAllPages())
				}))
			}
		}
		wg.Wait()

		err = Execute(
			&ticker,
			e,
			logger,
			ensureConsistentDB,
			true,
		)
		require.NoError(t, err)
	}()

	func() {
		e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, pools, debug)
		require.NoError(t, err)
		defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

		ticker := atomic.Uint64{}

		require.NoError(t, Execute(
			&ticker,
			e,
			logger,
			ensureConsistentDB,
			true,
		))
	}()
}

func insertNotOrientedEdgeWithRetry(
	t testing.TB,
	ticker *atomic.Uint64,
	e *Executor,
	logger common.ITxnLogger,
	tableName string,
	edge storage.EdgeInfo,
) {
	err := ExecuteWithRetry(
		ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
			err = e.InsertEdge(txnID, tableName, edge, logger)
			if err != nil {
				if errors.Is(err, storage.ErrKeyNotFound) {
					return ErrRollback
				}

				return err
			}
			err = e.InsertEdge(txnID, tableName, storage.EdgeInfo{
				SystemID:    storage.EdgeSystemID(uuid.New()),
				SrcVertexID: edge.DstVertexID,
				DstVertexID: edge.SrcVertexID,
				Data:        edge.Data,
			}, logger)
			if errors.Is(err, storage.ErrKeyNotFound) {
				return ErrRollback
			}
			return err
		},
	)
	if err != nil {
		require.ErrorIs(t, err, ErrRollback)
	}
}

func TestConcurrentGetTriangles(t *testing.T) {
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
			vertexCount:  10,
			connectivity: 1.0,
		},
	}

	fs := afero.NewMemMapFs()
	catalogBasePath := "/tmp/graphdb_test"
	e, pool, _, logger, err := setupExecutor(fs, catalogBasePath, 20, true)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	ticker := atomic.Uint64{}
	vertTableName := "person"
	verticesFieldName := "money"
	edgeTableName := "indepted_to"
	edgesFieldName := "debt_amount"

	threads := 20

	for _, test := range tests {
		t.Run(
			fmt.Sprintf("vertexCount=%d,connectivity=%f", test.vertexCount, test.connectivity),
			func(t *testing.T) {
				setupTables(
					t,
					e,
					&ticker,
					vertTableName,
					verticesFieldName,
					edgeTableName,
					edgesFieldName,
					logger,
				)

				graphInfo := generateRandomGraph(
					t,
					test.vertexCount,
					test.connectivity,
					rand.New(rand.NewSource(42)),
					true,
				)
				expectedTriangles := graphCountTriangles(graphInfo.g)

				intToVertSystemID, edgesSystemInfo := instantiateGraph(
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
					edgeTableName,
					edgesFieldName,
					intToVertSystemID,
					edgesSystemInfo,
					1,
					1,
				)

				var wg sync.WaitGroup

				wg.Add(threads)

				for range threads {
					go func() {
						defer wg.Done()

						err := Execute(
							&ticker,
							e,
							logger,
							func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
								triangles, err := e.GetAllTriangles(txnID, vertTableName, logger)
								require.NoError(t, err)
								if !assert.Equal(t, uint64(len(expectedTriangles)), triangles) {
									t.Logf(
										"Graph: \n%s\nExpected triangles %v",
										graphInfo.GraphVizRepr(),
										expectedTriangles,
									)
									t.FailNow()
								}
								return nil
							},
							true,
						)
						require.NoError(t, err)
					}()
				}

				wg.Wait()

				err = Execute(
					&ticker,
					e,
					logger,
					func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
						require.NoError(t, e.DropVertexTable(txnID, vertTableName, logger))
						require.NoError(t, e.DropEdgeTable(txnID, edgeTableName, logger))
						return nil
					},
					false,
				)
				require.NoError(t, err)
			},
		)
	}
}

const (
	vertexType = 1
	edgeType   = 2
)

type GraphGenerator struct {
	operations    []op
	vertexCount   int
	edges         map[undirectedEdge]struct{}
	vertexIDs     []storage.VertexSystemID
	edgeProb      int
	vertFieldName string
	edgeFieldName string
}

type op struct {
	t int
	v storage.VertexInfo
	e storage.EdgeInfo
}

type undirectedEdge struct {
	a storage.VertexSystemID
	b storage.VertexSystemID
}

func makeUndirectedEdge(u, v storage.VertexSystemID) undirectedEdge {
	if bytes.Compare(u[:], v[:]) < 0 {
		return undirectedEdge{a: u, b: v}
	}

	return undirectedEdge{a: v, b: u}
}

func NewGraphGenerator(edgeProb int, vertFieldName, edgeFieldName string) *GraphGenerator {
	return &GraphGenerator{
		edges:         make(map[undirectedEdge]struct{}),
		edgeProb:      edgeProb,
		vertFieldName: vertFieldName,
		edgeFieldName: edgeFieldName,
	}
}

func (g *GraphGenerator) Generate(operationsCount, minTriangles int) []op {
	for i := 0; i < operationsCount; i++ {
		generated := false
		for !generated {
			var tp int
			if g.vertexCount < 2 {
				tp = vertexType
			} else {
				r := rand.Intn(100)
				if r < g.edgeProb && len(g.vertexIDs) >= 2 {
					tp = edgeType
				} else {
					tp = vertexType
				}
			}

			if tp == vertexType {
				g.addVertex()
				generated = true
			} else {
				if len(g.vertexIDs) < 2 {
					continue
				}

				srcIdx := rand.Intn(len(g.vertexIDs))
				dstIdx := rand.Intn(len(g.vertexIDs))
				for srcIdx == dstIdx {
					dstIdx = rand.Intn(len(g.vertexIDs))
				}

				srcID := g.vertexIDs[srcIdx]
				dstID := g.vertexIDs[dstIdx]

				ue := makeUndirectedEdge(srcID, dstID)
				if _, exists := g.edges[ue]; exists {
					continue
				}

				g.edges[ue] = struct{}{}

				g.addEdge(srcID, dstID)
				generated = true
			}
		}
	}

	g.ensureTriangles(minTriangles)

	return g.operations
}

func (g *GraphGenerator) sortOperations() {
	var vertexOps, edgeOps []op

	for _, o := range g.operations {
		if o.t == vertexType {
			vertexOps = append(vertexOps, o)
		} else {
			edgeOps = append(edgeOps, o)
		}
	}

	g.operations = append(vertexOps, edgeOps...)
}

func (g *GraphGenerator) addVertex() {
	vertexID := storage.VertexSystemID(uuid.New())
	g.vertexCount++

	newOp := op{
		t: vertexType,
		v: storage.VertexInfo{
			SystemID: vertexID,
			Data: map[string]any{
				g.vertFieldName: int64(g.vertexCount),
			},
		},
	}

	g.operations = append(g.operations, newOp)
	g.vertexIDs = append(g.vertexIDs, vertexID)
}

func (g *GraphGenerator) addEdge(src, dst storage.VertexSystemID) {
	edgeID := storage.EdgeSystemID(uuid.New())
	newOp := op{
		t: edgeType,
		e: storage.EdgeInfo{
			SystemID:    edgeID,
			SrcVertexID: src,
			DstVertexID: dst,
			Data: map[string]any{
				g.edgeFieldName: int64(rand.Intn(10) + 1),
			},
		},
	}

	g.operations = append(g.operations, newOp)
}

func (g *GraphGenerator) ensureTriangles(minTriangles int) {
	triangles := g.countTriangles()

	for triangles < minTriangles {
		if len(g.vertexIDs) < 3 {
			for len(g.vertexIDs) < 3 {
				g.addVertex()
			}
		}

		indices := rand.Perm(len(g.vertexIDs))[:3]
		a := g.vertexIDs[indices[0]]
		b := g.vertexIDs[indices[1]]
		c := g.vertexIDs[indices[2]]

		edgesToCreate := []undirectedEdge{
			makeUndirectedEdge(a, b),
			makeUndirectedEdge(a, c),
			makeUndirectedEdge(b, c),
		}

		fullTriangleExists := true
		for _, e := range edgesToCreate {
			if _, exists := g.edges[e]; !exists {
				fullTriangleExists = false
				break
			}
		}

		if fullTriangleExists {
			continue
		}

		for _, e := range edgesToCreate {
			if _, exists := g.edges[e]; !exists {
				g.edges[e] = struct{}{}

				if bytes.Compare(e.a[:], e.b[:]) < 0 {
					g.addEdge(e.a, e.b)
				} else {
					g.addEdge(e.b, e.a)
				}
			}
		}

		triangles = g.countTriangles()
	}
}

func (g *GraphGenerator) countTriangles() int {
	count := 0
	n := len(g.vertexIDs)
	if n < 3 {
		return 0
	}

	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			for k := j + 1; k < n; k++ {
				a := g.vertexIDs[i]
				b := g.vertexIDs[j]
				c := g.vertexIDs[k]

				if g.hasEdge(a, b) && g.hasEdge(a, c) && g.hasEdge(b, c) {
					count++
				}
			}
		}
	}

	return count
}

func (g *GraphGenerator) hasEdge(u, v storage.VertexSystemID) bool {
	if bytes.Compare(u[:], v[:]) < 0 {
		_, exists := g.edges[makeUndirectedEdge(u, v)]
		return exists
	}

	return false
}

func (g *GraphGenerator) getVertexIndex(systemID storage.VertexSystemID) int {
	for i, id := range g.vertexIDs {
		if id == systemID {
			return i
		}
	}
	return -1
}

func TestConcurrentGetTrianglesWithWrites(t *testing.T) {
	fs := afero.NewMemMapFs()
	catalogBasePath := "/tmp/graphdb_concurrent_get_triangles_with_write"
	poolPageCount := uint64(50)
	debugMode := false

	e, debugPool, _, logger, err := setupExecutor(fs, catalogBasePath, poolPageCount, debugMode)
	require.NoError(t, err)
	defer func() { require.NoError(t, debugPool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	var ticker atomic.Uint64
	vertTableName := "person"
	vertFieldName := "id"
	edgeTableName := "is-friend"
	edgeFieldName := "years"

	tests := []struct {
		threadsCount   int
		opsCnt         int
		minTriangleCnt int
	}{
		{
			threadsCount:   10,
			opsCnt:         400,
			minTriangleCnt: 20,
		},
		{
			threadsCount:   20,
			opsCnt:         200,
			minTriangleCnt: 30,
		},
		{
			threadsCount:   15,
			opsCnt:         300,
			minTriangleCnt: 257,
		},
	}

	for i, test := range tests {
		t.Run(
			fmt.Sprintf("test %d, threads=%d, ops=%d, minTrianglesCount=%d",
				i, test.threadsCount, test.opsCnt, test.minTriangleCnt),
			func(t *testing.T) {
				// Create the vertex table
				err = Execute(
					&ticker,
					e,
					logger,
					func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
						schema := storage.Schema{
							{Name: vertFieldName, Type: storage.ColumnTypeInt64},
						}
						return e.CreateVertexType(txnID, vertTableName, schema, logger)
					},
					false,
				)
				require.NoError(t, err)

				// Create the edges table
				err = Execute(
					&ticker,
					e,
					logger,
					func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
						schema := storage.Schema{
							{Name: edgeFieldName, Type: storage.ColumnTypeInt64},
						}
						return e.CreateEdgeType(
							txnID,
							edgeTableName,
							schema,
							vertTableName,
							vertTableName,
							logger,
						)
					},
					false,
				)
				require.NoError(t, err)

				// generate operations
				opGen := NewGraphGenerator(60, vertFieldName, edgeFieldName)
				opGen.Generate(test.opsCnt, test.minTriangleCnt)

				var (
					checkInterval = test.opsCnt / 4

					g      = make(map[int][]int)
					opChan = make(chan op, test.threadsCount)

					mu sync.RWMutex
					wg sync.WaitGroup
				)

				wg.Add(test.threadsCount)

				for range test.threadsCount {
					go func() {
						defer wg.Done()

						for o := range opChan {
							if o.t == vertexType {
								mu.Lock()
								insertVertexWithRetry(t, &ticker, e, logger, vertTableName, o.v)
								d := opGen.getVertexIndex(o.v.SystemID)
								g[d] = make([]int, 0)
								mu.Unlock()
							} else {
								mu.Lock()
								insertNotOrientedEdgeWithRetry(t, &ticker, e, logger, edgeTableName, o.e)
								u, v := opGen.getVertexIndex(o.e.SrcVertexID), opGen.getVertexIndex(o.e.DstVertexID)
								g[u] = append(g[u], v)
								g[v] = append(g[v], u)
								mu.Unlock()
							}
						}
					}()
				}

				operationCounter := 0

				for _, o := range opGen.operations {
					opChan <- o
					operationCounter++

					if operationCounter%checkInterval == 0 {
						err = ExecuteWithRetry(
							&ticker,
							e,
							logger,
							func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
								mu.RLock()
								defer mu.RUnlock()
								triangles, err := e.GetAllTriangles(txnID, vertTableName, logger)
								if err != nil {
									return err
								}
								trCnt := uint64(len(graphCountTriangles(g)))
								require.Equal(t, trCnt, triangles)

								return nil
							},
						)
						require.NoError(t, err)
					}
				}

				close(opChan)
				wg.Wait()

				mu.RLock()
				trCnt := uint64(len(graphCountTriangles(g)))
				mu.RUnlock()

				err = Execute(
					&ticker,
					e,
					logger,
					func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
						triangles, err := e.GetAllTriangles(txnID, vertTableName, logger)
						require.NoError(t, err)
						require.Equal(t, trCnt, triangles)
						slog.Info("final check", "db", triangles, "inmemory", trCnt)
						return nil
					},
					false,
				)
				require.NoError(t, err)

				err = Execute(
					&ticker,
					e,
					logger,
					func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error) {
						require.NoError(t, e.DropVertexTable(txnID, vertTableName, logger))
						require.NoError(t, e.DropEdgeTable(txnID, edgeTableName, logger))
						return nil
					},
					false,
				)
				require.NoError(t, err)
			})
	}
}

func ExecuteWithRetry(
	ticker *atomic.Uint64,
	executor *Executor,
	logger common.ITxnLogger,
	fn Task,
) error {
	txnID := common.TxnID(ticker.Add(1))

	job := func() error {
		defer executor.Locker.Unlock(txnID)
		ctxLogger := logger.WithContext(txnID)

		if err := ctxLogger.AppendBegin(); err != nil {
			return fmt.Errorf("failed to append begin: %w", err)
		}

		err := fn(txnID, executor, ctxLogger)

		if err != nil {
			myassert.NoError(ctxLogger.AppendAbort())
			ctxLogger.Rollback()
			return err
		}
		if err = ctxLogger.AppendCommit(); err != nil {
			return fmt.Errorf("failed to append commit: %w", err)
		} else if err = ctxLogger.AppendTxnEnd(); err != nil {
			return fmt.Errorf("failed to append txn end: %w", err)
		}
		return nil
	}

	for {
		err := job()
		if errors.Is(err, txns.ErrDeadlockPrevention) {
			continue
		}
		return err
	}
}

func TestRepeatableRead(t *testing.T) {
	fs := afero.NewOsFs()
	catalogBasePath := t.TempDir()
	poolPageCount := uint64(50)
	debugMode := false

	e, debugPool, locker, logger, err := setupExecutor(
		fs,
		catalogBasePath,
		poolPageCount,
		debugMode,
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, debugPool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	var ticker atomic.Uint64
	vertTableName := "person"
	vertFieldName := "id"
	edgeTableName := "is-friend"
	edgeFieldName := "years"
	setupTables(t, e, &ticker, vertTableName, vertFieldName, edgeTableName, edgeFieldName, logger)

	const (
		jobsCount      = 100
		workerPoolSize = 20
	)

	pool, err := ants.NewPool(workerPoolSize)
	require.NoError(t, err)
	defer pool.Release()

	vertex := storage.VertexInfo{
		SystemID: storage.VertexSystemID(uuid.New()),
		Data:     map[string]any{vertFieldName: int64(0)},
	}

	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) error {
			err := e.InsertVertex(txnID, vertTableName, vertex, logger)
			require.NoError(t, err)
			return nil
		},
		false,
	)
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(jobsCount)
	for range jobsCount {
		err := pool.Submit(func() {
			defer wg.Done()
			err := ExecuteWithRetry(
				&ticker,
				e,
				logger,
				func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) error {
					dbVert, err := e.SelectVertex(txnID, vertTableName, vertex.SystemID, logger)
					if err != nil {
						return err
					}
					oldValue := dbVert.Data[vertFieldName].(int64)

					newData := map[string]any{vertFieldName: int64(oldValue + 1)}
					err = e.UpdateVertex(txnID, vertTableName, vertex.SystemID, newData, logger)
					if err != nil {
						return err
					}

					updatedDbVert, err := e.SelectVertex(
						txnID,
						vertTableName,
						vertex.SystemID,
						logger,
					)
					if err != nil {
						return err
					}
					require.Equal(t, newData[vertFieldName], updatedDbVert.Data[vertFieldName])
					return nil
				},
			)
			require.NoError(t, err)
		})
		require.NoError(t, err)
	}

	time.AfterFunc(time.Second*10, func() {
		t.Logf("Dependency graph:\n%s", locker.DumpDependencyGraph())
	})
	wg.Wait()

	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) error {
			vert, err := e.SelectVertex(txnID, vertTableName, vertex.SystemID, logger)
			require.NoError(t, err)
			require.Equal(t, int64(jobsCount), vert.Data[vertFieldName].(int64))
			return nil
		},
		true,
	)
	require.NoError(t, err)
}

func TestBankTransactions(t *testing.T) {
	fs := afero.NewOsFs()
	catalogBasePath := t.TempDir()
	poolPageCount := uint64(50)
	debugMode := false

	e, debugPool, locker, logger, err := setupExecutor(
		fs,
		catalogBasePath,
		poolPageCount,
		debugMode,
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, debugPool.EnsureAllPagesUnpinnedAndUnlocked()) }()
	defer func() { require.True(t, locker.AreAllQueuesEmpty()) }()

	ticker := atomic.Uint64{}
	vertTableName := "person"
	balanceField := "balance"
	vertSchema := storage.Schema{
		{Name: balanceField, Type: storage.ColumnTypeInt64},
	}

	edgeTableName := "transfer"
	amountField := "amount"
	timestampField := "timestamp"
	edgeSchema := storage.Schema{
		{Name: amountField, Type: storage.ColumnTypeInt64},
		{Name: timestampField, Type: storage.ColumnTypeInt64},
	}

	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) error {
			err := e.CreateVertexType(txnID, vertTableName, vertSchema, logger)
			require.NoError(t, err)
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
		false,
	)
	require.NoError(t, err)

	const (
		nClients       = 1000
		nTransactions  = 1000
		initialBalance = 100
		workerPoolSize = 10
		totalMoney     = nClients * initialBalance
	)

	vertices := make([]storage.VertexInfo, 0, nClients)
	for range nClients {
		vertices = append(vertices, storage.VertexInfo{
			SystemID: storage.VertexSystemID(uuid.New()),
			Data:     map[string]any{balanceField: int64(initialBalance)},
		})
	}

	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) error {
			return e.InsertVertices(txnID, vertTableName, vertices, logger)
		},
		false,
	)
	require.NoError(t, err)

	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) error {
			s := int64(0)
			for _, vert := range vertices {
				vert, err := e.SelectVertex(txnID, vertTableName, vert.SystemID, logger)
				if err != nil {
					return err
				}
				s += vert.Data[balanceField].(int64)
			}
			require.Equal(t, int64(totalMoney), s)
			return nil
		},
		true,
	)
	require.NoError(t, err)

	workerPool, err := ants.NewPool(workerPoolSize)
	require.NoError(t, err)
	defer workerPool.Release()

	wg := sync.WaitGroup{}
	wg.Add(nClients)
	for range nClients {
		err := workerPool.Submit(func() {
			defer wg.Done()

			err := ExecuteWithRetry(
				&ticker,
				e,
				logger,
				func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) error {
					src := rand.Intn(nClients)
					dst := rand.Intn(nClients)
					for dst == src {
						dst = rand.Intn(nClients)
					}

					srcVertID := vertices[src].SystemID
					dstVertID := vertices[dst].SystemID

					log.Printf(
						"txnID: %d - Starting transfer from vertex %d to vertex %d",
						txnID,
						src,
						dst,
					)

					srcVert, err := e.SelectVertex(txnID, vertTableName, srcVertID, logger)
					if err != nil {
						log.Printf("txnID: %d - Failed to select source vertex: %v", txnID, err)
						return err
					}
					dstVert, err := e.SelectVertex(txnID, vertTableName, dstVertID, logger)
					if err != nil {
						log.Printf(
							"txnID: %d - Failed to select destination vertex: %v",
							txnID,
							err,
						)
						return err
					}

					srcBalance := srcVert.Data[balanceField].(int64)
					if srcBalance == 0 {
						log.Printf(
							"txnID: %d - Source vertex %d has zero balance, skipping transfer",
							txnID,
							src,
						)
						return nil
					}
					dstBalance := dstVert.Data[balanceField].(int64)

					transferAmount := int64(rand.Intn(int(srcBalance)))
					transferTimestamp := time.Now().Unix()
					log.Printf(
						"txnID: %d - Transferring amount %d from vertex %d (balance: %d) to vertex %d (balance: %d)",
						txnID,
						transferAmount,
						src,
						srcBalance,
						dst,
						dstBalance,
					)

					transferEdge := storage.EdgeInfo{
						SystemID:    storage.EdgeSystemID(uuid.New()),
						SrcVertexID: srcVertID,
						DstVertexID: dstVertID,
						Data: map[string]any{
							amountField:    int64(transferAmount),
							timestampField: int64(transferTimestamp),
						},
					}

					err = e.UpdateVertex(
						txnID,
						vertTableName,
						srcVertID,
						map[string]any{
							balanceField: int64(srcBalance - transferAmount),
						},
						logger,
					)
					if err != nil {
						log.Printf(
							"txnID: %d - Failed to update source vertex balance: %v",
							txnID,
							err,
						)
						return err
					}

					err = e.UpdateVertex(
						txnID,
						vertTableName,
						dstVertID,
						map[string]any{
							balanceField: int64(dstBalance + transferAmount),
						},
						logger,
					)
					if err != nil {
						log.Printf(
							"txnID: %d - Failed to update destination vertex balance: %v",
							txnID,
							err,
						)
						return err
					}

					err = e.InsertEdge(txnID, edgeTableName, transferEdge, logger)
					if err != nil {
						log.Printf("txnID: %d - Failed to insert transfer edge: %v", txnID, err)
						return err
					}

					log.Printf(
						"txnID: %d - Successfully completed transfer of amount %d from vertex %d to vertex %d",
						txnID,
						transferAmount,
						src,
						dst,
					)
					return nil
				},
			)
			require.NoError(t, err)
		})
		require.NoError(t, err)
	}
	wg.Wait()

	err = Execute(
		&ticker,
		e,
		logger,
		func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) error {
			s := int64(0)
			for _, vert := range vertices {
				vert, err := e.SelectVertex(txnID, vertTableName, vert.SystemID, logger)
				if err != nil {
					return err
				}
				s += vert.Data[balanceField].(int64)
			}
			require.Equal(t, int64(totalMoney), s)
			return nil
		},
		true,
	)
	require.NoError(t, err)
}
