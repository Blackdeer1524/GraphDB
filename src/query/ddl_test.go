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

func TestCreateVertexType(t *testing.T) {
	e, logger, err := setupExecutor(10)
	require.NoError(t, err)

	ticket := atomic.Uint64{}

	err = func(txnID common.TxnID) (err error) {
		logger := logger.WithContext(txnID)

		if err := logger.AppendBegin(); err != nil {
			return fmt.Errorf("failed to append begin: %w", err)
		}

		defer func() {
			if err != nil {
				assert.NoError(logger.AppendAbort())
				logger.Rollback()
				err = errors.Join(err, logger.AppendTxnEnd())
				return
			}
			if err = logger.AppendCommit(); err != nil {
				err = fmt.Errorf("failed to append commit: %w", err)
			} else if err = logger.AppendTxnEnd(); err != nil {
				err = fmt.Errorf("failed to append txn end: %w", err)
			}
		}()

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

		require.NoError(t, logger.AppendCommit())
		return nil
	}(common.TxnID(ticket.Add(1)))

	require.NoError(t, err)
}
