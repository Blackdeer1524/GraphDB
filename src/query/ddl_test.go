package query

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/recovery"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/storage/disk"
	"github.com/Blackdeer1524/GraphDB/src/storage/engine"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
	"github.com/Blackdeer1524/GraphDB/src/storage/systemcatalog"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

func setupExecutor(poolPageCount uint64) (*Executor, error) {
	catalogBasePath := "/tmp/graphdb_test"
	fs := afero.NewMemMapFs()
	err := systemcatalog.InitSystemCatalog(catalogBasePath, fs)
	if err != nil {
		return nil, err
	}

	err = systemcatalog.CreateLogFileIfDoesntExist(catalogBasePath, fs)
	if err != nil {
		return nil, err
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
	executor := New(se, locker, logger)
	return executor, nil
}

func TestCreateVertexType(t *testing.T) {
	e, err := setupExecutor(10)
	require.NoError(t, err)

	tableName := "test"
	schema := storage.Schema{
		{Name: "money", Type: storage.ColumnTypeInt64},
	}
	err = e.CreateVertexType(tableName, schema)
	require.NoError(t, err)

	vid, err := e.InsertVertex(tableName, map[string]any{
		"money": 100,
	})
	require.NoError(t, err)

}
