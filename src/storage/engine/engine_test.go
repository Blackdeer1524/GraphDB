package engine

import (
	"os"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/storage/systemcatalog"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

func TestStorageEngine_CreateVertexTable(t *testing.T) {
	dir := t.TempDir()

	err := systemcatalog.InitSystemCatalog(dir, afero.NewOsFs())
	require.NoError(t, err)

	lockMgr := txns.NewLockManager()

	var se *StorageEngine

	se, err = New(
		dir,
		uint64(200),
		lockMgr,
		afero.NewOsFs(),
		func(indexMeta storage.IndexMeta, locker *txns.LockManager, logger common.ITxnLoggerWithContext) (storage.Index, error) {
			return nil, nil
		},
	)
	require.NoError(t, err)

	tableName := "User"
	schema := storage.Schema{
		{Name: "id", Type: storage.ColumnTypeInt64},
		{Name: "name", Type: storage.ColumnTypeUUID},
	}

	func() {
		firstTxnID := common.TxnID(1)
		cToken := txns.NewNilCatalogLockToken(firstTxnID)

		defer lockMgr.Unlock(firstTxnID)
		err = se.CreateVertexTable(firstTxnID, tableName, schema, cToken, common.NoLogs())
		require.NoError(t, err)

		tablePath := GetVertexTableFilePath(dir, tableName)
		info, err := os.Stat(tablePath)
		require.NoError(t, err)

		require.False(t, info.IsDir())

		tblMeta, err := se.catalog.GetVertexTableMeta(tableName)
		require.NoError(t, err)
		require.Equal(t, tableName, tblMeta.Name)

		require.Equal(t, uint64(1), se.catalog.CurrentVersion())
	}()

	func() {
		secondTxnID := common.TxnID(2)
		cToken := txns.NewNilCatalogLockToken(secondTxnID)
		defer lockMgr.Unlock(secondTxnID)

		err = se.CreateVertexTable(secondTxnID, tableName, schema, cToken, common.NoLogs())
		require.Error(t, err)
	}()
}

func TestStorageEngine_DropVertexTable(t *testing.T) {
	dir := t.TempDir()

	err := systemcatalog.InitSystemCatalog(dir, afero.NewOsFs())
	require.NoError(t, err)

	var se *StorageEngine

	lockMgr := txns.NewLockManager()
	se, err = New(
		dir,
		uint64(200),
		lockMgr,
		afero.NewOsFs(),
		func(indexMeta storage.IndexMeta, locker *txns.LockManager, logger common.ITxnLoggerWithContext) (storage.Index, error) {
			return nil, nil
		},
	)
	require.NoError(t, err)

	tableName := "User"
	schema := storage.Schema{
		{Name: "id", Type: storage.ColumnTypeInt64},
		{Name: "name", Type: storage.ColumnTypeUUID},
	}

	func() {
		firstTxnID := common.TxnID(1)
		defer lockMgr.Unlock(firstTxnID)

		cToken := txns.NewNilCatalogLockToken(firstTxnID)

		err = se.CreateVertexTable(firstTxnID, tableName, schema, cToken, common.NoLogs())
		require.NoError(t, err)

		tablePath := GetVertexTableFilePath(dir, tableName)
		info, err := os.Stat(tablePath)
		require.NoError(t, err)

		require.False(t, info.IsDir())

		err = se.DropVertexTable(firstTxnID, tableName, cToken, common.NoLogs())
		require.NoError(t, err)

		_, err = os.Stat(tablePath)
		require.NoError(t, err)

		err = se.DropVertexTable(firstTxnID, tableName, cToken, common.NoLogs())
		require.Error(t, err)

		require.Equal(t, uint64(2), se.catalog.CurrentVersion())
	}()

	func() {
		secondTxnID := common.TxnID(2)
		defer lockMgr.Unlock(secondTxnID)

		err = se.CreateVertexTable(secondTxnID, tableName, schema, common.NoLogs())
		require.NoError(t, err)

		tablePath := getTableFilePath(dir, getVertexTableName(tableName))
		_, err := os.Stat(tablePath)
		require.NoError(t, err)
	}()
}

func TestStorageEngine_CreateEdgeTable(t *testing.T) {
	dir := t.TempDir()

	err := systemcatalog.InitSystemCatalog(dir, afero.NewOsFs())
	require.NoError(t, err)

	var se *StorageEngine

	lockMgr := txns.NewLockManager()
	se, err = New(
		dir,
		uint64(200),
		lockMgr,
		afero.NewOsFs(),
		func(indexMeta storage.IndexMeta, locker *txns.LockManager, logger common.ITxnLoggerWithContext) (storage.Index, error) {
			return nil, nil
		},
	)
	require.NoError(t, err)

	tableName := "IsFriendWith"
	schema := storage.Schema{
		{Name: "from", Type: storage.ColumnTypeInt64},
		{Name: "to", Type: storage.ColumnTypeInt64},
	}

	func() {
		firstTxnID := common.TxnID(1)
		defer lockMgr.Unlock(firstTxnID)

		err = se.CreateEdgeTable(firstTxnID, tableName, schema, common.NoLogs())
		require.NoError(t, err)

		tablePath := getTableFilePath(dir, getEdgeTableName(tableName))
		info, err := os.Stat(tablePath)
		require.NoError(t, err)

		require.False(t, info.IsDir())

		tblMeta, err := se.catalog.GetTableMeta(getEdgeTableName(tableName))
		require.NoError(t, err)
		require.Equal(t, tableName, tblMeta.Name)

		require.Greater(t, se.catalog.CurrentVersion(), uint64(0))
	}()

	func() {
		secondTxnID := common.TxnID(2)
		defer lockMgr.Unlock(secondTxnID)

		err = se.CreateEdgeTable(secondTxnID, tableName, schema, common.NoLogs())
		require.Error(t, err)
	}()
}

func TestStorageEngine_DropEdgesTable(t *testing.T) {
	dir := t.TempDir()

	err := systemcatalog.InitSystemCatalog(dir, afero.NewOsFs())
	require.NoError(t, err)

	var se *StorageEngine

	lockMgr := txns.NewLockManager()
	se, err = New(
		dir,
		uint64(200),
		lockMgr,
		afero.NewOsFs(),
		func(indexMeta storage.IndexMeta, locker *txns.LockManager, logger common.ITxnLoggerWithContext) (storage.Index, error) {
			return nil, nil
		},
	)
	require.NoError(t, err)

	tableName := "IsFriendWith"
	schema := storage.Schema{
		{Name: "from", Type: storage.ColumnTypeInt64},
		{Name: "to", Type: storage.ColumnTypeInt64},
	}

	func() {
		firstTxnID := common.TxnID(1)
		defer lockMgr.Unlock(firstTxnID)

		err = se.CreateEdgeTable(firstTxnID, tableName, schema, common.NoLogs())
		require.NoError(t, err)

		tablePath := getTableFilePath(dir, getEdgeTableName(tableName))
		info, err := os.Stat(tablePath)
		require.NoError(t, err)

		require.False(t, info.IsDir())

		err = se.DropEdgeTable(firstTxnID, tableName, common.NoLogs())
		require.NoError(t, err)

		_, err = os.Stat(tablePath)
		require.NoError(t, err)

		err = se.DropEdgeTable(firstTxnID, tableName, common.NoLogs())
		require.Error(t, err)

		require.Equal(t, uint64(2), se.catalog.CurrentVersion())
	}()

	func() {
		secondTxnID := common.TxnID(2)
		defer lockMgr.Unlock(secondTxnID)

		err = se.CreateEdgeTable(secondTxnID, tableName, schema, common.NoLogs())
		require.NoError(t, err)

		tablePath := getTableFilePath(dir, getEdgeTableName(tableName))
		_, err := os.Stat(tablePath)
		require.NoError(t, err)
	}()
}

func TestStorageEngine_CreateIndex(t *testing.T) {
	dir := t.TempDir()

	err := systemcatalog.InitSystemCatalog(dir, afero.NewOsFs())
	require.NoError(t, err)

	var se *StorageEngine

	lockMgr := txns.NewLockManager()
	se, err = New(
		dir,
		uint64(200),
		lockMgr,
		afero.NewOsFs(),
		func(indexMeta storage.IndexMeta, locker *txns.LockManager, logger common.ITxnLoggerWithContext) (storage.Index, error) {
			return nil, nil
		},
	)
	require.NoError(t, err)

	tableName := "User"
	schema := storage.Schema{
		{Name: "id", Type: storage.ColumnTypeInt64},
		{Name: "name", Type: storage.ColumnTypeUUID},
	}

	firstTxnID := common.TxnID(1)
	defer lockMgr.Unlock(firstTxnID)

	err = se.CreateVertexTable(firstTxnID, tableName, schema, common.NoLogs())
	require.NoError(t, err)

	indexName := "user_name"
	err = se.CreateVertexTableIndex(
		firstTxnID,
		indexName,
		tableName,
		[]string{"name"},
		8,
		common.NoLogs(),
	)
	require.NoError(t, err)

	tablePath := GetVertexIndexFilePath(dir, indexName)
	_, err = os.Stat(tablePath)
	require.NoError(t, err)

	_, err = se.catalog.GetIndexMeta(GetVertexIndexName(indexName))
	require.NoError(t, err)
}

func TestStorageEngine_DropIndex(t *testing.T) {
	dir := t.TempDir()

	err := systemcatalog.InitSystemCatalog(dir, afero.NewOsFs())
	require.NoError(t, err)

	var se *StorageEngine

	lockMgr := txns.NewLockManager()
	se, err = New(
		dir,
		uint64(200),
		lockMgr,
		afero.NewOsFs(),
		func(indexMeta storage.IndexMeta, locker *txns.LockManager, logger common.ITxnLoggerWithContext) (storage.Index, error) {
			return nil, nil
		},
	)
	require.NoError(t, err)

	tableName := "User"
	schema := storage.Schema{
		{Name: "id", Type: storage.ColumnTypeInt64},
		{Name: "name", Type: storage.ColumnTypeUUID},
	}

	firstTxnID := common.TxnID(1)
	defer lockMgr.Unlock(firstTxnID)

	err = se.CreateVertexTable(firstTxnID, tableName, schema, common.NoLogs())
	require.NoError(t, err)

	indexName := "user_name"

	err = se.CreateVertexTableIndex(
		firstTxnID,
		indexName,
		tableName,
		[]string{"name"},
		8,
		common.NoLogs(),
	)
	require.NoError(t, err)

	indexPath := GetVertexIndexFilePath(dir, indexName)
	_, err = os.Stat(indexPath)
	require.NoError(t, err)

	_, err = se.catalog.GetIndexMeta(indexName)
	require.NoError(t, err)

	err = se.DropVertexTableIndex(firstTxnID, indexName, common.NoLogs())
	require.NoError(t, err)

	_, err = os.Stat(indexPath)
	require.NoError(t, err)

	err = se.CreateVertexTableIndex(
		firstTxnID,
		indexName,
		tableName,
		[]string{"name"},
		8,
		common.NoLogs(),
	)
	require.NoError(t, err)

	_, err = os.Stat(indexPath)
	require.NoError(t, err)
}
