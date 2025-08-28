package engine

import (
	"os"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/storage/systemcatalog"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

func Test_getVertexTableFilePath(t *testing.T) {
	ans := GetVertexTableFilePath("/var/lib/graphdb", "friends")

	assert.Equal(t, "/var/lib/graphdb/tables/vertex/friends.tbl", ans)
}

func Test_getEdgeTableFilePath(t *testing.T) {
	ans := GetEdgeTableFilePath("/var/lib/graphdb", "friends")

	assert.Equal(t, "/var/lib/graphdb/tables/edge/friends.tbl", ans)
}

func Test_getIndexFilePath(t *testing.T) {
	ans := GetIndexFilePath("/var/lib/graphdb", "idx_user_name")

	assert.Equal(t, "/var/lib/graphdb/indexes/idx_user_name.idx", ans)
}

func TestStorageEngine_CreateVertexTable(t *testing.T) {
	dir := t.TempDir()

	err := systemcatalog.InitSystemCatalog(dir, afero.NewOsFs())
	require.NoError(t, err)

	lockMgr := txns.NewHierarchyLocker()

	var se *StorageEngine

	se, err = New(dir, uint64(200), lockMgr, afero.NewOsFs())
	require.NoError(t, err)

	tableName := "User"
	schema := storage.Schema{
		"id":   storage.Column{Type: "int"},
		"name": storage.Column{Type: "string"},
	}

	func() {
		firstTxnID := common.TxnID(1)
		defer lockMgr.UnlockByTxnID(firstTxnID)
		err = se.CreateVertexTable(firstTxnID, tableName, schema, common.NoLogs())
		require.NoError(t, err)

		tablePath := GetVertexTableFilePath(dir, tableName)
		info, err := os.Stat(tablePath)
		require.NoError(t, err)

		require.False(t, info.IsDir())

		tblMeta, err := se.catalog.GetVertexTableMeta(tableName)
		require.NoError(t, err)
		require.Equal(t, tableName, tblMeta.Name)

		require.Greater(t, se.catalog.CurrentVersion(), uint64(0))
	}()

	func() {
		secondTxnID := common.TxnID(2)
		defer lockMgr.UnlockByTxnID(secondTxnID)

		err = se.CreateVertexTable(secondTxnID, tableName, schema, common.NoLogs())
		require.Error(t, err)
	}()
}

func TestStorageEngine_DropVertexTable(t *testing.T) {
	dir := t.TempDir()

	err := systemcatalog.InitSystemCatalog(dir, afero.NewOsFs())
	require.NoError(t, err)

	var se *StorageEngine

	lockMgr := txns.NewHierarchyLocker()
	se, err = New(dir, uint64(200), lockMgr, afero.NewOsFs())
	require.NoError(t, err)

	tableName := "User"
	schema := storage.Schema{
		"id":   storage.Column{Type: "int"},
		"name": storage.Column{Type: "string"},
	}

	func() {
		firstTxnID := common.TxnID(1)
		defer lockMgr.UnlockByTxnID(firstTxnID)

		err = se.CreateVertexTable(firstTxnID, tableName, schema, common.NoLogs())
		require.NoError(t, err)

		tablePath := GetVertexTableFilePath(dir, tableName)
		info, err := os.Stat(tablePath)
		require.NoError(t, err)

		require.False(t, info.IsDir())

		err = se.DropVertexTable(1, tableName, common.NoLogs())
		require.NoError(t, err)

		_, err = os.Stat(tablePath)
		require.NoError(t, err)

		err = se.DropVertexTable(1, tableName, common.NoLogs())
		require.Error(t, err)

		require.Equal(t, uint64(2), se.catalog.CurrentVersion())
	}()

	func() {
		secondTxnID := common.TxnID(2)
		defer lockMgr.UnlockByTxnID(secondTxnID)

		err = se.CreateVertexTable(secondTxnID, tableName, schema, common.NoLogs())
		require.NoError(t, err)

		tablePath := GetVertexTableFilePath(dir, tableName)
		_, err := os.Stat(tablePath)
		require.NoError(t, err)
	}()
}

func TestStorageEngine_CreateEdgeTable(t *testing.T) {
	dir := t.TempDir()

	err := systemcatalog.InitSystemCatalog(dir, afero.NewOsFs())
	require.NoError(t, err)

	var se *StorageEngine

	lockMgr := txns.NewHierarchyLocker()
	se, err = New(dir, uint64(200), lockMgr, afero.NewOsFs())
	require.NoError(t, err)

	tableName := "IsFriendWith"
	schema := storage.Schema{
		"from": storage.Column{Type: "int"},
		"to":   storage.Column{Type: "int"},
	}

	func() {
		firstTxnID := common.TxnID(1)
		defer lockMgr.UnlockByTxnID(firstTxnID)

		err = se.CreateEdgesTable(firstTxnID, tableName, schema, common.NoLogs())
		require.NoError(t, err)

		tablePath := GetEdgeTableFilePath(dir, tableName)
		info, err := os.Stat(tablePath)
		require.NoError(t, err)

		require.False(t, info.IsDir())

		tblMeta, err := se.catalog.GetEdgeTableMeta(tableName)
		require.NoError(t, err)
		require.Equal(t, tableName, tblMeta.Name)

		require.Greater(t, se.catalog.CurrentVersion(), uint64(0))
	}()

	func() {
		secondTxnID := common.TxnID(2)
		defer lockMgr.UnlockByTxnID(secondTxnID)

		err = se.CreateEdgesTable(secondTxnID, tableName, schema, common.NoLogs())
		require.Error(t, err)
	}()
}

func TestStorageEngine_DropEdgesTable(t *testing.T) {
	dir := t.TempDir()

	err := systemcatalog.InitSystemCatalog(dir, afero.NewOsFs())
	require.NoError(t, err)

	var se *StorageEngine

	lockMgr := txns.NewHierarchyLocker()
	se, err = New(dir, uint64(200), lockMgr, afero.NewOsFs())
	require.NoError(t, err)

	tableName := "IsFriendWith"
	schema := storage.Schema{
		"from": storage.Column{Type: "int"},
		"to":   storage.Column{Type: "int"},
	}

	func() {
		firstTxnID := common.TxnID(1)
		defer lockMgr.UnlockByTxnID(firstTxnID)

		err = se.CreateEdgesTable(firstTxnID, tableName, schema, common.NoLogs())
		require.NoError(t, err)

		tablePath := GetEdgeTableFilePath(dir, tableName)
		info, err := os.Stat(tablePath)
		require.NoError(t, err)

		require.False(t, info.IsDir())

		err = se.DropEdgesTable(firstTxnID, tableName, common.NoLogs())
		require.NoError(t, err)

		_, err = os.Stat(tablePath)
		require.NoError(t, err)

		err = se.DropEdgesTable(firstTxnID, tableName, common.NoLogs())
		require.Error(t, err)

		require.Equal(t, uint64(2), se.catalog.CurrentVersion())
	}()

	func() {
		secondTxnID := common.TxnID(2)
		defer lockMgr.UnlockByTxnID(secondTxnID)

		err = se.CreateEdgesTable(secondTxnID, tableName, schema, common.NoLogs())
		require.NoError(t, err)

		tablePath := GetEdgeTableFilePath(dir, tableName)
		_, err := os.Stat(tablePath)
		require.NoError(t, err)
	}()
}

func TestStorageEngine_CreateIndex(t *testing.T) {
	dir := t.TempDir()

	err := systemcatalog.InitSystemCatalog(dir, afero.NewOsFs())
	require.NoError(t, err)

	var se *StorageEngine

	lockMgr := txns.NewHierarchyLocker()
	se, err = New(dir, uint64(200), lockMgr, afero.NewOsFs())
	require.NoError(t, err)

	tableName := "User"
	schema := storage.Schema{
		"id":   storage.Column{Type: "int"},
		"name": storage.Column{Type: "string"},
	}

	firstTxnID := common.TxnID(1)
	defer lockMgr.UnlockByTxnID(firstTxnID)

	err = se.CreateVertexTable(firstTxnID, tableName, schema, common.NoLogs())
	require.NoError(t, err)

	indexName := "idx_user_name"

	err = se.CreateIndex(
		firstTxnID,
		indexName,
		tableName,
		"vertex",
		[]string{"name"},
		8,
		common.NoLogs(),
	)
	require.NoError(t, err)

	tablePath := GetIndexFilePath(dir, indexName)
	_, err = os.Stat(tablePath)
	require.NoError(t, err)

	_, err = se.catalog.GetIndexMeta(indexName)
	require.NoError(t, err)
}

func TestStorageEngine_DropIndex(t *testing.T) {
	dir := t.TempDir()

	err := systemcatalog.InitSystemCatalog(dir, afero.NewOsFs())
	require.NoError(t, err)

	var se *StorageEngine

	lockMgr := txns.NewHierarchyLocker()
	se, err = New(dir, uint64(200), lockMgr, afero.NewOsFs())
	require.NoError(t, err)

	tableName := "User"
	schema := storage.Schema{
		"id":   storage.Column{Type: "int"},
		"name": storage.Column{Type: "string"},
	}

	firstTxnID := common.TxnID(1)
	defer lockMgr.UnlockByTxnID(firstTxnID)

	err = se.CreateVertexTable(firstTxnID, tableName, schema, common.NoLogs())
	require.NoError(t, err)

	indexName := "idx_user_name"

	err = se.CreateIndex(
		firstTxnID,
		indexName,
		tableName,
		"vertex",
		[]string{"name"},
		8,
		common.NoLogs(),
	)
	require.NoError(t, err)

	indexPath := GetIndexFilePath(dir, indexName)
	_, err = os.Stat(indexPath)
	require.NoError(t, err)

	_, err = se.catalog.GetIndexMeta(indexName)
	require.NoError(t, err)

	err = se.DropIndex(firstTxnID, indexName, common.NoLogs())
	require.NoError(t, err)

	_, err = os.Stat(indexPath)
	require.NoError(t, err)

	err = se.CreateIndex(
		firstTxnID,
		indexName,
		tableName,
		"vertex",
		[]string{"name"},
		8,
		common.NoLogs(),
	)
	require.NoError(t, err)

	_, err = os.Stat(indexPath)
	require.NoError(t, err)
}
