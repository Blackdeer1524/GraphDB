package engine

import (
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/storage/systemcatalog"
	"github.com/Blackdeer1524/GraphDB/src/storage/systemcatalog/mocks"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
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

	lockMgr := &mocks.MockLockManager{
		AllowLock: true,
	}

	var se *StorageEngine

	se, err = New(dir, uint64(200), afero.NewOsFs(), lockMgr)

	tableName := "User"
	schema := storage.Schema{
		"id":   storage.Column{Type: "int"},
		"name": storage.Column{Type: "string"},
	}

	err = se.CreateVertexTable(1, tableName, schema)
	require.NoError(t, err)

	tablePath := GetVertexTableFilePath(dir, tableName)
	info, err := os.Stat(tablePath)
	require.NoError(t, err)

	require.False(t, info.IsDir())

	tblMeta, err := se.catalog.GetVertexTableMeta(tableName)
	require.NoError(t, err)
	require.Equal(t, tableName, tblMeta.Name)

	require.Greater(t, se.catalog.CurrentVersion(), uint64(0))

	err = se.CreateVertexTable(2, tableName, schema)
	require.Error(t, err)
}

func TestStorageEngine_DropVertexTable(t *testing.T) {
	dir := t.TempDir()

	err := systemcatalog.InitSystemCatalog(dir, afero.NewOsFs())
	require.NoError(t, err)

	lockMgr := &mocks.MockLockManager{
		AllowLock: true,
	}

	var se *StorageEngine

	se, err = New(dir, uint64(200), afero.NewOsFs(), lockMgr)

	tableName := "User"
	schema := storage.Schema{
		"id":   storage.Column{Type: "int"},
		"name": storage.Column{Type: "string"},
	}

	err = se.CreateVertexTable(1, tableName, schema)
	require.NoError(t, err)

	tablePath := GetVertexTableFilePath(dir, tableName)
	info, err := os.Stat(tablePath)
	require.NoError(t, err)

	require.False(t, info.IsDir())

	err = se.DropVertexTable(1, tableName)
	require.NoError(t, err)

	info, err = os.Stat(tablePath)
	require.NoError(t, err)

	err = se.DropVertexTable(1, tableName)
	require.Error(t, err)

	require.Equal(t, uint64(2), se.catalog.CurrentVersion())

	err = se.CreateVertexTable(2, tableName, schema)
	require.NoError(t, err)

	tablePath = GetVertexTableFilePath(dir, tableName)
	info, err = os.Stat(tablePath)
	require.NoError(t, err)
}

func TestStorageEngine_CreateEdgeTable(t *testing.T) {
	dir := t.TempDir()

	err := systemcatalog.InitSystemCatalog(dir, afero.NewOsFs())
	require.NoError(t, err)

	lockMgr := &mocks.MockLockManager{
		AllowLock: true,
	}

	var se *StorageEngine

	se, err = New(dir, uint64(200), afero.NewOsFs(), lockMgr)

	tableName := "IsFriendWith"
	schema := storage.Schema{
		"from": storage.Column{Type: "int"},
		"to":   storage.Column{Type: "int"},
	}

	err = se.CreateEdgesTable(1, tableName, schema)
	require.NoError(t, err)

	tablePath := GetEdgeTableFilePath(dir, tableName)
	info, err := os.Stat(tablePath)
	require.NoError(t, err)

	require.False(t, info.IsDir())

	tblMeta, err := se.catalog.GetEdgeTableMeta(tableName)
	require.NoError(t, err)
	require.Equal(t, tableName, tblMeta.Name)

	require.Greater(t, se.catalog.CurrentVersion(), uint64(0))

	err = se.CreateEdgesTable(2, tableName, schema)
	require.Error(t, err)
}

func TestStorageEngine_DropEdgesTable(t *testing.T) {
	dir := t.TempDir()

	err := systemcatalog.InitSystemCatalog(dir, afero.NewOsFs())
	require.NoError(t, err)

	lockMgr := &mocks.MockLockManager{
		AllowLock: true,
	}

	var se *StorageEngine

	se, err = New(dir, uint64(200), afero.NewOsFs(), lockMgr)

	tableName := "IsFriendWith"
	schema := storage.Schema{
		"from": storage.Column{Type: "int"},
		"to":   storage.Column{Type: "int"},
	}

	err = se.CreateEdgesTable(1, tableName, schema)
	require.NoError(t, err)

	tablePath := GetEdgeTableFilePath(dir, tableName)
	info, err := os.Stat(tablePath)
	require.NoError(t, err)

	require.False(t, info.IsDir())

	err = se.DropEdgesTable(1, tableName)
	require.NoError(t, err)

	info, err = os.Stat(tablePath)
	require.NoError(t, err)

	err = se.DropEdgesTable(1, tableName)
	require.Error(t, err)

	require.Equal(t, uint64(2), se.catalog.CurrentVersion())

	err = se.CreateEdgesTable(2, tableName, schema)
	require.NoError(t, err)

	tablePath = GetEdgeTableFilePath(dir, tableName)
	info, err = os.Stat(tablePath)
	require.NoError(t, err)
}

func TestStorageEngine_CreateIndex(t *testing.T) {
	dir := t.TempDir()

	err := systemcatalog.InitSystemCatalog(dir, afero.NewOsFs())
	require.NoError(t, err)

	lockMgr := &mocks.MockLockManager{
		AllowLock: true,
	}

	var se *StorageEngine

	se, err = New(dir, uint64(200), afero.NewOsFs(), lockMgr)

	tableName := "User"
	schema := storage.Schema{
		"id":   storage.Column{Type: "int"},
		"name": storage.Column{Type: "string"},
	}

	err = se.CreateVertexTable(1, tableName, schema)
	require.NoError(t, err)

	indexName := "idx_user_name"

	err = se.CreateIndex(1, indexName, tableName, "vertex", []string{"name"}, 8)
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

	lockMgr := &mocks.MockLockManager{
		AllowLock: true,
	}

	var se *StorageEngine

	se, err = New(dir, uint64(200), afero.NewOsFs(), lockMgr)

	tableName := "User"
	schema := storage.Schema{
		"id":   storage.Column{Type: "int"},
		"name": storage.Column{Type: "string"},
	}

	err = se.CreateVertexTable(1, tableName, schema)
	require.NoError(t, err)

	indexName := "idx_user_name"

	err = se.CreateIndex(1, indexName, tableName, "vertex", []string{"name"}, 8)
	require.NoError(t, err)

	indexPath := GetIndexFilePath(dir, indexName)
	_, err = os.Stat(indexPath)
	require.NoError(t, err)

	_, err = se.catalog.GetIndexMeta(indexName)
	require.NoError(t, err)

	err = se.DropIndex(1, indexName)
	require.NoError(t, err)

	_, err = os.Stat(indexPath)
	require.NoError(t, err)

	err = se.CreateIndex(1, indexName, tableName, "vertex", []string{"name"}, 8)
	require.NoError(t, err)

	_, err = os.Stat(indexPath)
	require.NoError(t, err)
}
