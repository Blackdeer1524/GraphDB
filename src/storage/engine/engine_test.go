package engine

import (
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage/disk"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
	"github.com/Blackdeer1524/GraphDB/src/storage/systemcatalog"
	"github.com/Blackdeer1524/GraphDB/src/storage/systemcatalog/mocks"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"os"
	"testing"

	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/stretchr/testify/assert"
)

func Test_getVertexTableFilePath(t *testing.T) {
	ans := getVertexTableFilePath("/var/lib/graphdb", "friends")

	assert.Equal(t, "/var/lib/graphdb/tables/vertex/friends.tbl", ans)
}

func Test_getEdgeTableFilePath(t *testing.T) {
	ans := getEdgeTableFilePath("/var/lib/graphdb", "friends")

	assert.Equal(t, "/var/lib/graphdb/tables/edge/friends.tbl", ans)
}

func TestStorageEngine_CreateVertexTable(t *testing.T) {
	dir := t.TempDir()

	err := systemcatalog.InitSystemCatalog(dir, afero.NewOsFs())
	require.NoError(t, err)

	fileIDToFilePath := map[common.FileID]string{
		common.FileID(0): systemcatalog.GetSystemCatalogVersionFileName(dir),
	}

	diskMgr := disk.New[*page.SlottedPage](fileIDToFilePath, page.NewSlottedPage)

	catalog, err := systemcatalog.New(dir, afero.NewOsFs(), &mocks.MockDataBufferPool{Disk: diskMgr})
	require.NoError(t, err)

	lockMgr := &mocks.MockLockManager{
		AllowLock: true,
	}

	se := &StorageEngine{
		catalog: catalog,
		fs:      afero.NewOsFs(),
		lock:    lockMgr,
	}

	tableName := "User"
	schema := storage.Schema{
		"id":   storage.Column{Type: "int"},
		"name": storage.Column{Type: "string"},
	}

	err = se.CreateVertexTable(1, tableName, schema)
	require.NoError(t, err)

	tablePath := getVertexTableFilePath(dir, tableName)
	info, err := os.Stat(tablePath)
	require.NoError(t, err)

	require.False(t, info.IsDir())

	tblMeta, err := catalog.GetVertexTableMeta(tableName)
	require.NoError(t, err)
	require.Equal(t, tableName, tblMeta.Name)

	require.Greater(t, catalog.CurrentVersion(), uint64(0))

	err = se.CreateVertexTable(2, tableName, schema)
	require.Error(t, err)
}
