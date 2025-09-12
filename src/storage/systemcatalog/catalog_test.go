package systemcatalog

import (
	"path/filepath"
	"strconv"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/storage/disk"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
)

func newTestCatalogManager(t *testing.T) (*Catalog, afero.Fs, bufferpool.BufferPool, string) {
	fs := afero.NewMemMapFs()
	basePath := "/tmp/graphdb_syscat_test"
	require.NoError(t, fs.MkdirAll(basePath, 0o700))

	require.NoError(t, InitSystemCatalog(basePath, fs))
	versionFilePath := GetSystemCatalogVersionFilePath(basePath)
	dm := disk.New(
		func(_ common.FileID, _ common.PageID) *page.SlottedPage {
			return page.NewSlottedPage()
		},
		fs,
	)
	dm.InsertToFileMap(CatalogVersionFileID, versionFilePath)

	replacer := bufferpool.NewLRUReplacer()
	pool := bufferpool.New(8, replacer, dm)

	m, err := New(basePath, fs, pool, dm.UpdateFileMap)
	require.NoError(t, err)

	return m, fs, pool, basePath
}

func newMockCtxLogger(t *testing.T) *common.MockITxnLoggerWithContext {
	logger := common.NewMockITxnLoggerWithContext(t)
	logger.On("GetTxnID").Return(common.TxnID(1)).Maybe()
	logger.On("AppendUpdate", mock.Anything, mock.Anything, mock.Anything).
		Return(common.LogRecordLocInfo{Lsn: 1, Location: common.FileLocation{}}, nil).
		Maybe()
	return logger
}

func TestCatalogManager_EmptyCatalog_ReadsAfterLoad(t *testing.T) {
	m, _, _, basePath := newTestCatalogManager(t)

	// Base path and version
	require.NoError(t, m.Load())
	require.Equal(t, basePath, m.GetBasePath())
	require.Equal(t, uint64(0), m.CurrentVersion())

	// Exists checks (all false)
	require.NoError(t, m.Load())
	exists, err := m.VertexTableExists("users")
	require.NoError(t, err)
	require.False(t, exists)

	require.NoError(t, m.Load())
	exists, err = m.EdgeTableExists("follows")
	require.NoError(t, err)
	require.False(t, exists)

	require.NoError(t, m.Load())
	exists, err = m.DirTableExists(common.FileID(123))
	require.NoError(t, err)
	require.False(t, exists)

	require.NoError(t, m.Load())
	exists, err = m.VertexIndexExists("v_idx")
	require.NoError(t, err)
	require.False(t, exists)

	require.NoError(t, m.Load())
	exists, err = m.EdgeIndexExists("e_idx")
	require.NoError(t, err)
	require.False(t, exists)

	require.NoError(t, m.Load())
	exists, err = m.DirIndexExists("d_idx")
	require.NoError(t, err)
	require.False(t, exists)

	// Get by name/fileID should return not found
	require.NoError(t, m.Load())
	_, err = m.GetVertexTableMeta("users")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrEntityNotFound)

	require.NoError(t, m.Load())
	_, err = m.GetEdgeTableMeta("follows")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrEntityNotFound)

	require.NoError(t, m.Load())
	_, err = m.GetDirTableMeta(common.FileID(123))
	require.Error(t, err)
	require.ErrorIs(t, err, ErrEntityNotFound)

	require.NoError(t, m.Load())
	_, err = m.GetVertexTableIndexMeta("v_idx")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrEntityNotFound)

	require.NoError(t, m.Load())
	_, err = m.GetEdgeIndexMeta("e_idx")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrEntityNotFound)

	require.NoError(t, m.Load())
	_, err = m.GetDirIndexMeta("d_idx")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrEntityNotFound)

	// Get table names by FileID should return not found
	require.NoError(t, m.Load())
	_, err = m.GetVertexTableNameByFileID(common.FileID(321))
	require.Error(t, err)
	require.ErrorIs(t, err, ErrEntityNotFound)

	require.NoError(t, m.Load())
	_, err = m.GetEdgeTableNameByFileID(common.FileID(654))
	require.Error(t, err)
	require.ErrorIs(t, err, ErrEntityNotFound)

	// Get indexes lists require existing table -> expect not found
	require.NoError(t, m.Load())
	_, err = m.GetVertexTableIndexes("users")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrEntityNotFound)

	require.NoError(t, m.Load())
	_, err = m.GetEdgeTableIndexes("follows")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrEntityNotFound)

	// FileID->path map is empty
	require.NoError(t, m.Load())
	mp := m.getFileIDToPathMapAssumeLocked()
	require.Empty(t, mp)
}

func TestCatalogManager_AddEntitiesAndRead_AfterLoad(t *testing.T) {
	m, fs, _, basePath := newTestCatalogManager(t)

	// prepare metas
	vtID := m.GetNewFileID()
	vtMeta := storage.VertexTableMeta{
		Name:       "users",
		FileID:     vtID,
		PathToFile: filepath.Join(basePath, "users.dat"),
		Schema: storage.Schema{
			{Name: "id", Type: storage.ColumnTypeUUID},
			{Name: "name", Type: storage.ColumnTypeUint64},
		},
	}

	dtID := m.GetNewFileID()
	dtMeta := storage.DirTableMeta{
		VertexTableID: vtID,
		FileID:        dtID,
		PathToFile:    filepath.Join(basePath, "dir_users.dat"),
	}

	etID := m.GetNewFileID()
	etMeta := storage.EdgeTableMeta{
		Name:            "follows",
		FileID:          etID,
		PathToFile:      filepath.Join(basePath, "follows.dat"),
		Schema:          storage.Schema{{Name: "weight", Type: storage.ColumnTypeFloat64}},
		SrcVertexFileID: vtID,
		DstVertexFileID: vtID,
	}

	viID := m.GetNewFileID()
	veIdx := storage.IndexMeta{
		Name:        "user_by_name",
		PathToFile:  filepath.Join(basePath, "user_by_name.idx"),
		FileID:      viID,
		TableName:   vtMeta.Name,
		Columns:     []string{"name"},
		KeyBytesCnt: 8,
	}

	eiID := m.GetNewFileID()
	edIdx := storage.IndexMeta{
		Name:        "follows_by_weight",
		PathToFile:  filepath.Join(basePath, "follows_by_weight.idx"),
		FileID:      eiID,
		TableName:   etMeta.Name,
		Columns:     []string{"weight"},
		KeyBytesCnt: 8,
	}

	diID := m.GetNewFileID()
	dirIdx := storage.IndexMeta{
		Name:        "dir_by_ID",
		PathToFile:  filepath.Join(basePath, "dir_by_ID.idx"),
		FileID:      diID,
		TableName:   GetDirTableName(vtID),
		Columns:     []string{"ID"},
		KeyBytesCnt: 16,
	}

	// apply mutations
	require.NoError(t, m.AddVertexTable(vtMeta))
	require.NoError(t, m.AddDirTable(dtMeta))
	require.NoError(t, m.AddEdgeTable(etMeta))
	require.NoError(t, m.AddVertexIndex(veIdx))
	require.NoError(t, m.AddEdgeIndex(edIdx))
	require.NoError(t, m.AddDirIndex(dirIdx))

	// commit and bump version
	logger := newMockCtxLogger(t)
	require.NoError(t, m.CommitChanges(logger))

	// version increased and reads reflect committed data
	require.NoError(t, m.Load())
	require.Equal(t, uint64(1), m.CurrentVersion())

	exists, err := m.VertexTableExists(vtMeta.Name)
	require.NoError(t, err)
	require.True(t, exists)

	exists, err = m.EdgeTableExists(etMeta.Name)
	require.NoError(t, err)
	require.True(t, exists)

	exists, err = m.DirTableExists(vtID)
	require.NoError(t, err)
	require.True(t, exists)

	gotVT, err := m.GetVertexTableMeta(vtMeta.Name)
	require.NoError(t, err)
	require.Equal(t, vtMeta, gotVT)

	gotET, err := m.GetEdgeTableMeta(etMeta.Name)
	require.NoError(t, err)
	require.Equal(t, etMeta, gotET)

	gotDT, err := m.GetDirTableMeta(vtID)
	require.NoError(t, err)
	require.Equal(t, dtMeta, gotDT)

	name, err := m.GetVertexTableNameByFileID(vtID)
	require.NoError(t, err)
	require.Equal(t, vtMeta.Name, name)

	name, err = m.GetEdgeTableNameByFileID(etID)
	require.NoError(t, err)
	require.Equal(t, etMeta.Name, name)

	vidxs, err := m.GetVertexTableIndexes(vtMeta.Name)
	require.NoError(t, err)
	require.Len(t, vidxs, 1)
	require.Equal(t, veIdx, vidxs[0])

	eidxs, err := m.GetEdgeTableIndexes(etMeta.Name)
	require.NoError(t, err)
	require.Len(t, eidxs, 1)
	require.Equal(t, edIdx, eidxs[0])

	gotVeIdx, err := m.GetVertexTableIndexMeta(veIdx.Name)
	require.NoError(t, err)
	require.Equal(t, veIdx, gotVeIdx)

	gotEdIdx, err := m.GetEdgeIndexMeta(edIdx.Name)
	require.NoError(t, err)
	require.Equal(t, edIdx, gotEdIdx)

	gotDirIdx, err := m.GetDirIndexMeta(dirIdx.Name)
	require.NoError(t, err)
	require.Equal(t, dirIdx, gotDirIdx)

	// fileID to path map contains all entries
	paths := m.getFileIDToPathMapAssumeLocked()
	require.Equal(t, vtMeta.PathToFile, paths[vtID])
	require.Equal(t, etMeta.PathToFile, paths[etID])
	require.Equal(t, dtMeta.PathToFile, paths[dtID])
	require.Equal(t, veIdx.PathToFile, paths[viID])
	require.Equal(t, edIdx.PathToFile, paths[eiID])
	require.Equal(t, dirIdx.PathToFile, paths[diID])

	// ensure catalog files exist in fs
	_, err = fs.Stat(filepath.Join(basePath, "system_catalog_1.json"))
	require.NoError(t, err)
}

func TestCatalogManager_DropEntities_ReadsAfterLoad(t *testing.T) {
	m, _, _, basePath := newTestCatalogManager(t)

	// seed with one of each and commit
	vtID := m.GetNewFileID()
	vtMeta := storage.VertexTableMeta{
		Name:       "users",
		FileID:     vtID,
		PathToFile: filepath.Join(basePath, "users.dat"),
		Schema:     storage.Schema{{Name: "id", Type: storage.ColumnTypeUUID}},
	}
	etID := m.GetNewFileID()
	etMeta := storage.EdgeTableMeta{
		Name:            "follows",
		FileID:          etID,
		PathToFile:      filepath.Join(basePath, "follows.dat"),
		Schema:          storage.Schema{{Name: "weight", Type: storage.ColumnTypeFloat64}},
		SrcVertexFileID: vtID,
		DstVertexFileID: vtID,
	}
	dtID := m.GetNewFileID()
	dtMeta := storage.DirTableMeta{
		VertexTableID: vtID,
		FileID:        dtID,
		PathToFile:    filepath.Join(basePath, "dir_users.dat"),
	}

	viID := m.GetNewFileID()
	veIdx := storage.IndexMeta{
		Name:        "user_by_name",
		PathToFile:  filepath.Join(basePath, "user_by_name.idx"),
		FileID:      viID,
		TableName:   vtMeta.Name,
		Columns:     []string{"name"},
		KeyBytesCnt: 8,
	}
	eiID := m.GetNewFileID()
	edIdx := storage.IndexMeta{
		Name:        "follows_by_weight",
		PathToFile:  filepath.Join(basePath, "follows_by_weight.idx"),
		FileID:      eiID,
		TableName:   etMeta.Name,
		Columns:     []string{"weight"},
		KeyBytesCnt: 8,
	}
	diID := m.GetNewFileID()
	dirIdx := storage.IndexMeta{
		Name:        "dir_by_ID",
		PathToFile:  filepath.Join(basePath, "dir_by_ID.idx"),
		FileID:      diID,
		TableName:   GetDirTableName(vtID),
		Columns:     []string{"ID"},
		KeyBytesCnt: 16,
	}

	require.NoError(t, m.AddVertexTable(vtMeta))
	require.NoError(t, m.AddEdgeTable(etMeta))
	require.NoError(t, m.AddDirTable(dtMeta))
	require.NoError(t, m.AddVertexIndex(veIdx))
	require.NoError(t, m.AddEdgeIndex(edIdx))
	require.NoError(t, m.AddDirIndex(dirIdx))
	logger := newMockCtxLogger(t)
	require.NoError(t, m.CommitChanges(logger))

	// drop edge index and verify
	require.NoError(t, m.DropEdgeIndex(edIdx.Name))
	require.NoError(t, m.CommitChanges(logger))
	require.NoError(t, m.Load())
	exists, err := m.EdgeIndexExists(edIdx.Name)
	require.NoError(t, err)
	require.False(t, exists)
	require.NoError(t, m.Load())
	_, err = m.GetEdgeIndexMeta(edIdx.Name)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrEntityNotFound)

	// drop vertex table and verify
	require.NoError(t, m.DropVertexTable(vtMeta.Name))
	require.NoError(t, m.CommitChanges(logger))
	require.NoError(t, m.Load())
	exists, err = m.VertexTableExists(vtMeta.Name)
	require.NoError(t, err)
	require.False(t, exists)

	// drop dir table and verify
	require.NoError(t, m.DropDirTable(vtID))
	require.NoError(t, m.CommitChanges(logger))
	require.NoError(t, m.Load())
	exists, err = m.DirTableExists(vtID)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestCatalogManager_SimpleRollback(t *testing.T) {
	m, _, _, basePath := newTestCatalogManager(t)
	m.GetBasePath()

	require.NoError(t, m.AddVertexTable(storage.VertexTableMeta{
		Name:       "users",
		FileID:     m.GetNewFileID(),
		PathToFile: filepath.Join(basePath, "users.dat"),
		Schema:     storage.Schema{{Name: "id", Type: storage.ColumnTypeUUID}},
	}))

	require.NoError(t, m.Load())
	exists, err := m.VertexTableExists("users")
	require.NoError(t, err)
	require.False(t, exists)
	require.Equal(t, m.CurrentVersion(), uint64(0))
}

func TestCatalogManager_VersionRollback(t *testing.T) {
	m, _, _, basePath := newTestCatalogManager(t)
	m.GetBasePath()

	fileID := m.GetNewFileID()
	require.NoError(t, m.AddVertexTable(storage.VertexTableMeta{
		Name:       "users",
		FileID:     fileID,
		PathToFile: filepath.Join(basePath, strconv.Itoa(int(fileID))),
		Schema:     storage.Schema{{Name: "id", Type: storage.ColumnTypeUUID}},
	}))

	logger := newMockCtxLogger(t)
	require.NoError(t, m.CommitChanges(logger))

	exists, err := m.VertexTableExists("users")
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, uint64(1), m.CurrentVersion())

	// rollback
	m.currentVersionPage.UnsafeUpdateNoLogs(catalogVersionSlotNum, utils.ToBytes[uint64](0))

	require.NoError(t, m.Load())
	assert.Equal(t, uint64(0), m.CurrentVersion())
	exists, err = m.VertexTableExists("users")
	assert.NoError(t, err)
	assert.False(t, exists)
}
