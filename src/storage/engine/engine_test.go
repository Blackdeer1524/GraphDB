package engine

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

func newTestEngineWithMockCatalog(
	t *testing.T,
) (*StorageEngine, *storage.MockSystemCatalog, *txns.LockManager) {
	cat := storage.NewMockSystemCatalog(t)
	locker := txns.NewLockManager()
	fs := afero.NewMemMapFs()
	indexLoader := func(indexMeta storage.IndexMeta, pool bufferpool.BufferPool, locker *txns.LockManager, logger common.ITxnLoggerWithContext) (storage.Index, error) {
		return storage.NewMockIndex(t), nil
	}

	eng := New(cat, nil, nil, nil, locker, fs, indexLoader, true)
	return eng, cat, locker
}

func TestStorageEngine_GetVertexTableMeta_LoadOrder(t *testing.T) {
	eng, cat, locker := newTestEngineWithMockCatalog(t)
	ct := txns.NewNilCatalogLockToken(common.TxnID(1))

	expected := storage.VertexTableMeta{Name: "users"}
	step := 0
	cat.EXPECT().Load().Run(func() { require.Equal(t, 0, step); step = 1 }).Return(nil).Once()
	cat.EXPECT().
		GetVertexTableMeta("users").
		Run(func(_ string) { require.Equal(t, 1, step); step = 2 }).
		Return(expected, nil).
		Once()

	meta, err := eng.GetVertexTableMeta("users", ct)
	require.NoError(t, err)
	require.Equal(t, expected, meta)

	// ensure locker path exercised
	require.NotNil(t, locker)
}

func TestStorageEngine_GetEdgeTableMeta_LoadOrder(t *testing.T) {
	eng, cat, _ := newTestEngineWithMockCatalog(t)
	ct := txns.NewNilCatalogLockToken(common.TxnID(2))

	expected := storage.EdgeTableMeta{Name: "follows"}
	step := 0
	cat.EXPECT().Load().Run(func() { require.Equal(t, 0, step); step = 1 }).Return(nil).Once()
	cat.EXPECT().
		GetEdgeTableMeta("follows").
		Run(func(_ string) { require.Equal(t, 1, step); step = 2 }).
		Return(expected, nil).
		Once()

	meta, err := eng.GetEdgeTableMeta("follows", ct)
	require.NoError(t, err)
	require.Equal(t, expected, meta)
}

func TestStorageEngine_GetDirTableMeta_LoadOrder(t *testing.T) {
	eng, cat, _ := newTestEngineWithMockCatalog(t)
	ct := txns.NewNilCatalogLockToken(common.TxnID(3))
	fileID := common.FileID(10)

	expected := storage.DirTableMeta{VertexTableID: fileID}
	step := 0
	cat.EXPECT().Load().Run(func() { require.Equal(t, 0, step); step = 1 }).Return(nil).Once()
	cat.EXPECT().
		GetDirTableMeta(fileID).
		Run(func(_ common.FileID) { require.Equal(t, 1, step); step = 2 }).
		Return(expected, nil).
		Once()

	meta, err := eng.GetDirTableMeta(ct, fileID)
	require.NoError(t, err)
	require.Equal(t, expected, meta)
}

func TestStorageEngine_GetVertexTableIndexMeta_LoadOrder(t *testing.T) {
	eng, cat, _ := newTestEngineWithMockCatalog(t)
	ct := txns.NewNilCatalogLockToken(common.TxnID(4))

	expected := storage.IndexMeta{Name: "idx_users_name"}
	step := 0
	cat.EXPECT().Load().Run(func() { require.Equal(t, 0, step); step = 1 }).Return(nil).Once()
	cat.EXPECT().
		GetVertexTableIndexMeta("idx_users_name").
		Run(func(_ string) { require.Equal(t, 1, step); step = 2 }).
		Return(expected, nil).
		Once()

	meta, err := eng.GetVertexTableIndexMeta("idx_users_name", ct)
	require.NoError(t, err)
	require.Equal(t, expected, meta)
}

func TestStorageEngine_GetEdgeIndexMeta_LoadOrder(t *testing.T) {
	eng, cat, _ := newTestEngineWithMockCatalog(t)
	ct := txns.NewNilCatalogLockToken(common.TxnID(5))

	expected := storage.IndexMeta{Name: "idx_follows_weight"}
	step := 0
	cat.EXPECT().Load().Run(func() { require.Equal(t, 0, step); step = 1 }).Return(nil).Once()
	cat.EXPECT().
		GetEdgeIndexMeta("idx_follows_weight").
		Run(func(_ string) { require.Equal(t, 1, step); step = 2 }).
		Return(expected, nil).
		Once()

	meta, err := eng.GetEdgeIndexMeta("idx_follows_weight", ct)
	require.NoError(t, err)
	require.Equal(t, expected, meta)
}

func TestStorageEngine_GetVertexTableSystemIndexMeta_LoadOrder(t *testing.T) {
	eng, cat, _ := newTestEngineWithMockCatalog(t)
	ct := txns.NewNilCatalogLockToken(common.TxnID(6))
	fileID := common.FileID(42)
	internalName := "idx_internal_42"

	expected := storage.IndexMeta{Name: internalName}
	step := 0
	cat.EXPECT().Load().Run(func() { require.Equal(t, 0, step); step = 1 }).Return(nil).Once()
	cat.EXPECT().
		GetVertexTableIndexMeta(internalName).
		Run(func(_ string) { require.Equal(t, 1, step); step = 2 }).
		Return(expected, nil).
		Once()

	meta, err := eng.GetVertexTableSystemIndexMeta(fileID, ct)
	require.NoError(t, err)
	require.Equal(t, expected, meta)
}

func TestStorageEngine_GetEdgeTableSystemIndexMeta_LoadOrder(t *testing.T) {
	eng, cat, _ := newTestEngineWithMockCatalog(t)
	ct := txns.NewNilCatalogLockToken(common.TxnID(7))
	fileID := common.FileID(7)
	internalName := "idx_internal_7"

	expected := storage.IndexMeta{Name: internalName}
	step := 0
	cat.EXPECT().Load().Run(func() { require.Equal(t, 0, step); step = 1 }).Return(nil).Once()
	cat.EXPECT().
		GetEdgeIndexMeta(internalName).
		Run(func(_ string) { require.Equal(t, 1, step); step = 2 }).
		Return(expected, nil).
		Once()

	meta, err := eng.GetEdgeTableSystemIndexMeta(fileID, ct)
	require.NoError(t, err)
	require.Equal(t, expected, meta)
}

func TestStorageEngine_GetEdgeTableMetaByFileID_LoadOrder(t *testing.T) {
	eng, cat, _ := newTestEngineWithMockCatalog(t)
	ct := txns.NewNilCatalogLockToken(common.TxnID(8))
	fileID := common.FileID(77)

	expected := storage.EdgeTableMeta{Name: "edges"}
	step := 0
	cat.EXPECT().Load().Run(func() { require.Equal(t, 0, step); step = 1 }).Return(nil).Once()
	cat.EXPECT().
		GetEdgeTableNameByFileID(fileID).
		Run(func(_ common.FileID) { require.Equal(t, 1, step); step = 2 }).
		Return("edges", nil).
		Once()
	cat.EXPECT().
		GetEdgeTableMeta("edges").
		Run(func(_ string) { require.Equal(t, 2, step); step = 3 }).
		Return(expected, nil).
		Once()

	meta, err := eng.GetEdgeTableMetaByFileID(fileID, ct)
	require.NoError(t, err)
	require.Equal(t, expected, meta)
}

func TestStorageEngine_GetVertexTableMetaByFileID_LoadOrder(t *testing.T) {
	eng, cat, _ := newTestEngineWithMockCatalog(t)
	ct := txns.NewNilCatalogLockToken(common.TxnID(9))
	fileID := common.FileID(88)

	expected := storage.VertexTableMeta{Name: "users"}
	step := 0
	cat.EXPECT().Load().Run(func() { require.Equal(t, 0, step); step = 1 }).Return(nil).Once()
	cat.EXPECT().
		GetVertexTableNameByFileID(fileID).
		Run(func(_ common.FileID) { require.Equal(t, 1, step); step = 2 }).
		Return("users", nil).
		Once()
	cat.EXPECT().
		GetVertexTableMeta("users").
		Run(func(_ string) { require.Equal(t, 2, step); step = 3 }).
		Return(expected, nil).
		Once()

	meta, err := eng.GetVertexTableMetaByFileID(fileID, ct)
	require.NoError(t, err)
	require.Equal(t, expected, meta)
}

func TestStorageEngine_GetVertexTableIndex_LoadAndExistsOrder(t *testing.T) {
	eng, cat, _ := newTestEngineWithMockCatalog(t)
	ct := txns.NewNilCatalogLockToken(common.TxnID(10))
	logger := common.NewMockITxnLoggerWithContext(t)

	idxName := "idx_users_id"
	idxMeta := storage.IndexMeta{Name: idxName}
	step := 0
	cat.EXPECT().Load().Run(func() { require.Equal(t, 0, step); step = 1 }).Return(nil).Once()
	cat.EXPECT().
		VertexIndexExists(idxName).
		Run(func(_ string) { require.Equal(t, 1, step); step = 2 }).
		Return(true, nil).
		Once()
	cat.EXPECT().
		GetVertexTableIndexMeta(idxName).
		Run(func(_ string) { require.Equal(t, 2, step); step = 3 }).
		Return(idxMeta, nil).
		Once()

	idx, err := eng.GetVertexTableIndex(common.TxnID(10), idxName, ct, logger)
	require.NoError(t, err)
	require.NotNil(t, idx)
}

func TestStorageEngine_GetEdgeTableIndex_LoadAndExistsOrder(t *testing.T) {
	eng, cat, _ := newTestEngineWithMockCatalog(t)
	ct := txns.NewNilCatalogLockToken(common.TxnID(11))
	logger := common.NewMockITxnLoggerWithContext(t)

	idxName := "idx_edges_id"
	idxMeta := storage.IndexMeta{Name: idxName}
	step := 0
	cat.EXPECT().Load().Run(func() { require.Equal(t, 0, step); step = 1 }).Return(nil).Once()
	cat.EXPECT().
		EdgeIndexExists(idxName).
		Run(func(_ string) { require.Equal(t, 1, step); step = 2 }).
		Return(true, nil).
		Once()
	cat.EXPECT().
		GetEdgeIndexMeta(idxName).
		Run(func(_ string) { require.Equal(t, 2, step); step = 3 }).
		Return(idxMeta, nil).
		Once()

	idx, err := eng.GetEdgeTableIndex(common.TxnID(11), idxName, ct, logger)
	require.NoError(t, err)
	require.NotNil(t, idx)
}

func TestStorageEngine_GetDirTableSystemIndex_LoadAndExistsOrder(t *testing.T) {
	eng, cat, _ := newTestEngineWithMockCatalog(t)
	ct := txns.NewNilCatalogLockToken(common.TxnID(12))
	logger := common.NewMockITxnLoggerWithContext(t)

	fileID := common.FileID(5)
	internalName := "idx_internal_5"
	idxMeta := storage.IndexMeta{Name: internalName}
	step := 0
	cat.EXPECT().Load().Run(func() { require.Equal(t, 0, step); step = 1 }).Return(nil).Once()
	cat.EXPECT().
		DirIndexExists(internalName).
		Run(func(_ string) { require.Equal(t, 1, step); step = 2 }).
		Return(true, nil).
		Once()
	cat.EXPECT().
		GetDirIndexMeta(internalName).
		Run(func(_ string) { require.Equal(t, 2, step); step = 3 }).
		Return(idxMeta, nil).
		Once()

	idx, err := eng.GetDirTableSystemIndex(common.TxnID(12), fileID, ct, logger)
	require.NoError(t, err)
	require.NotNil(t, idx)
}
