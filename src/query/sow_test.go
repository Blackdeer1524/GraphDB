package query

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/query/mocks"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/storage/datastructures/inmemory"
	"github.com/Blackdeer1524/GraphDB/src/storage/engine"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

// Tests for GetVertexesOnDepth
func TestGetVertexesOnDepth_NilStorageEngine(t *testing.T) {
	locker := txns.NewLockManager()
	e := New(nil, locker, common.DummyLogger())

	_, err := e.GetVertexesOnDepth(1, 0)
	require.Error(t, err)
	assert.Equal(t, "storage engine is nil", err.Error())
}

func TestGetVertexesOnDepth_TransactionBeginError(t *testing.T) {
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(errors.New("begin error"))

	txnID := common.TxnID(1)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLocker := txns.MockILockManager{}
	mockLocker.EXPECT().Unlock(txnID).Return()
	mockLocker.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)

	se := mocks.NewDataMockStorageEngine(nil, nil, nil, nil, nil, nil)
	e := New(se, &mockLocker, &mockLogger)

	_, err := e.GetVertexesOnDepth(1, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "begin error")
}

func TestGetVertexesOnDepth_GetVertexRIDError(t *testing.T) {
	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendAbort().Return(nil)
	ctxMockLogger.EXPECT().Rollback().Return()
	ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLocker := txns.MockILockManager{}
	mockLocker.EXPECT().Unlock(txnID).Return()
	mockLocker.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)

	se := mocks.NewDataMockStorageEngine(nil, nil, nil, nil, nil, errors.New("rid error"))
	e := New(se, &mockLocker, &mockLogger)

	_, err := e.GetVertexesOnDepth(1, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rid error")
}

func TestGetVertexesOnDepth_Depth0(t *testing.T) {
	vertices := []engine.VertexID{1}
	edges := [][]engine.VertexID{}
	se := mocks.NewDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)

	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendCommit().Return(nil)
	ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)
	e := New(se, &mockLockMgr, &mockLogger)

	expected := []storage.VertexIDWithRID{{V: 1, R: common.RecordID{PageID: 100}}}
	res, err := e.GetVertexesOnDepth(1, 0)
	require.NoError(t, err)
	assert.Equal(t, expected, res)
}

func TestGetVertexesOnDepth_Depth1(t *testing.T) {
	vertices := []engine.VertexID{1, 2, 3}
	edges := [][]engine.VertexID{{1, 2}, {1, 3}}
	se := mocks.NewDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)

	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendCommit().Return(nil)
	ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)
	e := New(se, &mockLockMgr, &mockLogger)

	res, err := e.GetVertexesOnDepth(1, 1)
	require.NoError(t, err)

	expected := []storage.VertexIDWithRID{
		{V: 2, R: common.RecordID{PageID: 200}},
		{V: 3, R: common.RecordID{PageID: 300}},
	}
	assert.ElementsMatch(t, expected, res)
}

func TestGetVertexesOnDepth_Depth2(t *testing.T) {
	vertices := []engine.VertexID{1, 2, 3, 4, 5}
	edges := [][]engine.VertexID{{1, 2}, {1, 3}, {2, 4}, {3, 5}}
	se := mocks.NewDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)

	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendCommit().Return(nil)
	ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)
	e := New(se, &mockLockMgr, &mockLogger)

	expected := []storage.VertexIDWithRID{
		{V: 4, R: common.RecordID{PageID: 400}},
		{V: 5, R: common.RecordID{PageID: 500}},
	}
	res, err := e.GetVertexesOnDepth(1, 2)
	require.NoError(t, err)
	assert.ElementsMatch(t, expected, res)
}

func TestGetVertexesOnDepth_WithCycle(t *testing.T) {
	vertices := []engine.VertexID{1, 2, 3}
	edges := [][]engine.VertexID{{1, 2}, {2, 3}, {3, 1}}
	se := mocks.NewDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)

	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendCommit().Return(nil)
	ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)
	e := New(se, &mockLockMgr, &mockLogger)

	res, err := e.GetVertexesOnDepth(1, 1)
	require.NoError(t, err)

	expected := []storage.VertexIDWithRID{
		{V: 2, R: common.RecordID{PageID: 200}},
		{V: 3, R: common.RecordID{PageID: 300}},
	}
	assert.Equal(t, expected, res)
}

func TestGetVertexesOnDepth_CommitError(t *testing.T) {
	vertices := []engine.VertexID{1}
	edges := [][]engine.VertexID{}
	se := mocks.NewDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)

	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendCommit().Return(errors.New("commit error"))

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)

	e := New(se, &mockLockMgr, &mockLogger)
	_, err := e.GetVertexesOnDepth(1, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "commit error")
}

func TestGetVertexesOnDepth_RollbackOnError(t *testing.T) {
	vertices := []engine.VertexID{1}
	edges := [][]engine.VertexID{}
	se := mocks.NewDataMockStorageEngine(vertices, edges, nil, nil, nil, errors.New("rid error"))

	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendAbort().Return(nil)
	ctxMockLogger.EXPECT().Rollback().Return()
	ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)
	e := New(se, &mockLockMgr, &mockLogger)

	_, err := e.GetVertexesOnDepth(1, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get start vertex: rid error")
}

func TestGetVertexesOnDepth_DepthOverflow(t *testing.T) {
	vertices := []engine.VertexID{1}
	edges := [][]engine.VertexID{}
	se := mocks.NewDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)

	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendCommit().Return(nil)
	ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)
	e := New(se, &mockLockMgr, &mockLogger)

	res, err := e.GetVertexesOnDepth(1, ^uint32(0))
	require.NoError(t, err)
	assert.Empty(t, res, "При максимальной глубине ожидается пустой результат")
}

func TestBFS_NewQueueError(t *testing.T) {
	vertices := []engine.VertexID{1}
	edges := [][]engine.VertexID{}
	se := mocks.NewDataMockStorageEngine(vertices, edges, nil, errors.New("queue error"), nil, nil)

	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendAbort().Return(nil)
	ctxMockLogger.EXPECT().Rollback().Return()
	ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)
	e := New(se, &mockLockMgr, &mockLogger)

	start := storage.VertexIDWithRID{V: 1, R: common.RecordID{PageID: 100}}
	_, err := e.bfsWithDepth(0, start, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "queue error")
}

func TestBFS_NewBitMapError(t *testing.T) {
	vertices := []engine.VertexID{1}
	edges := [][]engine.VertexID{}
	se := mocks.NewDataMockStorageEngine(vertices, edges, nil, nil, errors.New("bitmap error"), nil)
	e := &Executor{se: se}
	start := storage.VertexIDWithRID{V: 1, R: common.RecordID{PageID: 100}}

	_, err := e.bfsWithDepth(0, start, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bitmap error")
}

func TestBFS_Depth0(t *testing.T) {
	vertices := []engine.VertexID{1}
	edges := [][]engine.VertexID{}
	se := mocks.NewDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)

	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendCommit().Return(nil)
	ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)
	e := New(se, &mockLockMgr, &mockLogger)

	start := storage.VertexIDWithRID{V: 1, R: common.RecordID{PageID: 100}}
	res, err := e.bfsWithDepth(0, start, 0)
	require.NoError(t, err)
	assert.Equal(t, []storage.VertexIDWithRID{start}, res)
}

func TestBFS_Depth1(t *testing.T) {
	vertices := []engine.VertexID{1, 2, 3}
	edges := [][]engine.VertexID{{1, 2}, {1, 3}}
	se := mocks.NewDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)

	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendCommit().Return(nil)
	ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)
	e := New(se, &mockLockMgr, &mockLogger)

	start := storage.VertexIDWithRID{V: 1, R: common.RecordID{PageID: 100}}
	res, err := e.bfsWithDepth(0, start, 1)
	require.NoError(t, err)
	assert.ElementsMatch(t, []storage.VertexIDWithRID{
		{V: 2, R: common.RecordID{PageID: 200}},
		{V: 3, R: common.RecordID{PageID: 300}},
	}, res)
}

func TestBFS_Depth2(t *testing.T) {
	vertices := []engine.VertexID{1, 2, 3, 4, 5}
	edges := [][]engine.VertexID{{1, 2}, {1, 3}, {2, 4}, {3, 5}}
	se := mocks.NewDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)

	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendCommit().Return(nil)
	ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)
	e := New(se, &mockLockMgr, &mockLogger)

	start := storage.VertexIDWithRID{V: 1, R: common.RecordID{PageID: 100}}
	res, err := e.bfsWithDepth(0, start, 2)
	require.NoError(t, err)
	assert.ElementsMatch(t, []storage.VertexIDWithRID{
		{V: 4, R: common.RecordID{PageID: 400}},
		{V: 5, R: common.RecordID{PageID: 500}},
	}, res)
}

func TestBFS_WithCycle(t *testing.T) {
	vertices := []engine.VertexID{1, 2, 3}
	edges := [][]engine.VertexID{{1, 2}, {2, 3}, {3, 1}}
	se := mocks.NewDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)

	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendCommit().Return(nil)
	ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)
	e := New(se, &mockLockMgr, &mockLogger)

	start := storage.VertexIDWithRID{V: 1, R: common.RecordID{PageID: 100}}
	res, err := e.bfsWithDepth(0, start, 1)
	require.NoError(t, err)
	assert.Equal(t,
		[]storage.VertexIDWithRID{
			{V: 2, R: common.RecordID{PageID: 200}},
			{V: 3, R: common.RecordID{PageID: 300}},
		},
		res,
	)
}

func TestBFS_TraverseNeighborsError(t *testing.T) {
	vertices := []engine.VertexID{1}
	edges := [][]engine.VertexID{}
	se := mocks.NewDataMockStorageEngine(
		vertices,
		edges,
		errors.New("neighbors error"),
		nil,
		nil,
		nil,
	)

	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendAbort().Return(nil)
	ctxMockLogger.EXPECT().Rollback().Return()
	ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)

	e := New(se, &mockLockMgr, &mockLogger)
	start := storage.VertexIDWithRID{V: 1, R: common.RecordID{PageID: 100}}
	_, err := e.bfsWithDepth(0, start, 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to traverse neighbors: neighbors error")
}

func TestBFS_NoVerticesAtTargetDepth(t *testing.T) {
	vertices := []engine.VertexID{1}
	edges := [][]engine.VertexID{}
	se := mocks.NewDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)

	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendCommit().Return(nil)
	ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)
	e := New(se, &mockLockMgr, &mockLogger)

	start := storage.VertexIDWithRID{V: 1, R: common.RecordID{PageID: 100}}
	res, err := e.bfsWithDepth(0, start, 1)
	require.NoError(t, err)
	assert.Empty(t, res)
}

func TestBFS_MultiplePathsToSameVertex(t *testing.T) {
	vertices := []engine.VertexID{1, 2, 3, 4}
	edges := [][]engine.VertexID{{1, 2}, {1, 3}, {2, 4}, {3, 4}}
	se := mocks.NewDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)

	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendCommit().Return(nil)
	ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)
	e := New(se, &mockLockMgr, &mockLogger)

	start := storage.VertexIDWithRID{V: 1, R: common.RecordID{PageID: 100}}
	res, err := e.bfsWithDepth(0, start, 2)
	require.NoError(t, err)
	assert.Equal(t, []storage.VertexIDWithRID{{V: 4, R: common.RecordID{PageID: 400}}}, res)
}

// GetAllVertexesWithFieldValue

func TestGetAllVertexesWithFieldValue(t *testing.T) {
	t.Run("nil storage engine", func(t *testing.T) {
		exec := &Executor{}
		res, err := exec.GetAllVertexesWithFieldValue("f", []byte("v"))
		assert.Nil(t, res)
		assert.Error(t, err)
	})

	t.Run("begin tx fails", func(t *testing.T) {
		txnID := common.TxnID(1)
		ctxMockLogger := common.MockITxnLoggerWithContext{}
		ctxMockLogger.EXPECT().AppendBegin().Return(errors.New("begin failed"))

		mockLogger := common.MockITxnLogger{}
		mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

		mockLockMgr := txns.MockILockManager{}
		mockLockMgr.EXPECT().Unlock(txnID).Return()
		mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
			txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
		)

		exec := New(new(mocks.DataMockStorageEngine), &mockLockMgr, &mockLogger)

		res, err := exec.GetAllVertexesWithFieldValue("f", []byte("v"))
		assert.Nil(t, res)
		assert.ErrorContains(t, err, "begin failed")
	})

	t.Run("all vertices fails", func(t *testing.T) {
		txnID := common.TxnID(1)
		ctxMockLogger := common.MockITxnLoggerWithContext{}
		ctxMockLogger.EXPECT().AppendBegin().Return(nil)
		ctxMockLogger.EXPECT().AppendAbort().Return(nil)
		ctxMockLogger.EXPECT().Rollback().Return()
		ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

		mockLogger := common.MockITxnLogger{}
		mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

		mockLockMgr := txns.MockILockManager{}
		mockLockMgr.EXPECT().Unlock(txnID).Return()
		mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
			txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
		)

		se := new(mocks.MockStorageEngine)
		se.On("AllVerticesWithValue", common.TxnID(1), "f", []byte("v")).
			Return(new(mocks.MockAllVerticesIter), errors.New("storage fail"))

		exec := New(se, &mockLockMgr, &mockLogger)

		res, err := exec.GetAllVertexesWithFieldValue("f", []byte("v"))
		assert.Nil(t, res)
		assert.ErrorContains(t, err, "failed to get vertices iterator")
	})

	t.Run("iterator close fails", func(t *testing.T) {
		txnID := common.TxnID(1)

		ctxMockLogger := common.MockITxnLoggerWithContext{}
		ctxMockLogger.EXPECT().AppendBegin().Return(nil)
		ctxMockLogger.EXPECT().AppendAbort().Return(nil)
		ctxMockLogger.EXPECT().Rollback().Return()
		ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

		mockLogger := common.MockITxnLogger{}
		mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

		mockLockMgr := txns.MockILockManager{}
		mockLockMgr.EXPECT().Unlock(txnID).Return()
		mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
			txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
		)

		se := new(mocks.MockStorageEngine)
		iter := new(mocks.MockAllVerticesIter)

		iter.SeqF = func(yield func(*storage.Vertex) bool) {
			yield(&storage.Vertex{ID: 1})
		}
		iter.CloseF = errors.New("close fail")

		se.On("AllVerticesWithValue", txnID, mock.Anything, mock.Anything).
			Return(iter, nil)

		exec := New(se, &mockLockMgr, &mockLogger)

		_, err := exec.GetAllVertexesWithFieldValue("f", []byte("v"))
		assert.ErrorContains(t, err, "close fail")
	})

	t.Run("commit fails", func(t *testing.T) {
		txnID := common.TxnID(1)
		ctxMockLogger := common.MockITxnLoggerWithContext{}
		ctxMockLogger.EXPECT().AppendBegin().Return(nil)
		ctxMockLogger.EXPECT().AppendCommit().Return(errors.New("commit fail"))

		mockLogger := common.MockITxnLogger{}
		mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

		mockLockMgr := txns.MockILockManager{}
		mockLockMgr.EXPECT().Unlock(txnID).Return()
		mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
			txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
		)

		se := new(mocks.MockStorageEngine)
		iter := new(mocks.MockAllVerticesIter)

		se.On("AllVerticesWithValue", txnID, "f", []byte("v")).Return(iter, nil)

		iter.SeqF = func(yield func(*storage.Vertex) bool) {
			yield(&storage.Vertex{ID: 2})
		}
		iter.CloseF = nil

		exec := New(se, &mockLockMgr, &mockLogger)
		_, err := exec.GetAllVertexesWithFieldValue("f", []byte("v"))
		assert.ErrorContains(t, err, "commit fail")
	})

	t.Run("success", func(t *testing.T) {
		txnID := common.TxnID(1)
		ctxMockLogger := common.MockITxnLoggerWithContext{}
		ctxMockLogger.EXPECT().AppendBegin().Return(nil)
		ctxMockLogger.EXPECT().AppendCommit().Return(nil)
		ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

		mockLogger := common.MockITxnLogger{}
		mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

		se := new(mocks.MockStorageEngine)
		iter := new(mocks.MockAllVerticesIter)

		se.On("AllVerticesWithValue", txnID, "f", []byte("v")).Return(iter, nil)

		expected := []*storage.Vertex{
			{ID: 1, Data: map[string]any{"f": "v"}},
			{ID: 2, Data: map[string]any{"f": "v"}},
		}

		iter.SeqF = func(yield func(*storage.Vertex) bool) {
			for _, v := range expected {
				if !yield(v) {
					break
				}
			}
		}

		mockLockMgr := txns.MockILockManager{}
		mockLockMgr.EXPECT().Unlock(txnID).Return()
		mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
			txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
		)

		exec := New(se, &mockLockMgr, &mockLogger)
		res, err := exec.GetAllVertexesWithFieldValue("f", []byte("v"))
		assert.NoError(t, err)
		assert.Equal(t, expected, res)
	})
}

// Tests GetAllVertexesWithFieldValue2

func TestGetAllVertexesWithFieldValue2_StorageNil(t *testing.T) {
	exec := &Executor{se: nil}

	res, err := exec.GetAllVertexesWithFieldValue2(
		"field",
		[]byte("val"),
		nil,
		1,
	)
	assert.Nil(t, res)
	assert.ErrorContains(t, err, "storage engine is nil")
}

func TestGetAllVertexesWithFieldValue2_BeginFails(t *testing.T) {
	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(errors.New("begin failed"))

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)

	exec := New(new(mocks.MockStorageEngine), &mockLockMgr, &mockLogger)
	res, err := exec.GetAllVertexesWithFieldValue2("f", []byte("v"), nil, 1)

	assert.Nil(t, res)
	assert.ErrorContains(t, err, "begin failed")
}

func TestGetAllVertexesWithFieldValue2_AllVerticesFails(t *testing.T) {
	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendAbort().Return(nil)
	ctxMockLogger.EXPECT().Rollback().Return()
	ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)

	se := new(mocks.MockStorageEngine)
	se.On("AllVerticesWithValue", txnID, "f", []byte("v")).
		Return((*mocks.MockAllVerticesIter)(nil), errors.New("all vertices error"))

	exec := New(se, &mockLockMgr, &mockLogger)
	res, err := exec.GetAllVertexesWithFieldValue2("f", []byte("v"), nil, 1)

	assert.Nil(t, res)
	assert.ErrorContains(t, err, "failed to get vertices iterator")
	se.AssertExpectations(t)
}

func TestGetAllVertexesWithFieldValue2_CountEdgesFails(t *testing.T) {
	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendAbort().Return(nil)
	ctxMockLogger.EXPECT().Rollback().Return()
	ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)

	iter := &mocks.MockAllVerticesIter{
		SeqF: func(yield func(*storage.Vertex) bool) {
			yield(&storage.Vertex{ID: 42})
		},
	}

	se := new(mocks.MockStorageEngine)
	se.On("AllVerticesWithValue", common.TxnID(1), "f", []byte("v")).
		Return(iter, nil)
	se.On("CountOfFilteredEdges", common.TxnID(1), engine.VertexID(42), mock.Anything).
		Return(uint64(0), errors.New("count failed"))

	exec := New(se, &mockLockMgr, &mockLogger)
	res, err := exec.GetAllVertexesWithFieldValue2("f", []byte("v"), nil, 1)

	assert.Nil(t, res)
	assert.ErrorContains(t, err, "failed to count edges")
	se.AssertExpectations(t)
}

func TestGetAllVertexesWithFieldValue2_SuccessPass(t *testing.T) {
	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendCommit().Return(nil)
	ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)

	iter := &mocks.MockAllVerticesIter{
		SeqF: func(yield func(*storage.Vertex) bool) {
			yield(&storage.Vertex{ID: 1})
		},
	}

	se := new(mocks.MockStorageEngine)
	se.On("AllVerticesWithValue", txnID, "f", []byte("v")).
		Return(iter, nil)
	se.On("CountOfFilteredEdges", txnID, engine.VertexID(1), mock.Anything).
		Return(uint64(5), nil)

	exec := New(se, &mockLockMgr, &mockLogger)
	res, err := exec.GetAllVertexesWithFieldValue2("f", []byte("v"), nil, 3)

	assert.NoError(t, err)
	assert.Len(t, res, 1)
	assert.Equal(t, engine.VertexID(1), res[0].ID)

	se.AssertExpectations(t)
}

func TestGetAllVertexesWithFieldValue2_SuccessFiltered(t *testing.T) {
	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendCommit().Return(nil)
	ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)

	iter := &mocks.MockAllVerticesIter{
		SeqF: func(yield func(*storage.Vertex) bool) {
			yield(&storage.Vertex{ID: 2})
		},
	}

	se := new(mocks.MockStorageEngine)
	se.On("AllVerticesWithValue", txnID, "f", []byte("v")).
		Return(iter, nil)
	se.On("CountOfFilteredEdges", txnID, engine.VertexID(2), mock.Anything).
		Return(uint64(1), nil)

	exec := New(se, &mockLockMgr, &mockLogger)
	res, err := exec.GetAllVertexesWithFieldValue2("f", []byte("v"), nil, 5)

	assert.NoError(t, err)
	assert.Len(t, res, 0)

	se.AssertExpectations(t)
}

func TestGetAllVertexesWithFieldValue2_MultipleResults(t *testing.T) {
	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendCommit().Return(nil)
	ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)

	se := new(mocks.MockStorageEngine)
	iter := new(mocks.MockAllVerticesIter)

	v1 := &storage.Vertex{ID: 1}
	v2 := &storage.Vertex{ID: 2}
	v3 := &storage.Vertex{ID: 3}
	v4 := &storage.Vertex{ID: 4}

	iter.SeqF = func(yield func(*storage.Vertex) bool) {
		yield(v1)
		yield(v2)
		yield(v3)
		yield(v4)
	}

	se.On("AllVerticesWithValue", txnID, "f", []byte("v")).Return(iter, nil)

	se.On("CountOfFilteredEdges", txnID, engine.VertexID(1), mock.Anything).
		Return(uint64(5), nil)
	se.On("CountOfFilteredEdges", txnID, engine.VertexID(2), mock.Anything).
		Return(uint64(2), nil)
	se.On("CountOfFilteredEdges", txnID, engine.VertexID(3), mock.Anything).
		Return(uint64(10), nil)
	se.On("CountOfFilteredEdges", txnID, engine.VertexID(4), mock.Anything).
		Return(uint64(3), nil)

	exec := New(se, &mockLockMgr, &mockLogger)

	res, err := exec.GetAllVertexesWithFieldValue2(
		"f",
		[]byte("v"),
		func(e *storage.Edge) bool { return true },
		3,
	)
	require.NoError(t, err)
	require.Len(t, res, 3)
	assert.Contains(t, res, v1)
	assert.Contains(t, res, v3)
	assert.Contains(t, res, v4)
}

// SumNeighborAttributes
func TestSumAttributeOverProperNeighbors_SumCorrectly(t *testing.T) {
	se := new(mocks.MockStorageEngine)
	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendCommit().Return(nil)
	ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)

	ex := New(se, &mockLockMgr, &mockLogger)

	neighbors := []*storage.Vertex{
		{ID: engine.VertexID(1), Data: map[string]interface{}{"val": 2.0}},
		{ID: engine.VertexID(2), Data: map[string]interface{}{"val": 3.5}},
	}

	iter := new(mocks.MockAllVerticesIter)
	iter.SeqF = func(yield func(*storage.Vertex) bool) {
		yield(neighbors[0])
		yield(neighbors[1])
	}

	se.On("GetNeighborsWithEdgeFilter", mock.Anything, engine.VertexID(1), mock.Anything).
		Return(iter, nil)

	res, err := ex.sumAttributeOverProperNeighbors(
		common.TxnID(1),
		&storage.Vertex{ID: engine.VertexID(1)},
		"val",
		nil,
	)
	assert.NoError(t, err)
	assert.Equal(t, 5.5, res)
}

func TestSumAttributeOverProperNeighbors_FieldMissing(t *testing.T) {
	se := new(mocks.MockStorageEngine)
	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendCommit().Return(nil)
	ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)

	ex := New(se, &mockLockMgr, &mockLogger)
	neighbors := []*storage.Vertex{
		{ID: engine.VertexID(1), Data: map[string]interface{}{}},
	}

	iter := new(mocks.MockAllVerticesIter)
	iter.SeqF = func(yield func(*storage.Vertex) bool) {
		yield(neighbors[0])
	}

	se.On("GetNeighborsWithEdgeFilter", mock.Anything, engine.VertexID(1), mock.Anything).
		Return(iter, nil)

	res, err := ex.sumAttributeOverProperNeighbors(
		txnID,
		&storage.Vertex{ID: engine.VertexID(1)},
		"val",
		nil,
	)
	assert.NoError(t, err)
	assert.Equal(t, 0.0, res)
}

func TestSumAttributeOverProperNeighbors_TypeMismatch(t *testing.T) {
	se := new(mocks.MockStorageEngine)
	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendAbort().Return(nil)
	ctxMockLogger.EXPECT().Rollback().Return()
	ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)

	ex := New(se, &mockLockMgr, &mockLogger)

	neighbors := []*storage.Vertex{
		{ID: engine.VertexID(1), Data: map[string]interface{}{"val": "notfloat"}},
	}

	iter := new(mocks.MockAllVerticesIter)
	iter.SeqF = func(yield func(*storage.Vertex) bool) {
		yield(neighbors[0])
	}

	se.On("GetNeighborsWithEdgeFilter", mock.Anything, engine.VertexID(1), mock.Anything).
		Return(iter, nil)

	res, err := ex.sumAttributeOverProperNeighbors(
		txnID,
		&storage.Vertex{ID: engine.VertexID(1)},
		"val",
		nil,
	)
	assert.Error(t, err)
	assert.Equal(t, 0.0, res)
}

func TestSumNeighborAttributes_SumAllVertices(t *testing.T) {
	se := new(mocks.MockStorageEngine)
	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendCommit().Return(nil)
	ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)

	ex := New(se, &mockLockMgr, &mockLogger)

	vertices := []*storage.Vertex{
		{ID: engine.VertexID(1)},
		{ID: engine.VertexID(2)},
	}

	verticesIter := new(mocks.MockAllVerticesIter)
	verticesIter.SeqF = func(yield func(*storage.Vertex) bool) {
		for _, v := range vertices {
			yield(v)
		}
	}

	se.On("GetAllVertices", common.TxnID(1)).Return(verticesIter, nil)

	neighborsV1 := []*storage.Vertex{
		{ID: engine.VertexID(11), Data: map[string]interface{}{"val": 1.0}},
	}
	neighborsV2 := []*storage.Vertex{
		{ID: engine.VertexID(12), Data: map[string]interface{}{"val": 1.0}},
	}

	iterV1 := new(mocks.MockAllVerticesIter)
	iterV1.SeqF = func(yield func(*storage.Vertex) bool) {
		for _, v := range neighborsV1 {
			yield(v)
		}
	}

	iterV2 := new(mocks.MockAllVerticesIter)
	iterV2.SeqF = func(yield func(*storage.Vertex) bool) {
		for _, v := range neighborsV2 {
			yield(v)
		}
	}

	se.On("GetNeighborsWithEdgeFilter", txnID, engine.VertexID(1), mock.Anything).
		Return(iterV1, nil)
	se.On("GetNeighborsWithEdgeFilter", txnID, engine.VertexID(2), mock.Anything).
		Return(iterV2, nil)
	se.On("NewAggregationAssociativeArray", txnID).
		Return(inmemory.NewInMemoryAssociativeArray[engine.VertexID, float64](), nil)

	resAA, err := ex.SumNeighborAttributes("val", nil, func(f float64) bool {
		return true
	})
	assert.NoError(t, err)

	v1val, ok1 := resAA.Get(engine.VertexID(1))
	v2val, ok2 := resAA.Get(engine.VertexID(2))

	assert.True(t, ok1)
	assert.True(t, ok2)
	assert.Equal(t, 1.0, v1val)
	assert.Equal(t, 1.0, v2val)

	se.AssertExpectations(t)
}

// GetAllTriangles

func TestGetAllTriangles_EmptyGraph(t *testing.T) {
	se := mocks.NewDataMockStorageEngine(nil, nil, nil, nil, nil, nil)

	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendCommit().Return(nil)
	ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)

	e := New(se, &mockLockMgr, &mockLogger)

	count, err := e.GetAllTriangles()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), count)
}

func TestGetAllTriangles_K3(t *testing.T) {
	vertices := []engine.VertexID{1, 2, 3}
	edges := [][]engine.VertexID{{1, 2}, {2, 3}, {1, 3}}
	se := mocks.NewDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)

	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendCommit().Return(nil)
	ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)

	e := New(se, &mockLockMgr, &mockLogger)

	count, err := e.GetAllTriangles()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), count, "K3 должен содержать ровно 1 треугольник")
}

func TestGetAllTriangles_K4(t *testing.T) {
	vertices := []engine.VertexID{1, 2, 3, 4}
	edges := [][]engine.VertexID{{1, 2}, {1, 3}, {1, 4}, {2, 3}, {2, 4}, {3, 4}}
	se := mocks.NewDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)

	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendCommit().Return(nil)
	ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)

	e := New(se, &mockLockMgr, &mockLogger)

	count, err := e.GetAllTriangles()
	require.NoError(t, err)
	assert.Equal(t, uint64(4), count, "K4 должен содержать ровно 4 треугольника")
}

func TestGetAllTriangles_DisconnectedWithTriangle(t *testing.T) {
	vertices := []engine.VertexID{1, 2, 3, 4}
	edges := [][]engine.VertexID{{1, 2}, {2, 3}, {1, 3}}
	se := mocks.NewDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)

	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendCommit().Return(nil)
	ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)

	e := New(se, &mockLockMgr, &mockLogger)

	count, err := e.GetAllTriangles()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), count)
}

func TestGetAllTriangles_SelfLoop(t *testing.T) {
	vertices := []engine.VertexID{1, 2, 3}
	edges := [][]engine.VertexID{{1, 1}, {1, 2}, {2, 3}, {1, 3}}
	se := mocks.NewDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)

	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendCommit().Return(nil)
	ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)

	e := New(se, &mockLockMgr, &mockLogger)

	count, err := e.GetAllTriangles()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), count)
}

func TestGetAllTriangles_TransactionBeginError(t *testing.T) {
	se := mocks.NewDataMockStorageEngine(nil, nil, nil, nil, nil, nil)

	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(errors.New("failed to begin tx"))

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)

	e := New(se, &mockLockMgr, &mockLogger)

	count, err := e.GetAllTriangles()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to begin tx")
	assert.Equal(t, uint64(0), count)
}

func TestGetAllTriangles_CommitError(t *testing.T) {
	vertices := []engine.VertexID{1, 2, 3}
	edges := [][]engine.VertexID{{1, 2}, {2, 3}, {1, 3}}
	se := mocks.NewDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)

	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendCommit().Return(errors.New("commit failed"))
	ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)

	e := New(se, &mockLockMgr, &mockLogger)

	count, err := e.GetAllTriangles()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "commit failed")
	assert.Equal(t, uint64(0), count)
}

func TestGetAllTriangles_NeighborsError(t *testing.T) {
	vertices := []engine.VertexID{1, 2, 3}
	edges := [][]engine.VertexID{{1, 2}, {2, 3}, {1, 3}}
	se := mocks.NewDataMockStorageEngine(
		vertices,
		edges,
		errors.New("failed to get neighbors"),
		nil,
		nil,
		nil,
	)

	txnID := common.TxnID(1)
	ctxMockLogger := common.MockITxnLoggerWithContext{}
	ctxMockLogger.EXPECT().AppendBegin().Return(nil)
	ctxMockLogger.EXPECT().AppendAbort().Return(nil)
	ctxMockLogger.EXPECT().Rollback().Return()
	ctxMockLogger.EXPECT().AppendTxnEnd().Return(nil)

	mockLogger := common.MockITxnLogger{}
	mockLogger.EXPECT().WithContext(txnID).Return(&ctxMockLogger)

	mockLockMgr := txns.MockILockManager{}
	mockLockMgr.EXPECT().Unlock(txnID).Return()
	mockLockMgr.EXPECT().LockCatalog(txnID, txns.GranularLockShared).Return(
		txns.NewCatalogLockToken(txnID, txns.GranularLockShared),
	)

	e := New(se, &mockLockMgr, &mockLogger)

	count, err := e.GetAllTriangles()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get neighbors")
	assert.Equal(t, uint64(0), count)
}
