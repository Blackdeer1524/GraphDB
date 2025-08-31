package query

import (
	"sync/atomic"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/storage/engine"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

type StorageEngine interface {
	NewQueue(common.TxnID) (storage.Queue, error)
	NewAggregationAssociativeArray(
		common.TxnID,
	) (storage.AssociativeArray[storage.VertexID, float64], error)
	NewBitMap(common.TxnID) (storage.BitMap, error)
	Neighbours(t common.TxnID, v storage.VertexID) (storage.NeighborIter, error)
	GetVertexRID(
		t common.TxnID,
		vertexType common.FileID,
		v storage.VertexID,
	) (storage.VertexIDWithRID, error)
	GetEdgeRID(
		t common.TxnID,
		edgeType common.FileID,
		e storage.EdgeID,
	) (storage.EdgeIDWithRID, error)
	AllVerticesWithValue(t common.TxnID, field string, value []byte) (storage.VerticesIter, error)
	CountOfFilteredEdges(t common.TxnID, v storage.VertexID, f storage.EdgeFilter) (uint64, error)
	GetAllVertices(t common.TxnID) (storage.VerticesIter, error)
	GetNeighborsWithEdgeFilter(
		t common.TxnID,
		v storage.VertexID,
		filter storage.EdgeFilter,
	) (storage.VerticesIter, error)

	CreateVertexTable(
		txnID common.TxnID,
		name string,
		schema storage.Schema,
		logger common.ITxnLoggerWithContext,
	) error
	DropVertexTable(txnID common.TxnID, name string, logger common.ITxnLoggerWithContext) error

	CreateEdgeTable(
		txnID common.TxnID,
		name string,
		schema storage.Schema,
		logger common.ITxnLoggerWithContext,
	) error
	DropEdgeTable(txnID common.TxnID, name string, logger common.ITxnLoggerWithContext) error

	CreateIndex(
		txnID common.TxnID,
		name string,
		tableName string,
		tableKind string,
		columns []string,
		keyBytesCnt uint32,
		logger common.ITxnLoggerWithContext,
	) error
	DropIndex(txnID common.TxnID, name string, logger common.ITxnLoggerWithContext) error
}

type Executor struct {
	catalog   engine.SystemCatalog
	pool      bufferpool.BufferPool
	se        StorageEngine
	txnTicker atomic.Uint64
	locker    txns.ILockManager
	logger    common.ITxnLogger
}

func New(
	catalog engine.SystemCatalog,
	se StorageEngine,
	locker txns.ILockManager,
	logger common.ITxnLogger,
) *Executor {
	return &Executor{
		catalog: catalog,
		se:      se,
		locker:  locker,
		logger:  logger,
	}
}
