package query

import (
	"sync/atomic"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

type StorageEngine interface {
	NewQueue(common.TxnID) (storage.Queue, error)
	NewAggregationAssociativeArray(
		common.TxnID,
	) (storage.AssociativeArray[storage.VertexID, float64], error)
	NewBitMap(common.TxnID) (storage.BitMap, error)
	Neighbours(t common.TxnID, v storage.VertexID) (storage.NeighborIter, error)
	GetVertexRID(t common.TxnID, v storage.VertexID) (storage.VertexIDWithRID, error)
	AllVerticesWithValue(t common.TxnID, field string, value []byte) (storage.VerticesIter, error)
	CountOfFilteredEdges(t common.TxnID, v storage.VertexID, f storage.EdgeFilter) (uint64, error)
	GetAllVertices(t common.TxnID) (storage.VerticesIter, error)
	GetNeighborsWithEdgeFilter(
		t common.TxnID,
		v storage.VertexID,
		filter storage.EdgeFilter,
	) (storage.VerticesIter, error)
}

type Executor struct {
	se        StorageEngine
	txnTicker atomic.Uint64
	locker    *txns.LockManager
	logger    common.ITxnLogger
}

func New(
	se StorageEngine,
	locker *txns.LockManager,
	logger common.ITxnLogger,
) *Executor {
	return &Executor{
		se:     se,
		locker: locker,
		logger: logger,
	}
}
