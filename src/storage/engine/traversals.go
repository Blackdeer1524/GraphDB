package engine

import (
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

// NewAggregationAssociativeArray implements storage.StorageEngine.
func (s *StorageEngine) NewAggregationAssociativeArray(
	common.TxnID,
) (storage.AssociativeArray[storage.VertexID, float64], error) {
	panic("unimplemented")
}

// NewBitMap implements storage.StorageEngine.
func (s *StorageEngine) NewBitMap(common.TxnID) (storage.BitMap, error) {
	panic("unimplemented")
}

// NewQueue implements storage.StorageEngine.
func (s *StorageEngine) NewQueue(common.TxnID) (storage.Queue, error) {
	panic("unimplemented")
}

// AllVerticesWithValue implements storage.StorageEngine.
func (s *StorageEngine) AllVerticesWithValue(
	t common.TxnID,
	field string,
	value []byte,
) (storage.VerticesIter, error) {
	panic("unimplemented")
}

// CountOfFilteredEdges implements storage.StorageEngine.
func (s *StorageEngine) CountOfFilteredEdges(
	t common.TxnID,
	v storage.VertexID,
	f storage.EdgeFilter,
) (uint64, error) {
	panic("unimplemented")
}

// GetAllVertices implements storage.StorageEngine.
func (s *StorageEngine) GetAllVertices(
	t common.TxnID,
	vertTableID common.FileID,
) (storage.VerticesIter, error) {
	panic("unimplemented")
}

// GetNeighborsWithEdgeFilter implements storage.StorageEngine.
func (s *StorageEngine) GetNeighborsWithEdgeFilter(
	t common.TxnID,
	v storage.VertexID,
	filter storage.EdgeFilter,
) (storage.VerticesIter, error) {
	panic("unimplemented")
}

// Neighbours implements storage.StorageEngine.
func (s *StorageEngine) Neighbours(
	txnID common.TxnID,
	vID storage.VertexID,
	vertTableToken *txns.FileLockToken,
	vertIndex storage.Index,
	logger common.ITxnLoggerWithContext,
) (storage.NeighborIter, error) {
	return newNeighboursIter(s, s.pool, vID, vertTableToken, vertIndex, s.locker, logger), nil
}
