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
	vertTableToken *txns.FileLockToken,
	vertIndex storage.Index,
	logger common.ITxnLoggerWithContext,
	field string,
	value []byte,
) (storage.VerticesIter, error) {
	panic("unimplemented")
}

func (s *StorageEngine) CountOfFilteredEdges(
	t common.TxnID,
	v storage.VertexID,
	vertTableToken *txns.FileLockToken,
	vertIndex storage.Index,
	logger common.ITxnLoggerWithContext,
	filter storage.EdgeFilter,
) (uint64, error) {
	edgesIter := newNeighboursEdgesIter(
		s,
		v,
		filter,
		vertTableToken,
		vertIndex,
		logger,
	)

	count := uint64(0)
	for range edgesIter.Seq() {
		count++
	}
	return count, nil
}

func (s *StorageEngine) GetAllVertices(
	t common.TxnID,
	vertTableID common.FileID,
) (storage.VerticesIter, error) {
	panic("unimplemented")
}

func (s *StorageEngine) GetNeighborsWithEdgeFilter(
	t common.TxnID,
	v storage.VertexID,
	vertTableToken *txns.FileLockToken,
	vertIndex storage.Index,
	edgeFilter storage.EdgeFilter,
	logger common.ITxnLoggerWithContext,
) (storage.VerticesIter, error) {
	return newNeighbourVertexIter(
		s,
		v,
		vertTableToken,
		vertIndex,
		storage.AllowAllVerticesFilter,
		edgeFilter,
		logger,
	), nil
}

func (s *StorageEngine) Neighbours(
	txnID common.TxnID,
	vID storage.VertexID,
	vertTableToken *txns.FileLockToken,
	vertIndex storage.Index,
	logger common.ITxnLoggerWithContext,
) (storage.NeighborIter, error) {
	return newNeighbourVertexIDsIter(
		s,
		vID,
		vertTableToken,
		vertIndex,
		storage.AllowAllEdgesFilter,
		logger,
	), nil
}
