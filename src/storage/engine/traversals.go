package engine

import (
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

// NewAggregationAssociativeArray implements storage.StorageEngine.
func (s *StorageEngine) NewAggregationAssociativeArray(
	common.TxnID,
) (storage.AssociativeArray[storage.VertexInternalID, float64], error) {
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
	cToken := vertTableToken.GetCatalogLockToken()
	vertTableMeta, err := s.GetVertexTableMetaByFileID(vertTableToken.GetFileID(), cToken)
	if err != nil {
		return nil, err
	}

	var valueVertexFilter storage.VertexFilter = func(v *storage.Vertex) bool {
		columnValue, ok := v.Data[field]
		if !ok {
			return false
		}
		return storage.CmpColumnValue(columnValue, value)
	}

	iter := newVertexTableScanIter(
		s,
		s.pool,
		valueVertexFilter,
		vertTableToken,
		vertTableMeta.Schema,
		s.locker,
	)
	return iter, nil
}

func (s *StorageEngine) CountOfFilteredEdges(
	t common.TxnID,
	v storage.VertexInternalID,
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
	vertTableToken *txns.FileLockToken,
) (storage.VerticesIter, error) {
	cToken := vertTableToken.GetCatalogLockToken()
	vertTableMeta, err := s.GetVertexTableMetaByFileID(vertTableToken.GetFileID(), cToken)
	if err != nil {
		return nil, err
	}

	iter := newVertexTableScanIter(
		s,
		s.pool,
		storage.AllowAllVerticesFilter,
		vertTableToken,
		vertTableMeta.Schema,
		s.locker,
	)
	return iter, nil
}

func (s *StorageEngine) GetNeighborsWithEdgeFilter(
	t common.TxnID,
	v storage.VertexInternalID,
	vertTableToken *txns.FileLockToken,
	vertIndex storage.Index,
	edgeFilter storage.EdgeFilter,
	logger common.ITxnLoggerWithContext,
) (storage.VerticesIter, error) {
	iter := newNeighbourVertexIter(
		s,
		v,
		vertTableToken,
		vertIndex,
		storage.AllowAllVerticesFilter,
		edgeFilter,
		s.locker,
		logger,
	)
	return iter, nil
}

func (s *StorageEngine) Neighbours(
	txnID common.TxnID,
	vID storage.VertexInternalID,
	vertTableToken *txns.FileLockToken,
	vertIndex storage.Index,
	logger common.ITxnLoggerWithContext,
) (storage.NeighborIDIter, error) {
	return newNeighbourVertexIDsIter(
		s,
		vID,
		vertTableToken,
		vertIndex,
		storage.AllowAllEdgesFilter,
		logger,
	), nil
}
