package query

import (
	"errors"
	"fmt"
	"math"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/storage/datastructures/inmemory"
	"github.com/Blackdeer1524/GraphDB/src/storage/engine"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

// traverseNeighborsWithDepth enqueues unvisited neighbors at next depth if <= targetDepth.
func (e *Executor) traverseNeighborsWithDepth(
	t common.TxnID,
	v storage.VertexSystemIDWithDepthAndRID,
	targetDepth uint32,
	vertexFilter storage.VertexFilter,
	seen storage.BitMap,
	q storage.Queue,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) (err error) {
	curDepth := v.D

	startFileToken := txns.NewNilFileLockToken(cToken, v.R.FileID)
	vertIndex, err := e.se.GetVertexTableSystemIndex(t, v.R.FileID, cToken, logger)
	if err != nil {
		return fmt.Errorf("failed to get vertex table internal index: %w", err)
	}
	defer vertIndex.Close()

	it, err := e.se.GetNeighborsWithEdgeFilter(
		t,
		v.V,
		startFileToken,
		vertIndex,
		storage.AllowAllEdgesFilter,
		logger,
	)
	if err != nil {
		return err
	}

	defer func() {
		err1 := it.Close()
		if err1 != nil {
			err = errors.Join(err, err1)
		}
	}()

	for u := range it.Seq() {
		vRID, vert, err := u.Destruct()
		if err != nil {
			return err
		}

		if !vertexFilter(&vert) {
			continue
		}

		var ok bool

		vID := storage.VertexID{SystemID: vert.ID, TableID: vRID.FileID}
		ok, err = seen.Get(vID)
		if err != nil {
			return fmt.Errorf("failed to get vertex visited status: %w", err)
		}

		// if visited
		if ok {
			continue
		}

		err = seen.Set(vID, true)
		if err != nil {
			return fmt.Errorf("failed to set vertex visited status: %w", err)
		}

		if curDepth == ^uint32(0) {
			return errors.New("depth overflow")
		}

		nd := curDepth + 1

		if nd <= targetDepth {
			err = q.Enqueue(
				storage.VertexSystemIDWithDepthAndRID{
					V: vert.ID,
					R: vRID,
					D: nd,
				},
			)
			if err != nil {
				return fmt.Errorf("failed to enqueue vertex: %w", err)
			}
		}
	}

	return nil
}

func (e *Executor) bfsWithDepth(
	tx common.TxnID,
	start storage.VertexSystemIDWithRID,
	targetDepth uint32,
	vertexFilter storage.VertexFilter,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) (result []storage.VertexSystemIDWithRID, err error) {
	result = make([]storage.VertexSystemIDWithRID, 0)

	q, err := e.se.NewQueue(tx)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, q.Close())
	}()

	seen, err := e.se.NewBitMap(tx)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, seen.Close())
	}()

	vID := storage.VertexID{SystemID: start.V, TableID: start.R.FileID}
	err = seen.Set(vID, true)
	if err != nil {
		return nil, fmt.Errorf("failed to set start vertex: %w", err)
	}

	err = q.Enqueue(storage.VertexSystemIDWithDepthAndRID{V: start.V, D: 0, R: start.R})
	if err != nil {
		return nil, fmt.Errorf("failed to enqueue start vertex: %w", err)
	}

	for {
		v, err := q.Dequeue()
		if err != nil {
			if errors.Is(err, storage.ErrQueueEmpty) {
				break
			}

			return nil, fmt.Errorf("failed to dequeue: %w", err)
		}

		if v.D > targetDepth {
			return nil, errors.New("depth overflow")
		}

		if v.D == targetDepth {
			result = append(result, storage.VertexSystemIDWithRID{V: v.V, R: v.R})

			continue
		}

		err = e.traverseNeighborsWithDepth(
			tx,
			v,
			targetDepth,
			vertexFilter,
			seen,
			q,
			cToken,
			logger,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to traverse neighbors: %w", err)
		}
	}

	return result, nil
}

// GetVertexesOnDepth is the first query from SOW. It returns all vertexes on a given depth.
// We will use BFS on graph because DFS cannot calculate right depth on graphs (except trees).
func (e *Executor) GetVertexesOnDepth(
	txnID common.TxnID,
	vertTableName string,
	start storage.VertexSystemID,
	targetDepth uint32,
	vertexFilter storage.VertexFilter,
	logger common.ITxnLoggerWithContext,
) (r []storage.VertexSystemIDWithRID, err error) {
	cToken := txns.NewNilCatalogLockToken(txnID)
	vertTableMeta, err := e.se.GetVertexTableMeta(vertTableName, cToken)
	if err != nil {
		return nil, fmt.Errorf("failed to get vertex table meta: %w", err)
	}

	var st storage.VertexSystemIDWithRID
	index, err := e.se.GetVertexTableSystemIndex(txnID, vertTableMeta.FileID, cToken, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to get vertex table internal index: %w", err)
	}
	defer index.Close()

	st, err = engine.GetVertexRID(txnID, start, index)
	if err != nil {
		return nil, fmt.Errorf("failed to get start vertex: %w", err)
	}

	var res []storage.VertexSystemIDWithRID

	res, err = e.bfsWithDepth(txnID, st, targetDepth, vertexFilter, cToken, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to bfsWithDepth: %w", err)
	}

	return res, nil
}

// GetAllVertexesWithFieldValue is the second query from SOW.
// It returns all vertexes with a given field value.
func (e *Executor) GetAllVertexesWithFieldValue(
	txnID common.TxnID,
	vertTableName string,
	field string,
	value []byte,
	logger common.ITxnLoggerWithContext,
) (res []storage.Vertex, err error) {
	var verticesIter storage.VerticesIter

	cToken := txns.NewNilCatalogLockToken(txnID)
	tableMeta, err := e.se.GetVertexTableMeta(vertTableName, cToken)
	if err != nil {
		return nil, fmt.Errorf("failed to get vertex table meta: %w", err)
	}

	vertIndex, err := e.se.GetVertexTableSystemIndex(txnID, tableMeta.FileID, cToken, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to get vertex table internal index: %w", err)
	}
	defer vertIndex.Close()

	vertToken := txns.NewNilFileLockToken(cToken, tableMeta.FileID)
	verticesIter, err = e.se.AllVerticesWithValue(txnID, vertToken, vertIndex, logger, field, value)
	if err != nil {
		return nil, fmt.Errorf("failed to get vertices iterator: %w", err)
	}
	defer func() {
		err = errors.Join(err, verticesIter.Close())
	}()

	res = make([]storage.Vertex, 0, 1024)

	for v := range verticesIter.Seq() {
		_, vert, err := v.Destruct()
		if err != nil {
			return nil, fmt.Errorf("failed to destruct vertex: %w", err)
		}
		res = append(res, vert)
	}

	return res, nil
}

// GetAllVertexesWithFieldValue2 is the third query from SOW (extended second query).
// It returns all vertexes with a given field value and uses filter on edges (degree of vertex
// with condition on edge).
func (e *Executor) GetAllVertexesWithFieldValue2(
	txnID common.TxnID,
	vertTableName string,
	field string,
	value []byte,
	filter storage.EdgeFilter,
	cutoffDegree uint64,
	logger common.ITxnLoggerWithContext,
) (res []storage.Vertex, err error) {
	var verticesIter storage.VerticesIter

	cToken := txns.NewNilCatalogLockToken(txnID)
	tableMeta, err := e.se.GetVertexTableMeta(vertTableName, cToken)
	if err != nil {
		return nil, fmt.Errorf("failed to get vertex table meta: %w", err)
	}

	vertIndex, err := e.se.GetVertexTableSystemIndex(txnID, tableMeta.FileID, cToken, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to get vertex table internal index: %w", err)
	}
	defer vertIndex.Close()

	vertToken := txns.NewNilFileLockToken(cToken, tableMeta.FileID)
	verticesIter, err = e.se.AllVerticesWithValue(txnID, vertToken, vertIndex, logger, field, value)
	if err != nil {
		return nil, fmt.Errorf("failed to get vertices iterator: %w", err)
	}
	defer func() {
		err1 := verticesIter.Close()
		if err1 != nil {
			err = errors.Join(err, fmt.Errorf("close failed: %w", err1))
		}
	}()

	res = make([]storage.Vertex, 0, 1024)

	for v := range verticesIter.Seq() {
		_, v, err := v.Destruct()
		if err != nil {
			return nil, fmt.Errorf("failed to destruct vertex: %w", err)
		}

		var cnt uint64
		cnt, err = e.se.CountOfFilteredEdges(txnID, v.ID, vertToken, vertIndex, logger, filter)
		if err != nil {
			return nil, fmt.Errorf("failed to count edges: %w", err)
		}

		if cutoffDegree > cnt {
			continue
		}

		res = append(res, v)
	}

	return res, nil
}

func (e *Executor) sumAttributeOverProperNeighbors(
	txnID common.TxnID,
	v *storage.Vertex,
	vertTableToken *txns.FileLockToken,
	vertIndex storage.Index,
	field string,
	edgeFilter storage.EdgeFilter,
	logger common.ITxnLoggerWithContext,
) (r float64, err error) {
	var res float64

	nIter, err := e.se.GetNeighborsWithEdgeFilter(
		txnID,
		v.ID,
		vertTableToken,
		vertIndex,
		edgeFilter,
		logger,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to get edges iterator: %w", err)
	}
	defer func() {
		err1 := nIter.Close()
		if err1 != nil {
			err = errors.Join(err, fmt.Errorf("close failed: %w", err1))
		}
	}()

	for nbIter := range nIter.Seq() {
		_, nVert, err := nbIter.Destruct()
		if err != nil {
			return 0, fmt.Errorf("failed to destruct neighbor: %w", err)
		}

		data, ok := nVert.Data[field]
		if !ok {
			continue
		}

		d, err := storage.ColumnToFloat(data)
		if err != nil {
			err = fmt.Errorf(
				"failed to convert field %s of vertex %v to float64: %w",
				field,
				nVert.ID,
				err,
			)
			return 0, err
		}
		res += d
	}

	return res, nil
}

// SumNeighborAttributes is the forth query from SOW. For each vertex it computes
// the sum of a given attribute over its neighboring vertices, subject to a constraint on the edge
// or attribute value (e.g., only include neighbors whose attribute exceeds a given threshold).
func (e *Executor) SumNeighborAttributes(
	txnID common.TxnID,
	vertTableName string,
	field string,
	filter storage.EdgeFilter,
	pred storage.SumNeighborAttributesFilter,
	logger common.ITxnLoggerWithContext,
) (r storage.AssociativeArray[storage.VertexID, float64], err error) {
	r, err = e.se.NewAggregationAssociativeArray(txnID)
	if err != nil {
		return nil, fmt.Errorf("failed to create aggregation associative array: %w", err)
	}

	var verticesIter storage.VerticesIter

	cToken := txns.NewNilCatalogLockToken(txnID)
	tableMeta, err := e.se.GetVertexTableMeta(vertTableName, cToken)
	if err != nil {
		return nil, fmt.Errorf("failed to get vertex table meta: %w", err)
	}

	vertToken := txns.NewNilFileLockToken(cToken, tableMeta.FileID)
	verticesIter, err = e.se.GetAllVertices(txnID, vertToken)
	if err != nil {
		return nil, fmt.Errorf("failed to get vertices iterator: %w", err)
	}
	defer func() {
		err1 := verticesIter.Close()
		if err1 != nil {
			err = errors.Join(err, fmt.Errorf("close failed: %w", err1))
		}
	}()

	lastVertexTableID := common.NilFileID
	var lastVertexToken *txns.FileLockToken
	var lastVertexIndex storage.Index
	defer func() {
		if lastVertexIndex != nil {
			lastVertexIndex.Close()
		}
	}()
	for v := range verticesIter.Seq() {
		vRID, nVert, err := v.Destruct()
		if err != nil {
			return nil, fmt.Errorf("failed to destruct vertex: %w", err)
		}

		if vRID.FileID != lastVertexTableID {
			lastVertexTableID = vRID.FileID
			lastVertexToken = txns.NewNilFileLockToken(cToken, vRID.FileID)
			if lastVertexIndex != nil {
				lastVertexIndex.Close()
			}
			lastVertexIndex, err = e.se.GetVertexTableSystemIndex(
				txnID,
				vRID.FileID,
				cToken,
				logger,
			)
			if err != nil {
				return nil, fmt.Errorf("failed to get vertex table internal index: %w", err)
			}
		}

		var res float64
		res, err = e.sumAttributeOverProperNeighbors(
			txnID,
			&nVert,
			lastVertexToken,
			lastVertexIndex,
			field,
			filter,
			logger,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to sum attribute over neighbors of vertex %v: %w",
				vRID,
				err,
			)
		}

		if !pred(res) {
			continue
		}

		vertexID := storage.VertexID{
			SystemID: nVert.ID,
			TableID:  vRID.FileID,
		}
		err = r.Set(vertexID, res)
		if err != nil {
			return nil, fmt.Errorf("failed to set value in aggregation associative array: %w", err)
		}
	}

	return r, nil
}

func (e *Executor) countCommonNeighbors(
	txnID common.TxnID,
	left storage.VertexSystemID,
	leftNeighbours storage.AssociativeArray[storage.VertexID, struct{}],
	leftFileToken *txns.FileLockToken,
	leftIndex storage.Index,
	logger common.ITxnLoggerWithContext,
) (r uint64, err error) {
	var rightNeighboursIter storage.NeighborIDIter

	rightNeighboursIter, err = e.se.Neighbours(txnID, left, leftFileToken, leftIndex, logger)
	if err != nil {
		return 0, fmt.Errorf("failed to get neighbors of vertex %v: %w", left, err)
	}
	defer func() {
		err1 := rightNeighboursIter.Close()
		if err1 != nil {
			err = errors.Join(err, fmt.Errorf("close failed: %w", err1))
		}
	}()

	for right := range rightNeighboursIter.Seq() {
		rightRID, err := right.Destruct()
		if err != nil {
			return 0, fmt.Errorf("failed to destruct vertex: %w", err)
		}

		rightVertID := storage.VertexID{SystemID: rightRID.V, TableID: rightRID.R.FileID}
		if rightVertID.SystemID == left && rightVertID.TableID == leftFileToken.GetFileID() {
			continue
		}

		_, ok := leftNeighbours.Get(rightVertID)
		if !ok {
			continue
		}
		r++
	}

	return r, nil
}

func (e *Executor) getVertexTriangleCount(
	txnID common.TxnID,
	vertSysID storage.VertexSystemID,
	vertTableToken *txns.FileLockToken,
	vertIndex storage.Index,
	logger common.ITxnLoggerWithContext,
) (r uint64, err error) {
	r = 0
	leftNeighboursIter, err := e.se.Neighbours(
		txnID,
		vertSysID,
		vertTableToken,
		vertIndex,
		logger,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to get neighbors of vertex %v: %w", vertSysID, err)
	}
	defer func() {
		err1 := leftNeighboursIter.Close()
		if err1 != nil {
			err = errors.Join(err, fmt.Errorf("close failed: %w", err1))
		}
	}()

	leftNeighbours := inmemory.NewInMemoryAssociativeArray[storage.VertexID, struct{}]()
	for l := range leftNeighboursIter.Seq() {
		vSystemIDwithRID, err := l.Destruct()
		if err != nil {
			return 0, fmt.Errorf("failed to get neighbor vertex: %w", err)
		}

		nVertID := storage.VertexID{
			SystemID: vSystemIDwithRID.V,
			TableID:  vSystemIDwithRID.R.FileID,
		}
		if nVertID.SystemID == vertSysID && nVertID.TableID == vertTableToken.GetFileID() {
			continue
		}

		err = leftNeighbours.Set(nVertID, struct{}{})
		if err != nil {
			err = fmt.Errorf("failed to set value in left neighbors associative array: %w", err)
			return 0, err
		}
	}

	cToken := vertTableToken.GetCatalogLockToken()
	leftNeighbours.Seq(func(left storage.VertexID, _ struct{}) bool {
		var add uint64
		var leftIndex storage.Index

		leftFileToken := txns.NewNilFileLockToken(cToken, left.TableID)
		leftIndex, err = e.se.GetVertexTableSystemIndex(txnID, left.TableID, cToken, logger)
		if err != nil {
			return false
		}
		defer leftIndex.Close()

		add, err = e.countCommonNeighbors(
			txnID,
			left.SystemID,
			leftNeighbours,
			leftFileToken,
			leftIndex,
			logger,
		)
		if err != nil {
			err = fmt.Errorf("failed to count common neighbors: %w", err)
			return false
		}

		if math.MaxUint64-r < add {
			err = errors.New("triangle count is bigger than uint64")
			return false
		}

		r += add
		return true
	})

	return
}

// GetAllTriangles is the fifth query from SOW.
// It returns all triangles in the graph (ignoring edge orientation).
func (e *Executor) GetAllTriangles(
	txnID common.TxnID,
	vertTableName string,
	logger common.ITxnLoggerWithContext,
) (r uint64, err error) {
	cToken := txns.NewNilCatalogLockToken(txnID)
	tableMeta, err := e.se.GetVertexTableMeta(vertTableName, cToken)
	if err != nil {
		return 0, fmt.Errorf("failed to get vertex table meta: %w", err)
	}
	vertIndex, err := e.se.GetVertexTableSystemIndex(txnID, tableMeta.FileID, cToken, logger)
	if err != nil {
		return 0, fmt.Errorf("failed to get vertex table internal index: %w", err)
	}
	defer vertIndex.Close()

	vertToken := txns.NewNilFileLockToken(cToken, tableMeta.FileID)
	verticesIter, err := e.se.GetAllVertices(txnID, vertToken)
	if err != nil {
		return 0, fmt.Errorf("failed to get vertices iterator: %w", err)
	}
	defer func() {
		err1 := verticesIter.Close()
		if err1 != nil {
			err = errors.Join(err, fmt.Errorf("close failed: %w", err1))
		}
	}()

	r = 0
	for v := range verticesIter.Seq() {
		_, vert, err := v.Destruct()
		if err != nil {
			return 0, fmt.Errorf("failed to destruct vertex: %w", err)
		}
		var add uint64

		add, err = e.getVertexTriangleCount(txnID, vert.ID, vertToken, vertIndex, logger)
		if err != nil {
			return 0, fmt.Errorf("failed to get triangle count of vertex %v: %w", vert.ID, err)
		}

		if math.MaxUint64-r < add {
			return 0, fmt.Errorf("triangle count is bigger than uint64")
		}

		r += add
	}

	return r / 6, nil
}
