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

func (e *Executor) newTxnID() common.TxnID {
	return common.TxnID(e.txnTicker.Add(1))
}

// traverseNeighborsWithDepth enqueues unvisited neighbors at next depth if <= targetDepth.
func (e *Executor) traverseNeighborsWithDepth(
	t common.TxnID,
	v storage.VertexInternalIDWithDepthAndRID,
	targetDepth uint32,
	seen storage.BitMap,
	q storage.Queue,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) (err error) {
	curDepth := v.D

	startFileToken := txns.NewNilFileLockToken(cToken, v.R.FileID)
	vertIndex, err := e.se.GetVertexTableInternalIndex(t, v.R.FileID, cToken, logger)
	if err != nil {
		return fmt.Errorf("failed to get vertex table internal index: %w", err)
	}

	it, err := e.se.Neighbours(t, v.V, startFileToken, vertIndex, logger)
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
		vIntID, err := u.Destruct()
		if err != nil {
			return err
		}

		var ok bool

		vID := storage.VertexID{InternalID: vIntID.V, TableID: vIntID.R.FileID}
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
				storage.VertexInternalIDWithDepthAndRID{V: vIntID.V, D: nd, R: vIntID.R},
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
	start storage.VertexInternalIDWithRID,
	targetDepth uint32,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) (result []storage.VertexInternalIDWithRID, err error) {
	result = make([]storage.VertexInternalIDWithRID, 0)

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

	vID := storage.VertexID{InternalID: start.V, TableID: start.R.FileID}
	err = seen.Set(vID, true)
	if err != nil {
		return nil, fmt.Errorf("failed to set start vertex: %w", err)
	}

	err = q.Enqueue(storage.VertexInternalIDWithDepthAndRID{V: start.V, D: 0, R: start.R})
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
			result = append(result, storage.VertexInternalIDWithRID{V: v.V, R: v.R})

			continue
		}

		err = e.traverseNeighborsWithDepth(tx, v, targetDepth, seen, q, cToken, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to traverse neighbors: %w", err)
		}
	}

	return result, nil
}

// GetVertexesOnDepth is the first query from SOW. It returns all vertexes on a given depth.
// We will use BFS on graph because DFS cannot calculate right depth on graphs (except trees).
func (e *Executor) GetVertexesOnDepth(
	vertTableID common.FileID,
	start storage.VertexInternalID,
	targetDepth uint32,
) (r []storage.VertexInternalIDWithRID, err error) {
	if e.se == nil {
		return nil, errors.New("storage engine is nil")
	}

	tx := e.newTxnID()
	defer e.locker.Unlock(tx)

	logger := e.logger.WithContext(tx)

	if err := logger.AppendBegin(); err != nil {
		return nil, fmt.Errorf("failed to append begin: %w", err)
	}

	defer func() {
		if err != nil {
			if err1 := logger.AppendAbort(); err1 != nil {
				err = errors.Join(err, fmt.Errorf("append abort failed: %w", err1))
			} else {
				logger.Rollback()
				err = errors.Join(err, logger.AppendTxnEnd())
			}
		} else {
			if err = logger.AppendCommit(); err != nil {
				err = fmt.Errorf("failed to append commit: %w", err)
			} else if err = logger.AppendTxnEnd(); err != nil {
				err = fmt.Errorf("failed to append txn end: %w", err)
			}
		}
	}()

	var st storage.VertexInternalIDWithRID
	cToken := txns.NewNilCatalogLockToken(tx)
	index, err := e.se.GetVertexTableInternalIndex(tx, vertTableID, cToken, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to get vertex table internal index: %w", err)
	}

	st, err = engine.GetVertexRID(tx, start, index)
	if err != nil {
		return nil, fmt.Errorf("failed to get start vertex: %w", err)
	}

	var res []storage.VertexInternalIDWithRID

	res, err = e.bfsWithDepth(tx, st, targetDepth, cToken, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to bfsWithDepth: %w", err)
	}

	return res, nil
}

// GetAllVertexesWithFieldValue is the second query from SOW.
// It returns all vertexes with a given field value.
func (e *Executor) GetAllVertexesWithFieldValue(
	vertTableName string,
	field string,
	value []byte,
) (res []storage.Vertex, err error) {
	if e.se == nil {
		return nil, errors.New("storage engine is nil")
	}

	txnID := e.newTxnID()
	defer e.locker.Unlock(txnID)

	logger := e.logger.WithContext(txnID)

	if err := logger.AppendBegin(); err != nil {
		return nil, fmt.Errorf("failed to append begin: %w", err)
	}

	defer func() {
		if err != nil {
			if err1 := logger.AppendAbort(); err1 != nil {
				err = errors.Join(err, fmt.Errorf("append abort failed: %w", err1))
			} else {
				logger.Rollback()
				err = errors.Join(err, logger.AppendTxnEnd())
			}
		} else {
			if err = logger.AppendCommit(); err != nil {
				err = fmt.Errorf("failed to append commit: %w", err)
			} else if err = logger.AppendTxnEnd(); err != nil {
				err = fmt.Errorf("failed to append txn end: %w", err)
			}
		}
	}()

	var verticesIter storage.VerticesIter

	cToken := txns.NewNilCatalogLockToken(txnID)
	tableMeta, err := e.se.GetVertexTableMeta(vertTableName, cToken)
	if err != nil {
		return nil, fmt.Errorf("failed to get vertex table meta: %w", err)
	}

	vertIndex, err := e.se.GetVertexTableInternalIndex(txnID, tableMeta.FileID, cToken, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to get vertex table internal index: %w", err)
	}

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
	vertTableName string,
	field string,
	value []byte,
	filter storage.EdgeFilter,
	cutoffDegree uint64,
) (res []storage.Vertex, err error) {
	if e.se == nil {
		return nil, errors.New("storage engine is nil")
	}

	txnID := e.newTxnID()
	defer e.locker.Unlock(txnID)

	logger := e.logger.WithContext(txnID)

	if err := logger.AppendBegin(); err != nil {
		return nil, fmt.Errorf("failed to append begin: %w", err)
	}

	defer func() {
		if err != nil {
			if err1 := logger.AppendAbort(); err1 != nil {
				err = errors.Join(err, fmt.Errorf("append abort failed: %w", err1))
				res = nil
			} else {
				logger.Rollback()
				err = errors.Join(err, logger.AppendTxnEnd())
				res = nil
			}
		} else {
			if err = logger.AppendCommit(); err != nil {
				err = fmt.Errorf("failed to append commit: %w", err)
				res = nil
			} else if err = logger.AppendTxnEnd(); err != nil {
				err = fmt.Errorf("failed to append txn end: %w", err)
				res = nil
			}
		}
	}()

	var verticesIter storage.VerticesIter

	cToken := txns.NewNilCatalogLockToken(txnID)
	tableMeta, err := e.se.GetVertexTableMeta(vertTableName, cToken)
	if err != nil {
		return nil, fmt.Errorf("failed to get vertex table meta: %w", err)
	}

	vertIndex, err := e.se.GetVertexTableInternalIndex(txnID, tableMeta.FileID, cToken, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to get vertex table internal index: %w", err)
	}

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
	vertTableName string,
	field string,
	filter storage.EdgeFilter,
	pred storage.SumNeighborAttributesFilter,
) (r storage.AssociativeArray[storage.VertexID, float64], err error) {
	if e.se == nil {
		return nil, errors.New("storage engine is nil")
	}

	txnID := e.newTxnID()
	defer e.locker.Unlock(txnID)

	logger := e.logger.WithContext(txnID)

	if err := logger.AppendBegin(); err != nil {
		return nil, fmt.Errorf("failed to append begin: %w", err)
	}

	defer func() {
		if err != nil {
			if err1 := logger.AppendAbort(); err1 != nil {
				err = errors.Join(err, fmt.Errorf("append abort failed: %w", err1))
			} else {
				logger.Rollback()
				err = errors.Join(err, logger.AppendTxnEnd())
			}
		} else {
			if err = logger.AppendCommit(); err != nil {
				err = fmt.Errorf("failed to append commit: %w", err)
			} else if err = logger.AppendTxnEnd(); err != nil {
				err = fmt.Errorf("failed to append txn end: %w", err)
			}
		}
	}()

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
	for v := range verticesIter.Seq() {
		vRID, nVert, err := v.Destruct()
		if err != nil {
			return nil, fmt.Errorf("failed to destruct vertex: %w", err)
		}

		if vRID.FileID != lastVertexTableID {
			lastVertexTableID = vRID.FileID
			lastVertexToken = txns.NewNilFileLockToken(cToken, vRID.FileID)
			lastVertexIndex, err = e.se.GetVertexTableInternalIndex(
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
			InternalID: nVert.ID,
			TableID:    vRID.FileID,
		}
		err = r.Set(vertexID, res)
		if err != nil {
			return nil, fmt.Errorf("failed to set value in aggregation associative array: %w", err)
		}
	}

	return r, nil
}

func (e *Executor) countCommonNeighbors(
	tx common.TxnID,
	left storage.VertexInternalID,
	leftNeighbours storage.AssociativeArray[storage.VertexID, struct{}],
	leftFileToken *txns.FileLockToken,
	leftIndex storage.Index,
	logger common.ITxnLoggerWithContext,
) (r uint64, err error) {
	var rightNeighboursIter storage.NeighborIDIter

	rightNeighboursIter, err = e.se.Neighbours(tx, left, leftFileToken, leftIndex, logger)
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

		rightVertID := storage.VertexID{InternalID: rightRID.V, TableID: rightRID.R.FileID}
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
	vInternalID storage.VertexInternalID,
	vertTableToken *txns.FileLockToken,
	vertIndex storage.Index,
	logger common.ITxnLoggerWithContext,
) (r uint64, err error) {
	r = 0
	leftNeighboursIter, err := e.se.Neighbours(
		txnID,
		vInternalID,
		vertTableToken,
		vertIndex,
		logger,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to get neighbors of vertex %v: %w", vInternalID, err)
	}
	defer func() {
		err1 := leftNeighboursIter.Close()
		if err1 != nil {
			err = errors.Join(err, fmt.Errorf("close failed: %w", err1))
		}
	}()

	leftNeighbours := inmemory.NewInMemoryAssociativeArray[storage.VertexID, struct{}]()
	for l := range leftNeighboursIter.Seq() {
		vInternalIDwithRID, err := l.Destruct()
		if err != nil {
			return 0, fmt.Errorf("failed to get neighbor vertex: %w", err)
		}

		vertID := storage.VertexID{
			InternalID: vInternalIDwithRID.V,
			TableID:    vInternalIDwithRID.R.FileID,
		}
		err = leftNeighbours.Set(vertID, struct{}{})
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
		leftIndex, err = e.se.GetVertexTableInternalIndex(txnID, left.TableID, cToken, logger)
		if err != nil {
			return false
		}

		add, err = e.countCommonNeighbors(
			txnID,
			left.InternalID,
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
func (e *Executor) GetAllTriangles(vertTableName string) (r uint64, err error) {
	txnID := e.newTxnID()
	defer e.locker.Unlock(txnID)

	logger := e.logger.WithContext(txnID)
	if err := logger.AppendBegin(); err != nil {
		return 0, fmt.Errorf("failed to append begin: %w", err)
	}

	defer func() {
		if err != nil {
			if err1 := logger.AppendAbort(); err1 != nil {
				err = errors.Join(err, fmt.Errorf("append abort failed: %w", err1))
			} else {
				logger.Rollback()
				err = errors.Join(err, logger.AppendTxnEnd())
			}
		} else {
			if err = logger.AppendCommit(); err != nil {
				err = fmt.Errorf("failed to append commit: %w", err)
			} else if err = logger.AppendTxnEnd(); err != nil {
				err = fmt.Errorf("failed to append txn end: %w", err)
			}
		}
	}()

	cToken := txns.NewNilCatalogLockToken(txnID)
	tableMeta, err := e.se.GetVertexTableMeta(vertTableName, cToken)
	if err != nil {
		return 0, fmt.Errorf("failed to get vertex table meta: %w", err)
	}
	vertIndex, err := e.se.GetVertexTableInternalIndex(txnID, tableMeta.FileID, cToken, logger)
	if err != nil {
		return 0, fmt.Errorf("failed to get vertex table internal index: %w", err)
	}
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
