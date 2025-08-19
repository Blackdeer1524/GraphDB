package query

import (
	"errors"
	"fmt"
	"math"

	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
)

// traverseNeighborsWithDepth enqueues unvisited neighbors at next depth if <= targetDepth.
func (e *Executor) traverseNeighborsWithDepth(t TxnID, v VertexWithDepthAndRID,
	targetDepth uint32, seen Visited, q Queue) (err error) {
	curDepth := v.D

	it, err := e.se.Neighbors(t, v.V)
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
		var ok bool

		ok, err = seen.Get(u.V)
		if err != nil {
			return fmt.Errorf("failed to get vertex visited status: %w", err)
		}

		// if visited
		if ok {
			continue
		}

		err = seen.Set(u.V, true)
		if err != nil {
			return fmt.Errorf("failed to set vertex visited status: %w", err)
		}

		if curDepth == ^uint32(0) {
			return errors.New("depth overflow")
		}

		nd := curDepth + 1

		if nd <= targetDepth {
			err = q.Enqueue(VertexWithDepthAndRID{V: u.V, D: nd, R: u.R})
			if err != nil {
				return fmt.Errorf("failed to enqueue vertex: %w", err)
			}
		}
	}

	return nil
}

func (e *Executor) bfsWithDepth(
	tx TxnID, start VertexIDWithRID,
	targetDepth uint32,
) (result []VertexIDWithRID, err error) {
	result = make([]VertexIDWithRID, 0)

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

	err = seen.Set(start.V, true)
	if err != nil {
		return nil, fmt.Errorf("failed to set start vertex: %w", err)
	}

	err = q.Enqueue(VertexWithDepthAndRID{V: start.V, D: 0, R: start.R})
	if err != nil {
		return nil, fmt.Errorf("failed to enqueue start vertex: %w", err)
	}

	for {
		var v VertexWithDepthAndRID

		v, err = q.Dequeue()
		if err != nil {
			if errors.Is(err, ErrQueueEmpty) {
				break
			}

			return nil, fmt.Errorf("failed to dequeue: %w", err)
		}

		if v.D > targetDepth {
			return nil, errors.New("depth overflow")
		}

		if v.D == targetDepth {
			result = append(result, VertexIDWithRID{V: v.V, R: v.R})

			continue
		}

		err = e.traverseNeighborsWithDepth(tx, v, targetDepth, seen, q)
		if err != nil {
			return nil, fmt.Errorf("failed to traverse neighbors: %w", err)
		}
	}

	return result, nil
}

// GetVertexesOnDepth is the first query from SOW. It returns all vertexes on a given depth.
// We will use BFS on graph because DFS cannot calculate right depth on graphs (except trees).
func (e *Executor) GetVertexesOnDepth(start VertexID, targetDepth uint32) (r []VertexIDWithRID, err error) {
	if e.se == nil {
		return nil, errors.New("storage engine is nil")
	}

	var tx TxnID

	tx, err = e.tm.Begin()
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			err1 := e.tm.RollbackTx(tx)

			assert.NoError(err1)
		}
	}()

	var st VertexIDWithRID

	st, err = e.se.GetVertexRID(tx, start)
	if err != nil {
		return nil, fmt.Errorf("failed to get start vertex: %w", err)
	}

	var res []VertexIDWithRID

	res, err = e.bfsWithDepth(tx, st, targetDepth)
	if err != nil {
		return nil, fmt.Errorf("failed to bfsWithDepth: %w", err)
	}

	err = e.tm.CommitTx(tx)
	if err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return res, nil
}

// GetAllVertexesWithFieldValue is the second query from SOW.
// It returns all vertexes with a given field value.
func (e *Executor) GetAllVertexesWithFieldValue(field string, value []byte) (res []*Vertex, err error) {
	if e.se == nil {
		return nil, errors.New("storage engine is nil")
	}

	var tx TxnID

	tx, err = e.tm.Begin()
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			err1 := e.tm.RollbackTx(tx)

			err = errors.Join(err, fmt.Errorf("rollback failed: %w", err1))
		}
	}()

	var verticesIter VerticesIter

	verticesIter, err = e.se.AllVerticesWithValue(tx, field, value)
	if err != nil {
		return nil, fmt.Errorf("failed to get vertices iterator: %w", err)
	}
	defer func() {
		err = errors.Join(err, verticesIter.Close())
	}()

	res = make([]*Vertex, 0, 1024)

	for v := range verticesIter.Seq() {
		res = append(res, v)
	}

	err = e.tm.CommitTx(tx)
	if err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return res, nil
}

// GetAllVertexesWithFieldValue2 is the third query from SOW (extended second query).
// It returns all vertexes with a given field value and uses filter on edges (degree of vertex
// with condition on edge).
func (e *Executor) GetAllVertexesWithFieldValue2(field string, value []byte,
	filter EdgeFilter, cutoffDegree uint64) (res []*Vertex, err error) {
	if e.se == nil {
		return nil, errors.New("storage engine is nil")
	}

	var tx TxnID

	tx, err = e.tm.Begin()
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			err1 := e.tm.RollbackTx(tx)

			if err1 != nil {
				err = errors.Join(err, fmt.Errorf("rollback failed: %w", err1))
			}
		}
	}()

	var verticesIter VerticesIter

	verticesIter, err = e.se.AllVerticesWithValue(tx, field, value)
	if err != nil {
		return nil, fmt.Errorf("failed to get vertices iterator: %w", err)
	}
	defer func() {
		err1 := verticesIter.Close()
		if err1 != nil {
			err = errors.Join(err, fmt.Errorf("close failed: %w", err1))
		}
	}()

	res = make([]*Vertex, 0, 1024)

	for v := range verticesIter.Seq() {
		var cnt uint64

		cnt, err = e.se.CountOfFilteredEdges(tx, v.ID, filter)
		if err != nil {
			return nil, fmt.Errorf("failed to count edges: %w", err)
		}

		if cutoffDegree > cnt {
			continue
		}

		res = append(res, v)
	}

	err = e.tm.CommitTx(tx)
	if err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return res, nil
}

func (e *Executor) sumAttributeOverProperNeighbors(tx TxnID, v *Vertex, field string, filter EdgeFilter) (r float64, err error) {
	var res float64

	var nIter VerticesIter

	nIter, err = e.se.GetNeighborsWithEdgeFilter(tx, v.ID, filter)
	if err != nil {
		return 0, fmt.Errorf("failed to get edges iterator: %w", err)
	}
	defer func() {
		err1 := nIter.Close()
		if err1 != nil {
			err = errors.Join(err, fmt.Errorf("close failed: %w", err1))
		}
	}()

	for niger := range nIter.Seq() {
		data, ok := niger.Data[field]
		if !ok {
			continue
		}

		d, ok := data.(float64)
		if !ok {
			return 0, fmt.Errorf("failed to convert field %s of vertex %v to float64", field, niger.ID)
		}

		res += d
	}

	return res, nil
}

// SumNeighborAttributes is the forth query from SOW. For each vertex it computes
// the sum of a given attribute over its neighboring vertices, subject to a constraint on the edge
// or attribute value (e.g., only include neighbors whose attribute exceeds a given threshold).
func (e *Executor) SumNeighborAttributes(field string, filter EdgeFilter,
	pred SumNeighborAttributesFilter) (r AssociativeArray[VertexID, float64], err error) {
	if e.se == nil {
		return nil, errors.New("storage engine is nil")
	}

	var tx TxnID

	tx, err = e.tm.Begin()
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			err1 := e.tm.RollbackTx(tx)
			if err1 != nil {
				err = errors.Join(err, fmt.Errorf("rollback failed: %w", err1))
			}
		}
	}()

	r, err = e.se.NewAggregationAssociativeArray(tx)
	if err != nil {
		return nil, fmt.Errorf("failed to create aggregation associative array: %w", err)
	}

	var verticesIter VerticesIter

	verticesIter, err = e.se.GetAllVertices(tx)
	if err != nil {
		return nil, fmt.Errorf("failed to get vertices iterator: %w", err)
	}
	defer func() {
		err1 := verticesIter.Close()
		if err1 != nil {
			err = errors.Join(err, fmt.Errorf("close failed: %w", err1))
		}
	}()

	for v := range verticesIter.Seq() {
		var res float64

		res, err = e.sumAttributeOverProperNeighbors(tx, v, field, filter)
		if err != nil {
			return nil, fmt.Errorf("failed to sum attribute over neighbors of vertex %v: %w", v.ID, err)
		}

		if !pred(res) {
			continue
		}

		err = r.Set(v.ID, res)
		if err != nil {
			return nil, fmt.Errorf("failed to set value in aggregation associative array: %w", err)
		}
	}

	err = e.tm.CommitTx(tx)
	if err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return r, nil
}

func (e *Executor) countCommonNeighbors(tx TxnID, left VertexID,
	leftNeighbors AssociativeArray[VertexID, struct{}]) (r uint64, err error) {
	var rightNeighborsIter NeighborIter

	rightNeighborsIter, err = e.se.Neighbors(tx, left)
	if err != nil {
		return 0, fmt.Errorf("failed to get neighbors of vertex %v: %w", left, err)
	}
	defer func() {
		err1 := rightNeighborsIter.Close()
		if err1 != nil {
			err = errors.Join(err, fmt.Errorf("close failed: %w", err1))
		}
	}()

	for right := range rightNeighborsIter.Seq() {
		if left >= right.V {
			continue
		}

		if _, ok := leftNeighbors.Get(right.V); ok {
			r++
		}
	}

	return r, nil
}

func (e *Executor) getVertexTriangleCount(tx TxnID, v *Vertex) (r uint64, err error) {
	r = 0

	var leftNeighborsIter NeighborIter

	leftNeighborsIter, err = e.se.Neighbors(tx, v.ID)
	if err != nil {
		return 0, fmt.Errorf("failed to get neighbors of vertex %v: %w", v.ID, err)
	}
	defer func() {
		err1 := leftNeighborsIter.Close()
		if err1 != nil {
			err = errors.Join(err, fmt.Errorf("close failed: %w", err1))
		}
	}()

	leftNeighbors := NewInMemoryAssociativeArray[VertexID, struct{}]()

	for l := range leftNeighborsIter.Seq() {
		err = leftNeighbors.Set(l.V, struct{}{})
		if err != nil {
			return 0, fmt.Errorf("failed to set value in left neighbors associative array: %w", err)
		}
	}

	leftNeighbors.Seq(func(id VertexID, s struct{}) bool {
		var add uint64

		add, err = e.countCommonNeighbors(tx, id, leftNeighbors)
		if err != nil {
			return false
		}

		if math.MaxUint64-r < add {
			err = errors.New("triangle count is bigger than uint64")

			return false
		}

		r += add

		return true
	})

	return r, nil
}

// GetAllTriangles is the fifth query from SOW.
// It returns all triangles in the graph (ignoring edge orientation).
func (e *Executor) GetAllTriangles() (r uint64, err error) {
	if e.se == nil {
		return 0, errors.New("storage engine is nil")
	}

	var tx TxnID

	tx, err = e.tm.Begin()
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			err1 := e.tm.RollbackTx(tx)

			if err1 != nil {
				err = errors.Join(err, fmt.Errorf("rollback failed: %w", err1))
			}
		}
	}()

	var verticesIter VerticesIter

	verticesIter, err = e.se.GetAllVertices(tx)
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
		var add uint64

		add, err = e.getVertexTriangleCount(tx, v)
		if err != nil {
			return 0, fmt.Errorf("failed to get triangle count of vertex %v: %w", v.ID, err)
		}

		if math.MaxUint64-r < add {
			return 0, fmt.Errorf("triangle count is bigger than uint64")
		}

		r += add
	}

	err = e.tm.CommitTx(tx)
	if err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return r / 3, nil
}
