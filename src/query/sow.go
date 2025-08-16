// Package query provides implementations of the queries defined
// in the statement of work (SOW). Each function corresponds to
// one of the specified queries on the graph.
//
// Link for pseudocode: https://github.com/Blackdeer1524/GraphDB/blob/main/docs/queries/queries.py
package query

// GetVertexesOnDepth is the first query from SOW.
// It returns all vertexes on a given depth.
// We will use BFS on graph because DFS cannot calculate
// right depth on graphs (except trees).
func (e *Executor) GetVertexesOnDepth() error {
	return nil
}

// GetAllVertexesWithFieldValue is the second query from SOW.
// It returns all vertexes with a given field value.
func (e *Executor) GetAllVertexesWithFieldValue() error {
	return nil
}

// GetAllVertexesWithFieldValue2 is the third query from SOW (extended second query).
// It returns all vertexes with a given field value and uses filter on edges (degree of vertex
// with condition on edge).
func (e *Executor) GetAllVertexesWithFieldValue2() error {
	return nil
}

// SumNeighborAttributes is the forth query from SOW.
// It computes, for each vertex, the sum of a given attribute
// over its neighboring vertices, subject to a constraint on the edge or attribute
// value (e.g., only include neighbors whose attribute exceeds a given threshold).
func (e *Executor) SumNeighborAttributes() error {
	return nil
}

// GetAllTriangles is the fifth query from SOW.
// It returns all triangles in the graph (ignoring edge orientation).
func (e *Executor) GetAllTriangles() error {
	return nil
}
