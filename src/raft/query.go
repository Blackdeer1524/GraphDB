package raft

import "errors"

type queryAction string

const (
	InsertVertex   queryAction = "insert_vertex"
	InsertVertices queryAction = "insert_vertices"
	InsertEdge     queryAction = "insert_edge"
	InsertEdges    queryAction = "insert_edges"
)

func (a queryAction) String() string {
	return string(a)
}

func queryActionFromString(v string) (queryAction, error) {
	switch v {
	case InsertVertex.String():
		return InsertVertex, nil
	case InsertVertices.String():
		return InsertVertices, nil
	case InsertEdge.String():
		return InsertEdge, nil
	case InsertEdges.String():
		return InsertEdges, nil
	default:
		return "", errors.New("unknown action")
	}
}
