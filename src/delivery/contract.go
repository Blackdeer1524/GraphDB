package delivery

import (
	"github.com/Blackdeer1524/GraphDB/src/raft"
	"github.com/Blackdeer1524/GraphDB/src/storage"
)

type Node interface {
	InsertVertex(tableName string, props map[string]any) (storage.VertexSystemID, error)
	InsertVertices(tableName string, props []map[string]any) ([]storage.VertexSystemID, error)
	InsertEdge(edgeTableName string, dto raft.InsertEdgeDTO) (storage.EdgeSystemID, error)
	InsertEdges(edgeTableName string, dto []raft.InsertEdgeDTO) ([]storage.EdgeSystemID, error)
}
