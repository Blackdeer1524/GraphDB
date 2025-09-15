package raft

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
)

// InsertVertex inserts a single vertex into the graph
func (n *Node) InsertVertex(tableName string, record storage.VertexInfo) (storage.VertexSystemID, error) {
	txnID := common.TxnID(atomic.AddUint64(&n.ticker, 1))

	recordBytes, err := json.Marshal(record)
	if err != nil {
		return storage.NilVertexID, fmt.Errorf("failed to marshal vertex record: %w", err)
	}

	data := fmt.Sprintf("%s\n%d\n%s\n%s", InsertVertex.String(), txnID, tableName, recordBytes)
	future := n.raft.Apply([]byte(data), 5*time.Second)
	if err := future.Error(); err != nil {
		return storage.NilVertexID, fmt.Errorf("raft apply failed: %w", err)
	}

	resp := future.Response()
	if resp == nil {
		return storage.NilVertexID, fmt.Errorf("no response from raft apply")
	}

	vID, ok := resp.(storage.VertexSystemID)
	if !ok {
		return storage.NilVertexID, fmt.Errorf("unexpected response type: %T", resp)
	}

	return vID, nil
}

// InsertVertices inserts multiple vertices in bulk
func (n *Node) InsertVertices(tableName string, records []storage.VertexInfo) ([]storage.VertexSystemID, error) {
	txnID := common.TxnID(atomic.AddUint64(&n.ticker, 1))

	recordsBytes, err := json.Marshal(records)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal vertices records: %w", err)
	}

	data := fmt.Sprintf("%s\n%d\n%s\n%s", InsertVertices.String(), txnID, tableName, recordsBytes)
	future := n.raft.Apply([]byte(data), 5*time.Second)
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("raft apply failed: %w", err)
	}

	resp := future.Response()
	if resp == nil {
		return nil, fmt.Errorf("no response from raft apply")
	}

	vIDs, ok := resp.([]storage.VertexSystemID)
	if !ok {
		return nil, fmt.Errorf("unexpected response type: %T", resp)
	}

	return vIDs, nil
}

// InsertEdge inserts a single edge into the graph
func (n *Node) InsertEdge(edgeTableName string, record storage.EdgeInfo) (storage.EdgeSystemID, error) {
	txnID := common.TxnID(atomic.AddUint64(&n.ticker, 1))

	recordBytes, err := json.Marshal(record)
	if err != nil {
		return storage.NilEdgeID, fmt.Errorf("failed to marshal edge record: %w", err)
	}

	data := fmt.Sprintf("%s\n%d\n%s\n%s", InsertEdge.String(), txnID, edgeTableName, recordBytes)
	future := n.raft.Apply([]byte(data), 5*time.Second)
	if err := future.Error(); err != nil {
		return storage.NilEdgeID, fmt.Errorf("raft apply failed: %w", err)
	}

	resp := future.Response()
	if resp == nil {
		return storage.NilEdgeID, fmt.Errorf("no response from raft apply")
	}

	edgeID, ok := resp.(storage.EdgeSystemID)
	if !ok {
		return storage.NilEdgeID, fmt.Errorf("unexpected response type: %T", resp)
	}

	return edgeID, nil
}

// InsertEdges inserts multiple edges into the graph
func (n *Node) InsertEdges(edgeTableName string, records []storage.EdgeInfo) ([]storage.EdgeSystemID, error) {
	txnID := common.TxnID(atomic.AddUint64(&n.ticker, 1))

	recordsBytes, err := json.Marshal(records)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal edges records: %w", err)
	}

	data := fmt.Sprintf("%s\n%d\n%s\n%s", InsertEdges.String(), txnID, edgeTableName, recordsBytes)
	future := n.raft.Apply([]byte(data), 5*time.Second)
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("raft apply failed: %w", err)
	}

	resp := future.Response()
	if resp == nil {
		return nil, fmt.Errorf("no response from raft apply")
	}

	edgeIDs, ok := resp.([]storage.EdgeSystemID)
	if !ok {
		return nil, fmt.Errorf("unexpected response type: %T", resp)
	}

	return edgeIDs, nil
}
