package raft

import (
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
)

type Inserter interface {
	InsertVertex(
		txnID common.TxnID,
		tableName string,
		record storage.VertexInfo,
		logger common.ITxnLoggerWithContext,
	) error
	InsertVertices(
		txnID common.TxnID,
		tableName string,
		records []storage.VertexInfo,
		logger common.ITxnLoggerWithContext,
	) error
	InsertEdge(
		txnID common.TxnID,
		edgeTableName string,
		record storage.EdgeInfo,
		logger common.ITxnLoggerWithContext,
	) error
	InsertEdges(
		txnID common.TxnID,
		edgeTableName string,
		data []storage.EdgeInfo,
		logger common.ITxnLoggerWithContext,
	) error
}
