package query

import (
	"fmt"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

func (e *Executor) SelectVertex(
	txnID common.TxnID,
	tableName string,
	vertexID storage.VertexSystemID,
	logger common.ITxnLoggerWithContext,
) (v storage.Vertex, err error) {
	cToken := txns.NewNilCatalogLockToken(txnID)
	vertexTableMeta, err := e.se.GetVertexTableMeta(tableName, cToken)
	if err != nil {
		return storage.Vertex{}, fmt.Errorf("failed to get vertex table meta: %w", err)
	}

	vertexIndex, err := e.se.GetVertexTableSystemIndex(
		txnID,
		vertexTableMeta.FileID,
		cToken,
		logger,
	)
	if err != nil {
		return storage.Vertex{}, fmt.Errorf("failed to get vertex table internal index: %w", err)
	}
	defer vertexIndex.Close()

	vertSystems, data, err := e.se.SelectVertex(
		txnID,
		vertexID,
		vertexIndex,
		vertexTableMeta.Schema,
	)
	if err != nil {
		return storage.Vertex{}, fmt.Errorf("failed to select vertex: %w", err)
	}

	vert := storage.Vertex{
		VertexSystemFields: vertSystems,
		Data:               data,
	}
	return vert, nil
}

func (e *Executor) InsertVertex(
	txnID common.TxnID,
	tableName string,
	record map[string]any,
	logger common.ITxnLoggerWithContext,
) (vID storage.VertexSystemID, err error) {
	records := []map[string]any{record}
	vIDs, err := e.InsertVertices(txnID, tableName, records, logger)
	if err != nil {
		return storage.NilVertexID, fmt.Errorf("failed to insert vertex: %w", err)
	}
	return vIDs[0], nil
}

func (e *Executor) InsertVertices(
	txnID common.TxnID,
	tableName string,
	records []map[string]any,
	logger common.ITxnLoggerWithContext,
) (vIDs []storage.VertexSystemID, err error) {
	cToken := txns.NewNilCatalogLockToken(txnID)
	tableMeta, err := e.se.GetVertexTableMeta(tableName, cToken)
	if err != nil {
		return nil, fmt.Errorf("failed to get vertex table meta: %w", err)
	}

	tableIndex, err := e.se.GetVertexTableSystemIndex(txnID, tableMeta.FileID, cToken, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to get vertex table internal index: %w", err)
	}
	defer tableIndex.Close()

	fileToken := txns.NewNilFileLockToken(cToken, tableMeta.FileID)

	vIDs = make([]storage.VertexSystemID, len(records))
	for i, record := range records {
		vertID, err := e.se.InsertVertex(
			txnID,
			record,
			tableMeta.Schema,
			fileToken,
			tableIndex,
			logger,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to insert vertex: %w", err)
		}
		vIDs[i] = vertID
	}
	return vIDs, nil
}

func (e *Executor) DeleteVertex() error {
	return nil
}

func (e *Executor) UpdateVertex() error {
	return nil
}

func (e *Executor) SelectEdge(
	txnID common.TxnID,
	tableName string,
	edgeID storage.EdgeSystemID,
	logger common.ITxnLoggerWithContext,
) (edge storage.Edge, err error) {
	cToken := txns.NewNilCatalogLockToken(txnID)
	edgeTableMeta, err := e.se.GetEdgeTableMeta(tableName, cToken)
	if err != nil {
		return storage.Edge{}, fmt.Errorf("failed to get edge table meta: %w", err)
	}

	edgeIndex, err := e.se.GetEdgeTableSystemIndex(
		txnID,
		edgeTableMeta.FileID,
		cToken,
		logger,
	)
	if err != nil {
		return storage.Edge{}, fmt.Errorf("failed to get edge table internal index: %w", err)
	}
	defer edgeIndex.Close()

	edgeFileToken := txns.NewNilFileLockToken(cToken, edgeTableMeta.FileID)
	edgeSystems, data, err := e.se.SelectEdge(
		txnID,
		edgeID,
		edgeFileToken,
		edgeIndex,
		edgeTableMeta.Schema,
	)
	if err != nil {
		return storage.Edge{}, fmt.Errorf("failed to select edge: %w", err)
	}

	edge = storage.Edge{
		EdgeSystemFields: edgeSystems,
		Data:             data,
	}
	return edge, nil
}

type EdgeInfo struct {
	SrcVertexID storage.VertexSystemID
	DstVertexID storage.VertexSystemID
	Data        map[string]any
}

func (e *Executor) InsertEdge(
	txnID common.TxnID,
	edgeTableName string,
	record EdgeInfo,
	logger common.ITxnLoggerWithContext,
) (edgeID storage.EdgeSystemID, err error) {
	edges := []EdgeInfo{record}
	edgeIDs, err := e.InsertEdges(txnID, edgeTableName, edges, logger)
	if err != nil {
		return storage.NilEdgeID, fmt.Errorf("failed to insert edge: %w", err)
	}
	return edgeIDs[0], nil
}

func (e *Executor) InsertEdges(
	txnID common.TxnID,
	edgeTableName string,
	data []EdgeInfo,
	logger common.ITxnLoggerWithContext,
) (edgeIDs []storage.EdgeSystemID, err error) {
	cToken := txns.NewNilCatalogLockToken(txnID)
	edgeTableMeta, err := e.se.GetEdgeTableMeta(edgeTableName, cToken)
	if err != nil {
		return nil, fmt.Errorf("failed to get vertex table meta: %w", err)
	}

	edgeTableIndex, err := e.se.GetEdgeTableSystemIndex(
		txnID,
		edgeTableMeta.FileID,
		cToken,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get vertex table internal index: %w", err)
	}
	defer edgeTableIndex.Close()

	dirTableMeta, err := e.se.GetDirTableMeta(cToken, edgeTableMeta.SrcVertexFileID)
	if err != nil {
		return nil, fmt.Errorf("failed to get dir table meta: %w", err)
	}

	srcVertDirTableIndex, err := e.se.GetDirTableSystemIndex(
		txnID,
		dirTableMeta.FileID,
		cToken,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to get src vertex table internal index: %w",
			err,
		)
	}
	defer srcVertDirTableIndex.Close()

	srcVertTableIndex, err := e.se.GetVertexTableSystemIndex(
		txnID,
		edgeTableMeta.SrcVertexFileID,
		cToken,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to get src vertex table internal index: %w",
			err,
		)
	}
	defer srcVertTableIndex.Close()

	srcVertToken := txns.NewNilFileLockToken(cToken, edgeTableMeta.SrcVertexFileID)
	srcVertDirToken := txns.NewNilFileLockToken(cToken, dirTableMeta.FileID)
	edgeTableToken := txns.NewNilFileLockToken(cToken, edgeTableMeta.FileID)

	edgeIDs = make([]storage.EdgeSystemID, len(data))
	for i, record := range data {
		edgeID, err := e.se.InsertEdge(
			txnID,
			record.SrcVertexID,
			record.DstVertexID,
			record.Data,
			edgeTableMeta.Schema,
			srcVertToken,
			srcVertTableIndex,
			srcVertDirToken,
			srcVertDirTableIndex,
			edgeTableToken,
			edgeTableIndex,
			logger,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to insert edge: %w", err)
		}
		edgeIDs[i] = edgeID
	}
	return edgeIDs, nil
}

func (e *Executor) DeleteEdge() error {
	return nil
}

func (e *Executor) UpdateEdge() error {
	return nil
}
