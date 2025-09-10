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
	data map[string]any,
	logger common.ITxnLoggerWithContext,
) (vID storage.VertexSystemID, err error) {
	cToken := txns.NewNilCatalogLockToken(txnID)
	tableMeta, err := e.se.GetVertexTableMeta(tableName, cToken)
	if err != nil {
		return storage.NilVertexID, fmt.Errorf("failed to get vertex table meta: %w", err)
	}

	tableIndex, err := e.se.GetVertexTableSystemIndex(txnID, tableMeta.FileID, cToken, logger)
	if err != nil {
		return storage.NilVertexID, fmt.Errorf("failed to get vertex table internal index: %w", err)
	}

	fileToken := txns.NewNilFileLockToken(cToken, tableMeta.FileID)
	vertID, err := e.se.InsertVertex(txnID, data, tableMeta.Schema, fileToken, tableIndex, logger)
	if err != nil {
		return storage.NilVertexID, fmt.Errorf("failed to insert vertex: %w", err)
	}

	return vertID, nil
}

func (e *Executor) DeleteVertex() error {
	return nil
}

func (e *Executor) UpdateVertex() error {
	return nil
}

func (e *Executor) SelectEdge() error {
	return nil
}

func (e *Executor) InsertEdge(
	txnID common.TxnID,
	edgeTableName string,
	srcVertexID storage.VertexSystemID,
	dstVertexID storage.VertexSystemID,
	data map[string]any,
	logger common.ITxnLoggerWithContext,
) (edgeID storage.EdgeSystemID, err error) {
	cToken := txns.NewNilCatalogLockToken(txnID)
	edgeTableMeta, err := e.se.GetEdgeTableMeta(edgeTableName, cToken)
	if err != nil {
		return storage.NilEdgeID, fmt.Errorf("failed to get vertex table meta: %w", err)
	}

	edgeTableIndex, err := e.se.GetEdgeTableSystemIndex(
		txnID,
		edgeTableMeta.FileID,
		cToken,
		logger,
	)
	if err != nil {
		return storage.NilEdgeID, fmt.Errorf("failed to get vertex table internal index: %w", err)
	}

	dirTableMeta, err := e.se.GetDirTableMeta(cToken, edgeTableMeta.SrcVertexFileID)
	if err != nil {
		return storage.NilEdgeID, fmt.Errorf("failed to get dir table meta: %w", err)
	}

	srcVertDirTableIndex, err := e.se.GetVertexTableSystemIndex(
		txnID,
		dirTableMeta.FileID,
		cToken,
		logger,
	)
	if err != nil {
		return storage.NilEdgeID, fmt.Errorf(
			"failed to get src vertex table internal index: %w",
			err,
		)
	}

	srcVertTableIndex, err := e.se.GetVertexTableSystemIndex(
		txnID,
		edgeTableMeta.SrcVertexFileID,
		cToken,
		logger,
	)
	if err != nil {
		return storage.NilEdgeID, fmt.Errorf(
			"failed to get src vertex table internal index: %w",
			err,
		)
	}

	srcVertToken := txns.NewNilFileLockToken(cToken, edgeTableMeta.SrcVertexFileID)
	srcVertDirToken := txns.NewNilFileLockToken(cToken, dirTableMeta.FileID)
	edgeTableToken := txns.NewNilFileLockToken(cToken, edgeTableMeta.FileID)
	vertID, err := e.se.InsertEdge(
		txnID,
		srcVertexID,
		dstVertexID,
		data,
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
		return storage.NilEdgeID, fmt.Errorf("failed to insert vertex: %w", err)
	}
	return vertID, nil
}

func (e *Executor) DeleteEdge() error {
	return nil
}

func (e *Executor) UpdateEdge() error {
	return nil
}
