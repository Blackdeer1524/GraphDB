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

	vertFileToken := txns.NewNilFileLockToken(cToken, vertexTableMeta.FileID)
	vertSystems, data, err := e.se.SelectVertex(
		txnID,
		vertexID,
		vertFileToken,
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
	record storage.VertexInfo,
	logger common.ITxnLoggerWithContext,
) error {
	records := []storage.VertexInfo{record}
	err := e.InsertVertices(txnID, tableName, records, logger)
	if err != nil {
		return fmt.Errorf("failed to insert vertex: %w", err)
	}
	return nil
}

func (e *Executor) InsertVertices(
	txnID common.TxnID,
	tableName string,
	records []storage.VertexInfo,
	logger common.ITxnLoggerWithContext,
) error {
	cToken := txns.NewNilCatalogLockToken(txnID)
	tableMeta, err := e.se.GetVertexTableMeta(tableName, cToken)
	if err != nil {
		return fmt.Errorf("failed to get vertex table meta: %w", err)
	}

	tableIndex, err := e.se.GetVertexTableSystemIndex(txnID, tableMeta.FileID, cToken, logger)
	if err != nil {
		return fmt.Errorf("failed to get vertex table internal index: %w", err)
	}
	defer tableIndex.Close()

	fileToken := txns.NewNilFileLockToken(cToken, tableMeta.FileID)

	for _, record := range records {
		err := e.se.InsertVertex(
			txnID,
			record.SystemID,
			record.Data,
			tableMeta.Schema,
			fileToken,
			tableIndex,
			logger,
		)
		if err != nil {
			return fmt.Errorf("failed to insert vertex: %w", err)
		}
	}
	return nil
}

func (e *Executor) DeleteVertex() error {
	return nil
}

func (e *Executor) UpdateVertex(
	txnID common.TxnID,
	tableName string,
	vertexID storage.VertexSystemID,
	data map[string]any,
	logger common.ITxnLoggerWithContext,
) error {
	cToken := txns.NewNilCatalogLockToken(txnID)
	tableMeta, err := e.se.GetVertexTableMeta(tableName, cToken)
	if err != nil {
		return fmt.Errorf("failed to get vertex table meta: %w", err)
	}

	tableIndex, err := e.se.GetVertexTableSystemIndex(txnID, tableMeta.FileID, cToken, logger)
	if err != nil {
		return fmt.Errorf("failed to get vertex table internal index: %w", err)
	}
	defer tableIndex.Close()

	fileToken := txns.NewNilFileLockToken(cToken, tableMeta.FileID)
	err = e.se.UpdateVertex(
		txnID,
		vertexID,
		data,
		tableMeta.Schema,
		fileToken,
		tableIndex,
		logger,
	)
	if err != nil {
		return fmt.Errorf("failed to update vertex: %w", err)
	}

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

func (e *Executor) InsertEdge(
	txnID common.TxnID,
	edgeTableName string,
	record storage.EdgeInfo,
	logger common.ITxnLoggerWithContext,
) error {
	edges := []storage.EdgeInfo{record}
	err := e.InsertEdges(txnID, edgeTableName, edges, logger)
	if err != nil {
		return fmt.Errorf("failed to insert edge: %w", err)
	}
	return nil
}

func (e *Executor) InsertEdges(
	txnID common.TxnID,
	edgeTableName string,
	data []storage.EdgeInfo,
	logger common.ITxnLoggerWithContext,
) error {
	cToken := txns.NewNilCatalogLockToken(txnID)
	edgeTableMeta, err := e.se.GetEdgeTableMeta(edgeTableName, cToken)
	if err != nil {
		return fmt.Errorf("failed to get vertex table meta: %w", err)
	}

	edgeSystemIndex, err := e.se.GetEdgeTableSystemIndex(
		txnID,
		edgeTableMeta.FileID,
		cToken,
		logger,
	)
	if err != nil {
		return fmt.Errorf("failed to get vertex table internal index: %w", err)
	}
	defer edgeSystemIndex.Close()

	dirTableMeta, err := e.se.GetDirTableMeta(cToken, edgeTableMeta.SrcVertexFileID)
	if err != nil {
		return fmt.Errorf("failed to get dir table meta: %w", err)
	}

	srcVertDirSystemIndex, err := e.se.GetDirTableSystemIndex(
		txnID,
		dirTableMeta.FileID,
		cToken,
		logger,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to get src vertex table internal index: %w",
			err,
		)
	}
	defer srcVertDirSystemIndex.Close()

	srcVertMeta, err := e.se.GetVertexTableMetaByFileID(edgeTableMeta.SrcVertexFileID, cToken)
	if err != nil {
		return fmt.Errorf("failed to get src vertex table meta: %w", err)
	}

	srcVertSystemIndex, err := e.se.GetVertexTableSystemIndex(
		txnID,
		edgeTableMeta.SrcVertexFileID,
		cToken,
		logger,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to get src vertex table internal index: %w",
			err,
		)
	}
	defer srcVertSystemIndex.Close()

	srcVertToken := txns.NewNilFileLockToken(cToken, edgeTableMeta.SrcVertexFileID)
	srcVertDirToken := txns.NewNilFileLockToken(cToken, dirTableMeta.FileID)
	edgeTableToken := txns.NewNilFileLockToken(cToken, edgeTableMeta.FileID)

	for _, edgeRecord := range data {
		err := e.se.InsertEdge(
			txnID,
			edgeRecord.SystemID,
			edgeRecord.SrcVertexID,
			edgeRecord.DstVertexID,
			edgeRecord.Data,
			edgeTableMeta.Schema,
			srcVertMeta.Schema,
			srcVertToken,
			srcVertSystemIndex,
			srcVertDirToken,
			srcVertDirSystemIndex,
			edgeTableToken,
			edgeSystemIndex,
			logger,
		)
		if err != nil {
			return fmt.Errorf("failed to insert edge: %w", err)
		}
	}
	return nil
}

func (e *Executor) DeleteEdge() error {
	return nil
}

func (e *Executor) UpdateEdge() error {
	return nil
}
