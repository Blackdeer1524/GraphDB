package query

import (
	"errors"
	"fmt"

	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

func (e *Executor) SelectVertex(
	tableName string,
	vertexID storage.VertexSystemID,
) (v storage.Vertex, err error) {
	txnID := e.newTxnID()
	logger := e.logger.WithContext(txnID)

	if err := logger.AppendBegin(); err != nil {
		return storage.Vertex{}, fmt.Errorf("failed to append begin: %w", err)
	}

	defer func() {
		if err != nil {
			assert.NoError(logger.AppendAbort())
			logger.Rollback()
			err = errors.Join(err, logger.AppendTxnEnd())
		} else {
			if err = logger.AppendCommit(); err != nil {
				err = fmt.Errorf("failed to append commit: %w", err)
			} else if err = logger.AppendTxnEnd(); err != nil {
				err = fmt.Errorf("failed to append txn end: %w", err)
			}
		}
	}()

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
	tableName string,
	data map[string]any,
) (vID storage.VertexSystemID, err error) {
	txnID := e.newTxnID()
	logger := e.logger.WithContext(txnID)

	if err := logger.AppendBegin(); err != nil {
		return storage.NilVertexID, fmt.Errorf("failed to append begin: %w", err)
	}

	defer func() {
		if err != nil {
			assert.NoError(logger.AppendAbort())
			logger.Rollback()
			err = errors.Join(err, logger.AppendTxnEnd())
			return
		}
		if err = logger.AppendCommit(); err != nil {
			err = fmt.Errorf("failed to append commit: %w", err)
		} else if err = logger.AppendTxnEnd(); err != nil {
			err = fmt.Errorf("failed to append txn end: %w", err)
		}
	}()

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
	edgeTableName string,
	srcVertexID storage.VertexSystemID,
	dstVertexID storage.VertexSystemID,
	data map[string]any,
) (edgeID storage.EdgeSystemID, err error) {
	txnID := e.newTxnID()
	logger := e.logger.WithContext(txnID)

	if err := logger.AppendBegin(); err != nil {
		return storage.NilEdgeID, fmt.Errorf("failed to append begin: %w", err)
	}

	defer func() {
		if err != nil {
			assert.NoError(logger.AppendAbort())
			logger.Rollback()
			err = errors.Join(err, logger.AppendTxnEnd())
			return
		}
		if err = logger.AppendCommit(); err != nil {
			err = fmt.Errorf("failed to append commit: %w", err)
		} else if err = logger.AppendTxnEnd(); err != nil {
			err = fmt.Errorf("failed to append txn end: %w", err)
		}
	}()

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
