package query

import (
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

func (e *Executor) CreateVertexType(
	txnID common.TxnID,
	tableName string,
	schema storage.Schema,
	logger common.ITxnLoggerWithContext,
) (err error) {
	cToken := txns.NewNilCatalogLockToken(txnID)
	return e.se.CreateVertexTable(txnID, tableName, schema, cToken, logger)
}

func (e *Executor) DropVertexTable(
	txnID common.TxnID,
	tableName string,
	logger common.ITxnLoggerWithContext,
) (err error) {
	cToken := txns.NewNilCatalogLockToken(txnID)
	return e.se.DropVertexTable(txnID, tableName, cToken, logger)
}

func (e *Executor) CreateEdgeType(
	txnID common.TxnID,
	tableName string,
	schema storage.Schema,
	srcVertexTableName string,
	dstVertexTableName string,
	logger common.ITxnLoggerWithContext,
) (err error) {
	cToken := txns.NewNilCatalogLockToken(txnID)
	srcTableMeta, err := e.se.GetVertexTableMeta(srcVertexTableName, cToken)
	if err != nil {
		return err
	}
	dstTableMeta, err := e.se.GetVertexTableMeta(dstVertexTableName, cToken)
	if err != nil {
		return err
	}

	err = e.se.CreateEdgeTable(
		txnID,
		tableName,
		schema,
		srcTableMeta.FileID,
		dstTableMeta.FileID,
		cToken,
		logger,
	)
	return err
}

func (e *Executor) DropEdgeTable(
	txnID common.TxnID,
	tableName string,
	logger common.ITxnLoggerWithContext,
) (err error) {
	cToken := txns.NewNilCatalogLockToken(txnID)
	return e.se.DropEdgeTable(txnID, tableName, cToken, logger)
}
