package query

import (
	"errors"
	"fmt"

	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
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
	_ = e.locker.LockCatalog(txnID, txns.GranularLockExclusive)

	if err := logger.AppendBegin(); err != nil {
		return fmt.Errorf("failed to append begin: %w", err)
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
	return e.se.CreateVertexTable(txnID, tableName, schema, cToken, logger)
}

func (e *Executor) CreateEdgeType(
	txnID common.TxnID,
	tableName string,
	schema storage.Schema,
	srcVertexTableName string,
	dstVertexTableName string,
	logger common.ITxnLoggerWithContext,
) (err error) {
	_ = e.locker.LockCatalog(txnID, txns.GranularLockExclusive)

	if err := logger.AppendBegin(); err != nil {
		return fmt.Errorf("failed to append begin: %w", err)
	}

	defer func() {
		if err != nil {
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
