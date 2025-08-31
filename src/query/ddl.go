package query

import (
	"errors"
	"fmt"

	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

func (e *Executor) CreateVertexType(name string, schema storage.Schema) (err error) {
	txnID := e.newTxnID()

	_ = e.locker.LockCatalog(txnID, txns.GranularLockExclusive)
	defer e.locker.Unlock(txnID)

	logger := e.logger.WithContext(txnID)
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

	if err = e.se.CreateVertexTable(txnID, name, schema, logger); err != nil {
		return err
	}
	if err = e.se.CreateDirectoryTable(txnID, name, logger); err != nil {
		return err
	}

	return nil
}

func (e *Executor) CreateEdgeType(name string, schema storage.Schema) (err error) {
	txnID := e.newTxnID()

	_ = e.locker.LockCatalog(txnID, txns.GranularLockExclusive)
	defer e.locker.Unlock(txnID)

	logger := e.logger.WithContext(txnID)
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

	if err = e.se.CreateEdgeTable(txnID, name, schema, logger); err != nil {
		return err
	}

	return nil
}
