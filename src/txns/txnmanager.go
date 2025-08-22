package txns

import (
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
)

type TxnManager struct {
	locker       *HierarchyLocker
	logger       common.ITxnLoggerWithContext
	rollbackFunc func(logger common.ITxnLoggerWithContext) error

	catalogLockToken *catalogLockToken
	fileLockToken    map[common.FileID]*fileLockToken
	pageLockToken    map[common.PageID]*pageLockToken

	failed bool
}

func NewTxnManager(logger common.ITxnLoggerWithContext) *TxnManager {
	return &TxnManager{
		locker: NewHierarchyLocker(),
		logger: logger,
		rollbackFunc: func(logger common.ITxnLoggerWithContext) error {
			err := logger.AppendAbort()
			if err != nil {
				return err
			}
			logger.Rollback()
			return nil
		},
		catalogLockToken: nil,
		fileLockToken:    make(map[common.FileID]*fileLockToken),
		pageLockToken:    make(map[common.PageID]*pageLockToken),
		failed:           false,
	}
}

func (m *TxnManager) WithRollback(
	f func(logger common.ITxnLoggerWithContext) error,
) {
	m.rollbackFunc = f
}

// func (m *TxnManager) Begin() error {
// 	return m.logger.AppendBegin()
// }
//
// func (m *TxnManager) Rollback() error {
// 	return m.rollbackFunc(m.logger)
// }
//
// func (m *TxnManager) LockCatalog(lockMode GranularLockMode) bool {
// 	assert.Assert(!m.failed, "txn already failed")
//
// 	if m.catalogLockToken != nil {
// 		return true
// 	}
//
// 	m.catalogLockToken = m.locker.LockCatalog(m.logger.GetTxnID(), lockMode)
// 	if m.catalogLockToken == nil {
// 		m.failed = true
// 		return false
// 	}
// 	return true
// }
//
// func (m *TxnManager) LockFile(
// 	fileID common.FileID,
// 	lockMode GranularLockMode,
// ) bool {
// 	assert.Assert(!m.failed, "txn already failed")
//
// 	switch lockMode {
// 	case GRANULAR_LOCK_SHARED_INTENTION_EXCLUSIVE:
// 		fallthrough
// 	case GRANULAR_LOCK_INTENTION_EXCLUSIVE:
// 		fallthrough
// 	case GRANULAR_LOCK_INTENTION_SHARED:
// 		if !m.LockCatalog(lockMode) {
// 			return false
// 		}
// 	case GRANULAR_LOCK_SHARED:
// 		if !m.LockCatalog(GRANULAR_LOCK_INTENTION_SHARED) {
// 			return false
// 		}
// 		fallthrough
// 	case GRANULAR_LOCK_EXCLUSIVE:
// 		if !m.LockCatalog(GRANULAR_LOCK_INTENTION_EXCLUSIVE) {
// 			return false
// 		}
// 		fallthrough
// 	default:
// 		assert.Assert(false, "invalid lock mode %s", lockMode)
// 		panic("unreachable")
// 	}
//
// 	assert.Assert(m.catalogLockToken != nil, "catalog lock token is nil")
//
// 	fileLockToken := m.locker.LockFile(m.catalogLockToken, fileID, lockMode)
// 	if fileLockToken == nil {
// 		m.failed = true
// 		return false
// 	}
// 	return true
// }
//
// func (m *TxnManager) LockPage(
// 	pageID common.PageID,
// 	lockMode PageLockMode,
// ) bool {
// 	assert.Assert(!m.failed, "txn already failed")
//
//
// }
//
// func (m *TxnManager) LockRecord(
// 	recordID common.RecordID,
// 	lockMode PageLockMode,
// ) bool {
// 	switch lockMode {
// 	case PAGE_LOCK_SHARED:
// 		if m.catalogLockToken == nil {
//
// 		}
// 		return m.LockPage(recordID, lockMode)
// 	case PAGE_LOCK_EXCLUSIVE:
// 		return m.LockPage(recordID, lockMode)
// 	default:
// 		assert.Assert(false, "invalid lock mode %s", lockMode)
// 		panic("unreachable")
// 	}
// }
//
// func (m *TxnManager) Begin(task func(common.TxnID) error) error {
// 	err := m.logger.AppendBegin()
// 	if err != nil {
// 		return err
// 	}
// 	return task(1)
// }
//
// func (m *TxnManager) Insert()
//
//
// func (m *TxnManager	) Rollback() error {
// 	return m.rollbackFunc(m.logger)
// }
//
// func (m *TxnManager) Commit() error {
