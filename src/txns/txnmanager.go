package txns

import (
	"errors"
	"sync/atomic"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
)

var ErrRollback = errors.New("Rollback")

type TxnManager struct {
	txnIDCounter atomic.Uint64
	locker       *HierarchyLocker
	logger       common.ITxnLogger
}

func NewTxnManager(logger common.ITxnLogger) *TxnManager {
	return &TxnManager{
		txnIDCounter: atomic.Uint64{},
		locker:       NewHierarchyLocker(),
		logger:       logger,
	}
}

func (m *TxnManager) WithContext(txnID common.TxnID) *txnManagerWithContext {
	return &txnManagerWithContext{
		txnID:  txnID,
		locker: m.locker,
		logger: m.logger.WithContext(txnID),
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

type txnManagerWithContext struct {
	txnID  common.TxnID
	locker *HierarchyLocker
	logger common.ITxnLoggerWithContext

	rollbackFunc func(logger common.ITxnLoggerWithContext) error

	catalogLockToken *catalogLockToken
	fileLockToken    map[common.FileID]*fileLockToken
	pageLockToken    map[common.PageID]*pageLockToken

	failed bool
}

func (m *txnManagerWithContext) Locker() *HierarchyLocker {
	return m.locker
}

func (m *txnManagerWithContext) TxnID() common.TxnID {
	return m.txnID
}

func (m *txnManagerWithContext) Logger() common.ITxnLoggerWithContext {
	return m.logger
}

func (m *txnManagerWithContext) WithRollback(
	f func(logger common.ITxnLoggerWithContext) error,
) {
	m.rollbackFunc = f
}

func (m *txnManagerWithContext) Begin(task func(*txnManagerWithContext) error) error {
	if err := m.logger.AppendBegin(); err != nil {
		return err
	}

	if err := task(m); err != nil {
		rollbackErr := m.rollbackFunc(m.logger)
		if errors.Is(err, ErrRollback) {
			return rollbackErr
		}
		err = errors.Join(err, rollbackErr)
		return err
	}
	if err := m.logger.AppendCommit(); err != nil {
		return err
	}
	if err := m.logger.AppendTxnEnd(); err != nil {
		return err
	}
	return nil
}
