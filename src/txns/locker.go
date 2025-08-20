package txns

import (
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
)

type Locker struct {
	catalogLockManager *lockManager[GranularLockMode, struct{}]
	fileLockManager    *lockManager[GranularLockMode, common.FileID] // for indexes and tables
	pageLockManager    *lockManager[PageLockMode, common.PageIdentity]
}

func NewLocker() *Locker {
	return &Locker{
		catalogLockManager: NewManager[GranularLockMode, struct{}](),
		fileLockManager:    NewManager[GranularLockMode, common.FileID](),
		pageLockManager:    NewManager[PageLockMode, common.PageIdentity](),
	}
}

type catalogLockToken struct {
	txnID common.TxnID
}

func newCatalogLockToken(txnID common.TxnID) *catalogLockToken {
	return &catalogLockToken{
		txnID: txnID,
	}
}

type fileLockToken struct {
	txnID  common.TxnID
	fileID common.FileID
}

func newFileLockToken(txnID common.TxnID, fileID common.FileID) *fileLockToken {
	return &fileLockToken{
		fileID: fileID,
		txnID:  txnID,
	}
}

type pageLockToken struct {
	txnID  common.TxnID
	pageID common.PageIdentity
}

func newPageLockToken(
	txnID common.TxnID,
	pageID common.PageIdentity,
) *pageLockToken {
	return &pageLockToken{
		pageID: pageID,
		txnID:  txnID,
	}
}

func (l *Locker) LockCatalog(
	txnID common.TxnID,
	lockMode GranularLockMode,
) *catalogLockToken {
	r := TxnLockRequest[GranularLockMode, struct{}]{
		txnID:    txnID,
		objectId: struct{}{},
		lockMode: lockMode,
	}

	n := l.catalogLockManager.Lock(r)
	if n == nil {
		return nil
	}
	<-n

	return newCatalogLockToken(r.txnID)
}

func (l *Locker) LockFile(
	t *catalogLockToken,
	fileID common.FileID,
	lockMode GranularLockMode,
) *fileLockToken {
	n := l.fileLockManager.Lock(TxnLockRequest[GranularLockMode, common.FileID]{
		txnID:    t.txnID,
		objectId: fileID,
		lockMode: lockMode,
	})
	if n == nil {
		return nil
	}
	<-n
	return newFileLockToken(t.txnID, fileID)
}

func (l *Locker) LockPage(
	t *fileLockToken,
	pageID common.PageID,
	lockMode PageLockMode,
) *pageLockToken {
	pageIdent := common.PageIdentity{
		FileID: t.fileID,
		PageID: pageID,
	}

	lockRequest := TxnLockRequest[PageLockMode, common.PageIdentity]{
		txnID:    t.txnID,
		objectId: pageIdent,
		lockMode: lockMode,
	}

	n := l.pageLockManager.Lock(lockRequest)
	if n == nil {
		return nil
	}
	<-n

	return newPageLockToken(t.txnID, pageIdent)
}

func (l *Locker) Unlock(t *catalogLockToken) {
	l.catalogLockManager.UnlockAll(t.txnID)
	l.fileLockManager.UnlockAll(t.txnID)
	l.pageLockManager.UnlockAll(t.txnID)
}

func (l *Locker) UpgradeCatalogLock(
	t *catalogLockToken,
	lockMode GranularLockMode,
) bool {
	n := l.catalogLockManager.Upgrade(
		TxnLockRequest[GranularLockMode, struct{}]{
			txnID:    t.txnID,
			objectId: struct{}{},
			lockMode: lockMode,
		},
	)

	if n == nil {
		return false
	}
	<-n
	return true
}

func (l *Locker) UpgradeFileLock(
	t *fileLockToken,
	lockMode GranularLockMode,
) bool {
	n := l.fileLockManager.Upgrade(
		TxnLockRequest[GranularLockMode, common.FileID]{
			txnID:    t.txnID,
			objectId: t.fileID,
			lockMode: lockMode,
		},
	)
	if n == nil {
		return false
	}
	<-n
	return true
}

func (l *Locker) UpgradePageLock(
	t *pageLockToken,
	lockMode PageLockMode,
) bool {
	lockRequest := TxnLockRequest[PageLockMode, common.PageIdentity]{
		txnID:    t.txnID,
		objectId: t.pageID,
		lockMode: lockMode,
	}

	n := l.pageLockManager.Upgrade(lockRequest)
	if n == nil {
		return false
	}
	<-n
	return true
}

func (l *Locker) GetActiveTransactions() []common.TxnID {
	catalogLockingTxns := l.catalogLockManager.GetActiveTransactions()
	fileLockingTxns := l.fileLockManager.GetActiveTransactions()
	pageLockingTxns := l.pageLockManager.GetActiveTransactions()

	merge := utils.MergeMaps(
		catalogLockingTxns,
		fileLockingTxns,
		pageLockingTxns,
	)
	res := make([]common.TxnID, 0, len(merge))
	for k := range merge {
		res = append(res, k)
	}

	return res
}
