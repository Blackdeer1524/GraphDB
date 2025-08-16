package txns

import (
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/optional"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
)

type Locker struct {
	catalogLockManager *Manager[GranularLockMode, struct{}]
	fileLockManager    *Manager[GranularLockMode, common.FileID] // for indexes and tables
	pageLockManager    *Manager[PageLockMode, common.PageIdentity]
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
) optional.Optional[utils.Pair[<-chan struct{}, *catalogLockToken]] {
	r := TxnLockRequest[GranularLockMode, struct{}]{
		txnID:    txnID,
		objectId: struct{}{},
		lockMode: lockMode,
	}

	n := l.catalogLockManager.Lock(r)
	if n == nil {
		return optional.None[utils.Pair[<-chan struct{}, *catalogLockToken]]()
	}

	return optional.Some(
		utils.Pair[<-chan struct{}, *catalogLockToken]{
			First:  n,
			Second: newCatalogLockToken(r.txnID),
		},
	)
}

func (l *Locker) LockFile(
	t *catalogLockToken,
	fileID common.FileID,
	lockMode GranularLockMode,
) optional.Optional[utils.Pair[<-chan struct{}, *fileLockToken]] {
	n := l.fileLockManager.Lock(TxnLockRequest[GranularLockMode, common.FileID]{
		txnID:    t.txnID,
		objectId: fileID,
		lockMode: lockMode,
	})
	if n == nil {
		return optional.None[utils.Pair[<-chan struct{}, *fileLockToken]]()
	}

	tt := newFileLockToken(t.txnID, fileID)

	return optional.Some(
		utils.Pair[<-chan struct{}, *fileLockToken]{
			First:  n,
			Second: tt,
		},
	)
}

func (l *Locker) LockPage(
	t *fileLockToken,
	pageID common.PageID,
	lockMode PageLockMode,
) optional.Optional[utils.Pair[<-chan struct{}, *pageLockToken]] {
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
		return optional.None[utils.Pair[<-chan struct{}, *pageLockToken]]()
	}

	return optional.Some(
		utils.Pair[<-chan struct{}, *pageLockToken]{
			First:  n,
			Second: newPageLockToken(t.txnID, pageIdent),
		},
	)
}

func (l *Locker) Unlock(t *catalogLockToken) {
	l.catalogLockManager.UnlockAll(t.txnID)
	l.fileLockManager.UnlockAll(t.txnID)
	l.pageLockManager.UnlockAll(t.txnID)
}

func (l *Locker) UpgradeCatalogLock(
	t *catalogLockToken,
	lockMode GranularLockMode,
) optional.Optional[<-chan struct{}] {
	n := l.catalogLockManager.Upgrade(
		TxnLockRequest[GranularLockMode, struct{}]{
			txnID:    t.txnID,
			objectId: struct{}{},
			lockMode: lockMode,
		},
	)

	if n == nil {
		return optional.None[<-chan struct{}]()
	}
	return optional.Some(n)
}

func (l *Locker) UpgradeFileLock(
	t *fileLockToken,
	lockMode GranularLockMode,
) optional.Optional[<-chan struct{}] {
	n := l.fileLockManager.Upgrade(
		TxnLockRequest[GranularLockMode, common.FileID]{
			txnID:    t.txnID,
			objectId: t.fileID,
			lockMode: lockMode,
		},
	)
	if n == nil {
		return optional.None[<-chan struct{}]()
	}
	return optional.Some(n)
}

func (l *Locker) UpgradePageLock(
	t *pageLockToken,
	lockMode PageLockMode,
) optional.Optional[<-chan struct{}] {
	lockRequest := TxnLockRequest[PageLockMode, common.PageIdentity]{
		txnID:    t.txnID,
		objectId: t.pageID,
		lockMode: lockMode,
	}

	n := l.pageLockManager.Upgrade(lockRequest)
	if n == nil {
		return optional.None[<-chan struct{}]()
	}
	return optional.Some(n)
}
