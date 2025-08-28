package txns

import (
	"fmt"
	"strings"

	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
)

type Locker interface {
	LockCatalog(txnID common.TxnID, lockMode GranularLockMode) *CatalogLockToken
	LockFile(t *CatalogLockToken, fileID common.FileID, lockMode GranularLockMode) *FileLockToken
	LockPage(ft *FileLockToken, pageID common.PageID, lockMode PageLockMode) *PageLockToken
	Unlock(t *CatalogLockToken)
	UpgradeCatalogLock(t *CatalogLockToken, lockMode GranularLockMode) bool
	UpgradeFileLock(ft *FileLockToken, lockMode GranularLockMode) bool
	UpgradePageLock(pt *PageLockToken) bool
}

type HierarchyLocker struct {
	catalogLockManager *lockManager[GranularLockMode, struct{}]
	fileLockManager    *lockManager[GranularLockMode, common.FileID] // for indexes and tables
	pageLockManager    *lockManager[PageLockMode, common.PageIdentity]
}

var _ Locker = &HierarchyLocker{}

func NewHierarchyLocker() *HierarchyLocker {
	return &HierarchyLocker{
		catalogLockManager: NewManager[GranularLockMode, struct{}](),
		fileLockManager:    NewManager[GranularLockMode, common.FileID](),
		pageLockManager:    NewManager[PageLockMode, common.PageIdentity](),
	}
}

func (l *HierarchyLocker) DumpDependencyGraph() string {
	sb := strings.Builder{}

	plGraph := l.pageLockManager.GetGraphSnaphot()
	sb.WriteString(fmt.Sprintf("Page Locking [is cyclic:%v]:\n", plGraph.IsCyclic()))
	sb.WriteString(plGraph.Dump())
	sb.WriteString("\n")

	flGraph := l.fileLockManager.GetGraphSnaphot()
	sb.WriteString(fmt.Sprintf("File Locking [is cyclic:%v]:\n", flGraph.IsCyclic()))
	sb.WriteString(flGraph.Dump())
	sb.WriteString("\n")

	clGraph := l.catalogLockManager.GetGraphSnaphot()
	sb.WriteString(fmt.Sprintf("Catalog Locking [is cyclic:%v]:\n", clGraph.IsCyclic()))
	sb.WriteString(clGraph.Dump())
	return sb.String()
}

type CatalogLockToken struct {
	txnID    common.TxnID
	lockMode GranularLockMode
}

func newCatalogLockToken(
	txnID common.TxnID,
	mode GranularLockMode,
) *CatalogLockToken {
	return &CatalogLockToken{
		txnID:    txnID,
		lockMode: mode,
	}
}

type FileLockToken struct {
	txnID    common.TxnID
	fileID   common.FileID
	lockMode GranularLockMode

	ct *CatalogLockToken
}

func newFileLockToken(
	txnID common.TxnID,
	fileID common.FileID,
	lockMode GranularLockMode,
	ct *CatalogLockToken,
) *FileLockToken {
	return &FileLockToken{
		txnID:    txnID,
		fileID:   fileID,
		lockMode: lockMode,
		ct:       ct,
	}
}

type PageLockToken struct {
	txnID    common.TxnID
	lockMode PageLockMode
	ft       *FileLockToken
	pageID   common.PageIdentity
}

func newPageLockToken(
	txnID common.TxnID,
	pageID common.PageIdentity,
	lockMode PageLockMode,
	ft *FileLockToken,
) *PageLockToken {
	return &PageLockToken{
		txnID:    txnID,
		lockMode: lockMode,
		ft:       ft,
		pageID:   pageID,
	}
}

func (l *HierarchyLocker) LockCatalog(
	txnID common.TxnID,
	lockMode GranularLockMode,
) *CatalogLockToken {
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

	return newCatalogLockToken(r.txnID, lockMode)
}

func (l *HierarchyLocker) LockFile(
	t *CatalogLockToken,
	fileID common.FileID,
	lockMode GranularLockMode,
) *FileLockToken {
	switch lockMode {
	case GRANULAR_LOCK_INTENTION_SHARED,
		GRANULAR_LOCK_INTENTION_EXCLUSIVE,
		GRANULAR_LOCK_SHARED_INTENTION_EXCLUSIVE:
		if !l.UpgradeCatalogLock(t, lockMode) {
			return nil
		}
	case GRANULAR_LOCK_SHARED:
		if !l.UpgradeCatalogLock(t, GRANULAR_LOCK_INTENTION_SHARED) {
			return nil
		}
	case GRANULAR_LOCK_EXCLUSIVE:
		if !l.UpgradeCatalogLock(t, GRANULAR_LOCK_INTENTION_EXCLUSIVE) {
			return nil
		}
	default:
		assert.Assert(false, "invalid lock mode %v", lockMode)
		panic("unreachable")
	}

	n := l.fileLockManager.Lock(TxnLockRequest[GranularLockMode, common.FileID]{
		txnID:    t.txnID,
		objectId: fileID,
		lockMode: lockMode,
	})
	if n == nil {
		return nil
	}
	<-n
	return newFileLockToken(t.txnID, fileID, lockMode, t)
}

func (l *HierarchyLocker) LockPage(
	ft *FileLockToken,
	pageID common.PageID,
	lockMode PageLockMode,
) *PageLockToken {
	switch lockMode {
	case PAGE_LOCK_SHARED:
		if !l.UpgradeFileLock(ft, GRANULAR_LOCK_INTENTION_SHARED) {
			return nil
		}
	case PAGE_LOCK_EXCLUSIVE:
		if !l.UpgradeFileLock(ft, GRANULAR_LOCK_INTENTION_EXCLUSIVE) {
			return nil
		}
	}

	pageIdent := common.PageIdentity{
		FileID: ft.fileID,
		PageID: pageID,
	}

	lockRequest := TxnLockRequest[PageLockMode, common.PageIdentity]{
		txnID:    ft.txnID,
		objectId: pageIdent,
		lockMode: lockMode,
	}

	n := l.pageLockManager.Lock(lockRequest)
	if n == nil {
		return nil
	}
	<-n

	return newPageLockToken(ft.txnID, pageIdent, lockMode, ft)
}

func (l *HierarchyLocker) UnlockByTxnID(txnID common.TxnID) {
	l.catalogLockManager.UnlockAll(txnID)
	l.fileLockManager.UnlockAll(txnID)
	l.pageLockManager.UnlockAll(txnID)
}

func (l *HierarchyLocker) Unlock(t *CatalogLockToken) {
	l.catalogLockManager.UnlockAll(t.txnID)
	l.fileLockManager.UnlockAll(t.txnID)
	l.pageLockManager.UnlockAll(t.txnID)
}

func (l *HierarchyLocker) UpgradeCatalogLock(
	t *CatalogLockToken,
	lockMode GranularLockMode,
) bool {
	if lockMode.Upgradable(t.lockMode) || lockMode.Equal(t.lockMode) {
		return true
	}

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

func (l *HierarchyLocker) UpgradeFileLock(
	ft *FileLockToken,
	lockMode GranularLockMode,
) bool {
	if lockMode.Upgradable(ft.lockMode) || lockMode.Equal(ft.lockMode) {
		return true
	}

	switch lockMode {
	case GRANULAR_LOCK_INTENTION_SHARED,
		GRANULAR_LOCK_INTENTION_EXCLUSIVE,
		GRANULAR_LOCK_SHARED_INTENTION_EXCLUSIVE:
		if !l.UpgradeCatalogLock(ft.ct, lockMode) {
			return false
		}
	case GRANULAR_LOCK_SHARED:
		if !l.UpgradeCatalogLock(ft.ct, GRANULAR_LOCK_INTENTION_SHARED) {
			return false
		}
	case GRANULAR_LOCK_EXCLUSIVE:
		if !l.UpgradeCatalogLock(ft.ct, GRANULAR_LOCK_INTENTION_EXCLUSIVE) {
			return false
		}
	default:
		assert.Assert(false, "invalid lock mode %v", lockMode)
		return false
	}

	n := l.fileLockManager.Upgrade(
		TxnLockRequest[GranularLockMode, common.FileID]{
			txnID:    ft.txnID,
			objectId: ft.fileID,
			lockMode: lockMode,
		},
	)
	if n == nil {
		return false
	}
	<-n
	return true
}

func (l *HierarchyLocker) UpgradePageLock(pt *PageLockToken) bool {
	if pt.lockMode.Equal(PAGE_LOCK_EXCLUSIVE) {
		return true
	}

	if !l.UpgradeFileLock(pt.ft, GRANULAR_LOCK_INTENTION_EXCLUSIVE) {
		return false
	}

	lockRequest := TxnLockRequest[PageLockMode, common.PageIdentity]{
		txnID:    pt.txnID,
		objectId: pt.pageID,
		lockMode: PAGE_LOCK_EXCLUSIVE,
	}

	n := l.pageLockManager.Upgrade(lockRequest)
	if n == nil {
		return false
	}
	<-n
	return true
}

func (l *HierarchyLocker) GetActiveTransactions() []common.TxnID {
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

func (l *HierarchyLocker) AreAllQueuesEmpty() bool {
	return l.catalogLockManager.AreAllQueuesEmpty() &&
		l.fileLockManager.AreAllQueuesEmpty() &&
		l.pageLockManager.AreAllQueuesEmpty()
}
