package txns

import (
	"fmt"
	"strings"

	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
)

type ILockManager interface {
	LockCatalog(txnID common.TxnID, lockMode GranularLockMode) *CatalogLockToken
	LockFile(t *CatalogLockToken, fileID common.FileID, lockMode GranularLockMode) *FileLockToken
	LockPage(ft *FileLockToken, pageID common.PageID, lockMode PageLockMode) *PageLockToken
	Unlock(txnID common.TxnID)
	UpgradeCatalogLock(t *CatalogLockToken, lockMode GranularLockMode) bool
	UpgradeFileLock(ft *FileLockToken, lockMode GranularLockMode) bool
	UpgradePageLock(pt *PageLockToken) bool
}

type LockManager struct {
	catalogLockManager *lockManager[GranularLockMode, struct{}]
	fileLockManager    *lockManager[GranularLockMode, common.FileID] // for indexes and tables
	pageLockManager    *lockManager[PageLockMode, common.PageIdentity]
}

var _ ILockManager = &LockManager{}

func NewLockManager() *LockManager {
	return &LockManager{
		catalogLockManager: NewManager[GranularLockMode, struct{}](),
		fileLockManager:    NewManager[GranularLockMode, common.FileID](),
		pageLockManager:    NewManager[PageLockMode, common.PageIdentity](),
	}
}

func (l *LockManager) DumpDependencyGraph() string {
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
	wasSetUp bool
	txnID    common.TxnID
	lockMode GranularLockMode
}

func NewNilCatalogLockToken(txnID common.TxnID) *CatalogLockToken {
	return &CatalogLockToken{
		wasSetUp: false,
		txnID:    txnID,
		lockMode: GranularLockShared,
	}
}

func (t *CatalogLockToken) IsNil() bool {
	return !t.wasSetUp
}

func NewCatalogLockToken(
	txnID common.TxnID,
	mode GranularLockMode,
) *CatalogLockToken {
	return &CatalogLockToken{
		txnID:    txnID,
		lockMode: mode,
		wasSetUp: true,
	}
}

func (t *CatalogLockToken) String() string {
	if t.IsNil() {
		return "CatalogLockToken{nil}"
	}
	return fmt.Sprintf("CatalogLockToken{txnID: %v, lockMode: %s}", t.txnID, t.lockMode)
}

type FileLockToken struct {
	wasSetUp bool

	txnID    common.TxnID
	fileID   common.FileID
	lockMode GranularLockMode

	ct *CatalogLockToken
}

func NewNilFileLockToken(ct *CatalogLockToken, fileID common.FileID) *FileLockToken {
	return &FileLockToken{
		wasSetUp: false,
		txnID:    ct.txnID,
		fileID:   fileID,
		lockMode: GranularLockShared,
		ct:       ct,
	}
}

func (t *FileLockToken) IsNil() bool {
	return !t.wasSetUp
}

func (t *FileLockToken) GetTxnID() common.TxnID {
	return t.txnID
}

func (f *FileLockToken) GetFileID() common.FileID {
	return f.fileID
}

func (t *FileLockToken) String() string {
	if t.IsNil() {
		return "FileLockToken{nil}"
	}
	return fmt.Sprintf(
		"FileLockToken{txnID: %v, fileID: %v, lockMode: %s}",
		t.txnID,
		t.fileID,
		t.lockMode,
	)
}

func newFileLockToken(
	fileID common.FileID,
	lockMode GranularLockMode,
	ct *CatalogLockToken,
) *FileLockToken {
	return &FileLockToken{
		wasSetUp: true,
		txnID:    ct.txnID,
		fileID:   fileID,
		lockMode: lockMode,
		ct:       ct,
	}
}

type PageLockToken struct {
	wasSetUp bool

	txnID    common.TxnID
	lockMode PageLockMode
	ft       *FileLockToken
	pageID   common.PageIdentity
}

func (t *PageLockToken) IsNil() bool {
	return !t.wasSetUp
}

func (t *PageLockToken) String() string {
	if t.IsNil() {
		return "PageLockToken{nil}"
	}

	return fmt.Sprintf(
		"PageLockToken{txnID: %v, lockMode: %s, ft: %s, pageID: %v}",
		t.txnID,
		t.lockMode,
		t.ft,
		t.pageID,
	)
}

func NewNilPageLockToken(ft *FileLockToken, pageIdent common.PageIdentity) *PageLockToken {
	return &PageLockToken{
		wasSetUp: false,
		txnID:    ft.txnID,
		lockMode: PageLockShared,
		ft:       ft,
		pageID:   pageIdent,
	}
}

func NewPageLockToken(
	pageID common.PageIdentity,
	lockMode PageLockMode,
	ft *FileLockToken,
) *PageLockToken {
	return &PageLockToken{
		wasSetUp: true,
		txnID:    ft.txnID,
		lockMode: lockMode,
		ft:       ft,
		pageID:   pageID,
	}
}

func (l *LockManager) LockCatalog(
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

	return NewCatalogLockToken(r.txnID, lockMode)
}

func (l *LockManager) LockFile(
	t *CatalogLockToken,
	fileID common.FileID,
	lockMode GranularLockMode,
) *FileLockToken {
	switch lockMode {
	case GranularLockIntentionShared,
		GranularLockIntentionExclusive,
		GranularLockSharedIntentionExclusive:
		if !l.UpgradeCatalogLock(t, lockMode) {
			return nil
		}
	case GranularLockShared:
		if !l.UpgradeCatalogLock(t, GranularLockIntentionShared) {
			return nil
		}
	case GranularLockExclusive:
		if !l.UpgradeCatalogLock(t, GranularLockIntentionExclusive) {
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
	return newFileLockToken(fileID, lockMode, t)
}

func (l *LockManager) LockPage(
	ft *FileLockToken,
	pageID common.PageID,
	lockMode PageLockMode,
) *PageLockToken {
	switch lockMode {
	case PageLockShared:
		if !l.UpgradeFileLock(ft, GranularLockIntentionShared) {
			return nil
		}
	case PageLockExclusive:
		if !l.UpgradeFileLock(ft, GranularLockIntentionExclusive) {
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

	return NewPageLockToken(pageIdent, lockMode, ft)
}

func (l *LockManager) Unlock(txnID common.TxnID) {
	l.catalogLockManager.UnlockAll(txnID)
	l.fileLockManager.UnlockAll(txnID)
	l.pageLockManager.UnlockAll(txnID)
}

func (l *LockManager) UpgradeCatalogLock(
	t *CatalogLockToken,
	lockMode GranularLockMode,
) bool {
	req := TxnLockRequest[GranularLockMode, struct{}]{
		txnID:    t.txnID,
		objectId: struct{}{},
		lockMode: lockMode,
	}
	if t.IsNil() {
		ct := l.LockCatalog(t.txnID, lockMode)
		if ct == nil {
			return false
		}
		*t = *ct
		return true
	}

	if lockMode.Upgradable(t.lockMode) || lockMode.Equal(t.lockMode) {
		return true
	}
	n := l.catalogLockManager.Upgrade(req)
	if n == nil {
		return false
	}
	<-n
	return true
}

func (l *LockManager) UpgradeFileLock(
	ft *FileLockToken,
	lockMode GranularLockMode,
) bool {
	if !ft.IsNil() && (lockMode.Upgradable(ft.lockMode) || lockMode.Equal(ft.lockMode)) {
		return true
	}

	switch lockMode {
	case GranularLockIntentionShared,
		GranularLockIntentionExclusive,
		GranularLockSharedIntentionExclusive:
		if !l.UpgradeCatalogLock(ft.ct, lockMode) {
			return false
		}
	case GranularLockShared:
		if !l.UpgradeCatalogLock(ft.ct, GranularLockIntentionShared) {
			return false
		}
	case GranularLockExclusive:
		if !l.UpgradeCatalogLock(ft.ct, GranularLockIntentionExclusive) {
			return false
		}
	default:
		assert.Assert(false, "invalid lock mode %v", lockMode)
		return false
	}

	req := TxnLockRequest[GranularLockMode, common.FileID]{
		txnID:    ft.txnID,
		objectId: ft.fileID,
		lockMode: lockMode,
	}

	if ft.IsNil() {
		innerFt := l.LockFile(ft.ct, ft.fileID, lockMode)
		if innerFt == nil {
			return false
		}
		*ft = *innerFt
		return true
	}

	n := l.fileLockManager.Upgrade(req)
	if n == nil {
		return false
	}
	<-n
	return true
}

func (l *LockManager) UpgradePageLock(pt *PageLockToken) bool {
	if !pt.IsNil() && pt.lockMode.Equal(PageLockExclusive) {
		return true
	}

	if !l.UpgradeFileLock(pt.ft, GranularLockIntentionExclusive) {
		return false
	}

	req := TxnLockRequest[PageLockMode, common.PageIdentity]{
		txnID:    pt.txnID,
		objectId: pt.pageID,
		lockMode: PageLockExclusive,
	}

	if pt.IsNil() {
		innerPt := l.LockPage(pt.ft, pt.pageID.PageID, PageLockExclusive)
		if innerPt == nil {
			return false
		}
		*pt = *innerPt
		return true
	}

	n := l.pageLockManager.Upgrade(req)
	if n == nil {
		return false
	}
	<-n
	return true
}

func (l *LockManager) GetActiveTransactions() []common.TxnID {
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

func (l *LockManager) AreAllQueuesEmpty() bool {
	return l.catalogLockManager.AreAllQueuesEmpty() &&
		l.fileLockManager.AreAllQueuesEmpty() &&
		l.pageLockManager.AreAllQueuesEmpty()
}
