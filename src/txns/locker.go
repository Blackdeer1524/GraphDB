package txns

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
)

var ErrDeadlockPrevention = errors.New("deadlock prevention")

type ILockManager interface {
	LockCatalog(txnID common.TxnID, lockMode GranularLockMode) *CatalogLockToken
	LockFile(t *CatalogLockToken, fileID common.FileID, lockMode GranularLockMode) *FileLockToken
	LockPage(ft *FileLockToken, pageID common.PageID, lockMode PageLockMode) *PageLockToken
	Unlock(txnID common.TxnID)
	UpgradeCatalogLock(t *CatalogLockToken, lockMode GranularLockMode) bool
	UpgradeFileLock(ft *FileLockToken, lockMode GranularLockMode) bool
	UpgradePageLock(pt *PageLockToken, lockMode PageLockMode) bool
}

type LockManager struct {
	catalogLockManager *lockGranularityManager[GranularLockMode, struct{}]
	fileLockManager    *lockGranularityManager[GranularLockMode, common.FileID] // for indexes and tables
	pageLockManager    *lockGranularityManager[PageLockMode, common.PageIdentity]
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

func (t *CatalogLockToken) WasSetUp() bool {
	return t.wasSetUp
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
	if !t.WasSetUp() {
		return "CatalogLockToken{nil}"
	}
	return fmt.Sprintf("CatalogLockToken{txnID: %v, lockMode: %s}", t.txnID, t.lockMode)
}

func (t *CatalogLockToken) GetTxnID() common.TxnID {
	return t.txnID
}

type FileLockToken struct {
	wasSetUp bool

	txnID    common.TxnID
	fileID   common.FileID
	lockMode GranularLockMode

	ct *CatalogLockToken
}

func NewNilFileLockToken(ct *CatalogLockToken, fileID common.FileID) *FileLockToken {
	assert.Assert(ct != nil, "catalog lock token shouldn't be nil")

	return &FileLockToken{
		wasSetUp: false,
		txnID:    ct.txnID,
		fileID:   fileID,
		lockMode: GranularLockShared,
		ct:       ct,
	}
}

func (t *FileLockToken) WasSetUp() bool {
	return t.wasSetUp
}

func (t *FileLockToken) GetTxnID() common.TxnID {
	return t.txnID
}

func (f *FileLockToken) GetFileID() common.FileID {
	return f.fileID
}

func (f *FileLockToken) GetCatalogLockToken() *CatalogLockToken {
	return f.ct
}

func (t *FileLockToken) String() string {
	if !t.WasSetUp() {
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
	assert.Assert(ct != nil, "catalog lock token shouldn't be nil")

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

func (t *PageLockToken) WasSetUp() bool {
	return t.wasSetUp
}

func (t *PageLockToken) String() string {
	if !t.WasSetUp() {
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
	assert.Assert(ft != nil, "file lock token shouldn't be nil")

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
	assert.Assert(ft != nil, "file lock token shouldn't be nil")

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
	ct *CatalogLockToken,
	fileID common.FileID,
	lockMode GranularLockMode,
) *FileLockToken {
	switch lockMode {
	case GranularLockIntentionShared, GranularLockIntentionExclusive:
		if !l.UpgradeCatalogLock(ct, lockMode) {
			return nil
		}
	case GranularLockSharedIntentionExclusive:
		if !l.UpgradeCatalogLock(ct, GranularLockIntentionExclusive) {
			return nil
		}
	case GranularLockShared:
		if !l.UpgradeCatalogLock(ct, GranularLockIntentionShared) {
			return nil
		}
	case GranularLockExclusive:
		if !l.UpgradeCatalogLock(ct, GranularLockIntentionExclusive) {
			return nil
		}
	default:
		assert.Assert(false, "invalid lock mode %v", lockMode)
		panic("unreachable")
	}

	n := l.fileLockManager.Lock(TxnLockRequest[GranularLockMode, common.FileID]{
		txnID:    ct.txnID,
		objectId: fileID,
		lockMode: lockMode,
	})
	if n == nil {
		return nil
	}
	<-n
	return newFileLockToken(fileID, lockMode, ct)
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
		switch ft.lockMode {
		case GranularLockExclusive:
			res := NewPageLockToken(
				common.PageIdentity{FileID: ft.fileID, PageID: pageID},
				PageLockExclusive,
				ft,
			)
			return res
		case GranularLockShared:
			res := NewPageLockToken(
				common.PageIdentity{FileID: ft.fileID, PageID: pageID},
				PageLockShared,
				ft,
			)
			return res
		}
	case PageLockExclusive:
		if !l.UpgradeFileLock(ft, GranularLockIntentionExclusive) {
			return nil
		}
		if ft.lockMode == GranularLockExclusive {
			res := NewPageLockToken(
				common.PageIdentity{FileID: ft.fileID, PageID: pageID},
				PageLockExclusive,
				ft,
			)
			return res
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
	if !t.WasSetUp() {
		ct := l.LockCatalog(t.txnID, lockMode)
		if ct == nil {
			return false
		}
		*t = *ct
		return true
	}

	if lockMode.WeakerOrEqual(t.lockMode) {
		return true
	}

	req := TxnLockRequest[GranularLockMode, struct{}]{
		txnID:    t.txnID,
		objectId: struct{}{},
		lockMode: lockMode,
	}
	n := l.catalogLockManager.Upgrade(req)
	if n == nil {
		return false
	}
	<-n
	t.lockMode = t.lockMode.Combine(lockMode)
	return true
}

func (l *LockManager) UpgradeFileLock(
	ft *FileLockToken,
	reqLockMode GranularLockMode,
) bool {
	switch reqLockMode {
	case GranularLockIntentionShared:
		if !l.UpgradeCatalogLock(ft.ct, GranularLockIntentionShared) {
			return false
		}
	case GranularLockIntentionExclusive:
		if !l.UpgradeCatalogLock(ft.ct, GranularLockIntentionExclusive) {
			return false
		}
	case GranularLockShared:
		if !l.UpgradeCatalogLock(ft.ct, GranularLockIntentionShared) {
			return false
		}
	case GranularLockSharedIntentionExclusive:
		if !l.UpgradeCatalogLock(ft.ct, GranularLockIntentionExclusive) {
			return false
		}
	case GranularLockExclusive:
		if !l.UpgradeCatalogLock(ft.ct, GranularLockIntentionExclusive) {
			return false
		}
	default:
		assert.Assert(false, "invalid lock mode %v", reqLockMode)
		panic("unreachable")
	}

	if !ft.WasSetUp() {
		innerFt := l.LockFile(ft.ct, ft.fileID, reqLockMode)
		if innerFt == nil {
			return false
		}
		*ft = *innerFt
		return true
	}

	switch ft.ct.lockMode {
	case GranularLockExclusive:
		ft.lockMode = ft.lockMode.Combine(GranularLockExclusive)
	case GranularLockShared:
		ft.lockMode = ft.lockMode.Combine(GranularLockShared)
	case GranularLockSharedIntentionExclusive:
		ft.lockMode = ft.lockMode.Combine(GranularLockShared)
	}

	if reqLockMode.WeakerOrEqual(ft.lockMode) {
		return true
	}

	req := TxnLockRequest[GranularLockMode, common.FileID]{
		txnID:    ft.txnID,
		objectId: ft.fileID,
		lockMode: reqLockMode,
	}
	n := l.fileLockManager.Upgrade(req)
	if n == nil {
		return false
	}
	<-n

	ft.lockMode = ft.lockMode.Combine(reqLockMode)
	return true
}

func (l *LockManager) UpgradePageLock(pt *PageLockToken, lockMode PageLockMode) bool {
	switch lockMode {
	case PageLockShared:
		if !l.UpgradeFileLock(pt.ft, GranularLockIntentionShared) {
			return false
		}
	case PageLockExclusive:
		if !l.UpgradeFileLock(pt.ft, GranularLockIntentionExclusive) {
			return false
		}
	}

	if !pt.WasSetUp() {
		innerPt := l.LockPage(pt.ft, pt.pageID.PageID, lockMode)
		if innerPt == nil {
			return false
		}
		*pt = *innerPt
		return true
	}

	switch pt.ft.lockMode {
	case GranularLockExclusive:
		pt.lockMode = pt.lockMode.Combine(PageLockExclusive)
	case GranularLockShared:
		pt.lockMode = pt.lockMode.Combine(PageLockShared)
	case GranularLockSharedIntentionExclusive:
		pt.lockMode = pt.lockMode.Combine(PageLockShared)
	}

	if lockMode.WeakerOrEqual(pt.lockMode) {
		return true
	}

	req := TxnLockRequest[PageLockMode, common.PageIdentity]{
		txnID:    pt.txnID,
		objectId: pt.pageID,
		lockMode: lockMode,
	}
	n := l.pageLockManager.Upgrade(req)
	if n == nil {
		return false
	}
	<-n

	pt.lockMode = pt.lockMode.Combine(lockMode)
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
