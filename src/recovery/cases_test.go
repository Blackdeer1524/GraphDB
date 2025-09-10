package recovery

import (
	"math"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/panjf2000/ants"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
	"github.com/Blackdeer1524/GraphDB/src/storage/disk"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

func TestBankTransactions(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping slow test in short mode")
	}

	generatedFileIDs := utils.GenerateUniqueInts[common.FileID](
		2,
		0,
		1024,
		rand.New(rand.NewSource(42)),
	)

	masterRecordPageIdent := common.PageIdentity{
		FileID: generatedFileIDs[0],
		PageID: common.CheckpointInfoPageID,
	}

	const (
		startBalance      = uint32(60)
		rollbackCutoff    = startBalance / 3
		clientsCount      = 5000
		txnsCount         = 10000
		retryCount        = 3
		maxEntriesPerPage = 12
		workersCount      = 1_000
	)

	pagesLowerBound := uint64(clientsCount / maxEntriesPerPage)
	// pagesUpperBound := uint64(clientsCount) + 10

	diskManager := disk.NewInMemoryManager()
	pool := bufferpool.New(pagesLowerBound, bufferpool.NewLRUReplacer(), diskManager)
	debugPool := bufferpool.NewDebugBufferPool(pool)
	debugPool.MarkPageAsLeaking(masterRecordPageIdent)
	files := generatedFileIDs[1:]
	defer func() {
		assert.NoError(t, debugPool.EnsureAllPagesUnpinnedAndUnlocked())
	}()

	setupLoggerMasterPage(
		t,
		debugPool,
		masterRecordPageIdent.FileID,
		common.LogRecordLocInfo{
			Lsn:      common.NilLSN,
			Location: common.FileLocation{PageID: common.CheckpointInfoPageID + 1, SlotNum: 0},
		},
	)
	logger := NewTxnLogger(debugPool, generatedFileIDs[0])

	workerPool, err := ants.NewPool(workersCount)
	require.NoError(t, err)

	t.Logf("filling pages...")
	recordValues := fillPages(
		t,
		logger,
		math.MaxUint64,
		clientsCount,
		files,
		startBalance,
		maxEntriesPerPage,
	)
	require.NoError(t, debugPool.EnsureAllPagesUnpinnedAndUnlocked())
	t.Logf("filled pages")

	totalMoney := uint32(0)
	for id := range recordValues {
		pg, err := debugPool.GetPageNoCreate(id.PageIdentity())
		require.NoError(t, err)
		require.NoError(t,
			debugPool.WithMarkDirty(
				common.NilTxnID,
				id.PageIdentity(),
				pg,
				func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
					lockedPage.UnsafeUpdateNoLogs(id.SlotNum, utils.ToBytes[uint32](startBalance))
					return common.NewNilLogRecordLocation(), nil
				},
			))
		totalMoney += startBalance
		debugPool.Unpin(id.PageIdentity())
	}

	IDs := []common.RecordID{}
	for i := range recordValues {
		IDs = append(IDs, i)
	}

	locker := txns.NewLockManager()
	defer func() {
		stillLockedTxns := locker.GetActiveTransactions()
		assert.Equal(
			t,
			0,
			len(stillLockedTxns),
			"There are still locked transactions: %+v",
			stillLockedTxns,
		)
		assert.True(t, locker.AreAllQueuesEmpty())
	}()

	graphDump := func() {
		waitTime := 25
		t.Logf("Waiting for %d seconds...\n", waitTime)
		<-time.After(time.Duration(waitTime) * time.Second)

		t.Logf("Have been waiting for too long. Creating a graph...\n")
		graph := locker.DumpDependencyGraph()
		t.Logf("%s", graph)
	}
	require.NoError(t, workerPool.Submit(graphDump))

	succ := atomic.Uint64{}
	fileLockFail := atomic.Uint64{}
	myPageLockFail := atomic.Uint64{}
	balanceFail := atomic.Uint64{}
	firstPageLockFail := atomic.Uint64{}
	myPageUpgradeFail := atomic.Uint64{}
	firstPageUpgradeFail := atomic.Uint64{}
	rollbackCutoffFail := atomic.Uint64{}
	catalogUpgradeFail := atomic.Uint64{}
	fileLockUpgradeFail := atomic.Uint64{}
	task := func(txnID common.TxnID) bool {
		logger := logger.WithContext(txnID)

		res := utils.GenerateUniqueInts[int](2, 0, len(IDs)-1, rand.New(rand.NewSource(42)))
		me := IDs[res[0]]
		first := IDs[res[1]]

		err := logger.AppendBegin()
		require.NoError(t, err)

		ctoken := locker.LockCatalog(
			txnID,
			txns.GranularLockIntentionShared,
		)
		require.NotNil(t, ctoken)
		defer locker.Unlock(txnID)

		t.Logf("[%d] locking file %d", txnID, me.FileID)
		ttoken := locker.LockFile(
			ctoken,
			common.FileID(me.FileID),
			txns.GranularLockIntentionShared,
		)
		if ttoken == nil {
			t.Logf("[%d] failed to lock file %d", txnID, me.FileID)
			fileLockFail.Add(1)
			err = logger.AppendAbort()
			require.NoError(t, err)
			logger.Rollback()
			return false
		}
		t.Logf("[%d] locked file %d", txnID, me.FileID)

		t.Logf("[%d] locking page %d", txnID, me.PageID)
		myPageToken := locker.LockPage(
			ttoken,
			common.PageID(me.PageID),
			txns.PageLockShared,
		)
		if myPageToken == nil {
			t.Logf("[%d] failed to lock page %d", txnID, me.PageID)
			myPageLockFail.Add(1)
			err = logger.AppendAbort()
			require.NoError(t, err)
			logger.Rollback()
			return false
		}
		t.Logf("[%d] locked page %d", txnID, me.PageID)

		myPage, err := debugPool.GetPageNoCreate(me.PageIdentity())
		require.NoError(t, err)
		defer func() { debugPool.Unpin(me.PageIdentity()) }()

		myBalance := utils.FromBytes[uint32](myPage.LockedRead(me.SlotNum))

		if myBalance == 0 {
			t.Logf("[%d] balance is 0", txnID)
			balanceFail.Add(1)
			err = logger.AppendAbort()
			require.NoError(t, err)
			logger.Rollback()
			return false
		}
		t.Logf("[%d] balance is not 0", txnID)

		// try to read the first guy's balance
		t.Logf("[%d] locking first page %d", txnID, first.PageID)
		firstPageToken := locker.LockPage(
			ttoken,
			common.PageID(first.PageID),
			txns.PageLockShared,
		)
		if firstPageToken == nil {
			t.Logf("[%d] failed to lock first page %d", txnID, first.PageID)
			firstPageLockFail.Add(1)
			err = logger.AppendAbort()
			require.NoError(t, err)
			logger.Rollback()
			return false
		}
		t.Logf("[%d] locked first page %d", txnID, first.PageID)

		firstPage, err := debugPool.GetPageNoCreate(first.PageIdentity())
		require.NoError(t, err)
		defer func() { debugPool.Unpin(first.PageIdentity()) }()

		firstBalance := utils.FromBytes[uint32](firstPage.LockedRead(first.SlotNum))

		// transfering
		t.Logf("[%d] upgrading page lock %d", txnID, me.PageID)
		transferAmount := uint32(rand.Intn(int(myBalance)))
		if !locker.UpgradePageLock(myPageToken, txns.PageLockExclusive) {
			t.Logf("[%d] failed to upgrade page lock %d", txnID, me.PageID)
			myPageUpgradeFail.Add(1)
			err = logger.AppendAbort()
			require.NoError(t, err)
			logger.Rollback()
			return false
		}
		t.Logf("[%d] upgraded page lock %d", txnID, me.PageID)

		t.Logf("[%d] upgrading first page lock %d", txnID, first.PageID)
		if !locker.UpgradePageLock(firstPageToken, txns.PageLockExclusive) {
			t.Logf("[%d] failed to upgrade first page lock %d", txnID, first.PageID)
			firstPageUpgradeFail.Add(1)
			err = logger.AppendAbort()
			require.NoError(t, err)
			logger.Rollback()
			return false
		}
		t.Logf("[%d] upgraded first page lock %d", txnID, first.PageID)

		t.Logf("[%d] updating my page %d", txnID, me.PageID)
		myNewBalance := utils.ToBytes[uint32](myBalance - transferAmount)
		err = debugPool.WithMarkDirty(
			txnID,
			me.PageIdentity(),
			myPage,
			func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
				return lockedPage.UpdateWithLogs(myNewBalance, me, logger)
			},
		)
		require.NoError(t, err)
		t.Logf("[%d] updated my page %d", txnID, me.PageID)

		t.Logf("[%d] updating first page %d", txnID, first.PageID)
		firstNewBalance := utils.ToBytes[uint32](firstBalance + transferAmount)
		err = debugPool.WithMarkDirty(
			txnID,
			first.PageIdentity(),
			firstPage,
			func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
				return lockedPage.UpdateWithLogs(
					firstNewBalance,
					first,
					logger,
				)
			},
		)
		require.NoError(t, err)
		t.Logf("[%d] updated first page %d", txnID, first.PageID)

		t.Logf("[%d] reading my page %d", txnID, me.PageID)
		myNewBalanceFromPage := utils.FromBytes[uint32](myPage.LockedRead(me.SlotNum))
		require.Equal(t, myNewBalanceFromPage, myBalance-transferAmount)
		t.Logf("[%d] read my page %d", txnID, me.PageID)

		t.Logf("[%d] reading first page %d", txnID, first.PageID)
		firstNewBalanceFromPage := utils.FromBytes[uint32](
			firstPage.LockedRead(first.SlotNum),
		)
		require.Equal(
			t,
			firstNewBalanceFromPage,
			firstBalance+transferAmount,
		)
		t.Logf("[%d] read first page %d", txnID, first.PageID)

		if myNewBalanceFromPage < rollbackCutoff {
			t.Logf("[%d] rollback cutoff is not met", txnID)
			rollbackCutoffFail.Add(1)
			err = logger.AppendAbort()
			require.NoError(t, err)
			logger.Rollback()
			return false
		}
		t.Logf("[%d] rollback cutoff is met", txnID)

		err = logger.AppendCommit()
		require.NoError(t, err)
		t.Logf("[%d] committed", txnID)
		succ.Add(1)
		return true
	}

	wg := sync.WaitGroup{}

	txnsTicker := atomic.Uint64{}
	t.Logf("generating txn IDs...")
	txnIDs := utils.GenerateUniqueInts[common.TxnID](
		txnsCount,
		1,
		txnsCount+1,
		rand.New(rand.NewSource(42)),
	)
	t.Logf("generated txn IDs")
	retryingTask := func() {
		defer wg.Done()
		txnID := txnIDs[txnsTicker.Add(1)-1]
		for range retryCount {
			if task(txnID) {
				return
			}
			runtime.Gosched()
		}
	}

	for range txnsCount {
		wg.Add(1)
		require.NoError(t, workerPool.Submit(retryingTask))
	}
	wg.Wait()

	assert.Equal(t, txnsCount, int(txnsTicker.Load()))

	totalFail := int(fileLockFail.Load() +
		myPageLockFail.Load() +
		balanceFail.Load() +
		firstPageLockFail.Load() +
		catalogUpgradeFail.Load() +
		fileLockUpgradeFail.Load() +
		myPageUpgradeFail.Load() +
		firstPageUpgradeFail.Load() +
		rollbackCutoffFail.Load())

	assert.Less(t, totalFail, retryCount*txnsCount, "totalFail: %d", totalFail)

	successCount := succ.Load()
	assert.Greater(t, successCount, uint64(0))
	t.Logf(
		"fileLockFail: %d\n"+
			"myPageLockFail: %d\n"+
			"balanceFail: %d\n"+
			"firstPageLockFail: %d\n"+
			"catalogUpgradeFail: %d\n"+
			"fileLockUpgradeFail: %d\n"+
			"myPageUpgradeFail: %d\n"+
			"firstPageUpgradeFail: %d\n"+
			"rollbackCutoffFail: %d\n",
		fileLockFail.Load(),
		myPageLockFail.Load(),
		balanceFail.Load(),
		firstPageLockFail.Load(),
		catalogUpgradeFail.Load(),
		fileLockUpgradeFail.Load(),
		myPageUpgradeFail.Load(),
		firstPageUpgradeFail.Load(),
		rollbackCutoffFail.Load(),
	)

	t.Log("ensuring consistency...")
	finalTotalMoney := uint32(0)
	for id := range recordValues {
		page, err := debugPool.GetPageNoCreate(id.PageIdentity())
		require.NoError(t, err)
		curMoney := utils.FromBytes[uint32](page.LockedRead(id.SlotNum))
		finalTotalMoney += curMoney
		debugPool.Unpin(id.PageIdentity())
	}
	require.Equal(t, finalTotalMoney, totalMoney)
}
