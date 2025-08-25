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

	generatedFileIDs := utils.GenerateUniqueInts[common.FileID](2, 0, 1024)

	masterRecordPageIdent := common.PageIdentity{
		FileID: generatedFileIDs[0],
		PageID: checkpointInfoPageID,
	}

	const (
		startBalance      = uint32(60)
		rollbackCutoff    = uint32(0) // startBalance / 3
		clientsCount      = 7
		txnsCount         = 10_000
		retryCount        = 1
		maxEntriesPerPage = 2
		workersCount      = 10_000
	)

	pagesLowerBound := uint64(clientsCount / maxEntriesPerPage)
	// pagesUpperBound := uint64(clientsCount) + 10

	diskManager := disk.NewInMemoryManager()
	pool := bufferpool.NewDebugBufferPool(
		bufferpool.New(pagesLowerBound, bufferpool.NewLRUReplacer(), diskManager),
		map[common.PageIdentity]struct{}{
			masterRecordPageIdent: {},
		},
	)
	files := generatedFileIDs[1:]
	defer func() {
		assert.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked())
	}()

	setupLoggerMasterPage(
		t,
		pool,
		masterRecordPageIdent,
		common.FileLocation{
			PageID:  1,
			SlotNum: 0,
		},
	)
	logger := NewTxnLogger(pool, generatedFileIDs[0])
	pool.SetLogger(logger)

	workerPool, err := ants.NewPool(workersCount)
	require.NoError(t, err)

	recordValues := fillPages(
		t,
		logger,
		math.MaxUint64,
		clientsCount,
		files,
		startBalance,
		maxEntriesPerPage,
	)
	require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked())

	txnsTicker := atomic.Uint64{}

	totalMoney := uint32(0)
	for id := range recordValues {
		pg, err := pool.GetPageNoCreate(id.PageIdentity())
		require.NoError(t, err)
		require.NoError(t,
			pool.WithMarkDirty(
				common.NilTxnID,
				id.PageIdentity(),
				pg,
				func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
					lockedPage.UnsafeUpdateNoLogs(id.SlotNum, utils.ToBytes[uint32](startBalance))
					return common.NewNilLogRecordLocation(), nil
				},
			))
		totalMoney += startBalance
		pool.Unpin(id.PageIdentity())
	}

	IDs := []common.RecordID{}
	for i := range recordValues {
		IDs = append(IDs, i)
	}

	locker := txns.NewHierarchyLocker()
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
		waitTime := 20
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

		res := utils.GenerateUniqueInts[int](2, 0, len(IDs)-1)
		me := IDs[res[0]]
		first := IDs[res[1]]

		err := logger.AppendBegin()
		require.NoError(t, err)

		ctoken := locker.LockCatalog(
			txnID,
			txns.GRANULAR_LOCK_INTENTION_SHARED,
		)
		require.NotNil(t, ctoken)
		defer locker.Unlock(ctoken)

		ttoken := locker.LockFile(
			ctoken,
			common.FileID(me.FileID),
			txns.GRANULAR_LOCK_INTENTION_SHARED,
		)
		if ttoken == nil {
			fileLockFail.Add(1)
			err = logger.AppendAbort()
			require.NoError(t, err)
			logger.Rollback()
			return false
		}

		myPageToken := locker.LockPage(
			ttoken,
			common.PageID(me.PageID),
			txns.PAGE_LOCK_SHARED,
		)
		if myPageToken == nil {
			myPageLockFail.Add(1)
			err = logger.AppendAbort()
			require.NoError(t, err)
			logger.Rollback()
			return false
		}

		myPage, err := pool.GetPageNoCreate(me.PageIdentity())
		require.NoError(t, err)
		defer func() { pool.Unpin(me.PageIdentity()) }()

		myPage.RLock()
		myBalance := utils.FromBytes[uint32](myPage.Read(me.SlotNum))
		myPage.RUnlock()

		if myBalance == 0 {
			balanceFail.Add(1)
			err = logger.AppendAbort()
			require.NoError(t, err)
			logger.Rollback()
			return false
		}

		// try to read the first guy's balance
		firstPageToken := locker.LockPage(
			ttoken,
			common.PageID(first.PageID),
			txns.PAGE_LOCK_SHARED,
		)
		if firstPageToken == nil {
			firstPageLockFail.Add(1)
			err = logger.AppendAbort()
			require.NoError(t, err)
			logger.Rollback()
			return false
		}

		firstPage, err := pool.GetPageNoCreate(first.PageIdentity())
		require.NoError(t, err)
		defer func() { pool.Unpin(first.PageIdentity()) }()

		firstPage.RLock()
		firstBalance := utils.FromBytes[uint32](firstPage.Read(first.SlotNum))
		firstPage.RUnlock()

		// transfering
		transferAmount := uint32(rand.Intn(int(myBalance)))
		if !locker.UpgradePageLock(myPageToken) {
			myPageUpgradeFail.Add(1)
			err = logger.AppendAbort()
			require.NoError(t, err)
			logger.Rollback()
			return false
		}

		if !locker.UpgradePageLock(firstPageToken) {
			firstPageUpgradeFail.Add(1)
			err = logger.AppendAbort()
			require.NoError(t, err)
			logger.Rollback()
			return false
		}

		myNewBalance := utils.ToBytes[uint32](myBalance - transferAmount)
		err = pool.WithMarkDirty(
			txnID,
			me.PageIdentity(),
			myPage,
			func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
				return lockedPage.UpdateWithLogs(myNewBalance, me, logger)
			},
		)
		require.NoError(t, err)

		firstNewBalance := utils.ToBytes[uint32](firstBalance + transferAmount)
		err = pool.WithMarkDirty(
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

		myPage.RLock()
		myNewBalanceFromPage := utils.FromBytes[uint32](myPage.Read(me.SlotNum))
		myPage.RUnlock()
		require.Equal(t, myNewBalanceFromPage, myBalance-transferAmount)

		firstPage.RLock()
		firstNewBalanceFromPage := utils.FromBytes[uint32](
			firstPage.Read(first.SlotNum),
		)
		require.Equal(
			t,
			firstNewBalanceFromPage,
			firstBalance+transferAmount,
		)
		firstPage.RUnlock()

		if myNewBalanceFromPage < rollbackCutoff {
			rollbackCutoffFail.Add(1)
			err = logger.AppendAbort()
			require.NoError(t, err)
			logger.Rollback()
			return false
		}
		err = logger.AppendCommit()
		require.NoError(t, err)
		succ.Add(1)
		return true
	}

	wg := sync.WaitGroup{}
	retryingTask := func() {
		defer wg.Done()
		txnID := common.TxnID(txnsTicker.Add(1))
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

	successCount := succ.Load()
	assert.Greater(t, successCount, uint64(0))
	if int(successCount) < txnsCount/2 {
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
	}

	t.Log("ensuring consistency...")
	finalTotalMoney := uint32(0)
	for id := range recordValues {
		page, err := pool.GetPageNoCreate(id.PageIdentity())
		require.NoError(t, err)
		page.RLock()
		curMoney := utils.FromBytes[uint32](page.Read(id.SlotNum))
		finalTotalMoney += curMoney
		page.RUnlock()
		pool.Unpin(id.PageIdentity())
	}
	require.Equal(t, finalTotalMoney, totalMoney)
}
