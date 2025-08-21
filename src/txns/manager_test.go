package txns

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
)

func TestManagerBasicOperation(t *testing.T) {
	m := NewManager[PageLockMode, common.PageID]()

	// Test queue creation on first lock
	req := TxnLockRequest[PageLockMode, common.PageID]{
		txnID:    1,
		objectId: 100,
		lockMode: PAGE_LOCK_SHARED,
	}
	notifier := m.Lock(req)
	expectClosedChannel(t, notifier, "Initial lock should be granted")

	// Verify queue exists
	m.qsGuard.Lock()
	if _, exists := m.qs[100]; !exists {
		t.Error("Manager should create queue for new record ID")
	}
	m.qsGuard.Unlock()

	// Successful unlock
	m.Unlock(TxnUnlockRequest[common.PageID]{txnID: 1, objectId: 100})

	// Verify queue persists after unlock
	m.qsGuard.Lock()
	if _, exists := m.qs[100]; !exists {
		t.Error("Queue should remain after unlock")
	}
	m.qsGuard.Unlock()
}

func TestManagerConcurrentRecordAccess(t *testing.T) {
	m := NewManager[PageLockMode, common.PageID]()

	var wg sync.WaitGroup

	for i := range 50 {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			//nolint:gosec
			recordID := common.PageID(id & 1) // Two distinct records
			req := TxnLockRequest[PageLockMode, common.PageID]{
				txnID:    common.TxnID(id), //nolint:gosec
				objectId: recordID,
				lockMode: PAGE_LOCK_SHARED,
			}

			notifier := m.Lock(req)
			expectClosedChannel(
				t,
				notifier,
				"Concurrent access to different records should work",
			)

			m.Unlock(
				TxnUnlockRequest[common.PageID]{
					txnID:    common.TxnID(id),
					objectId: recordID,
				},
			) //nolint:gosec
		}(i)
	}

	wg.Wait()
}

func TestManagerUnlockPanicScenarios(t *testing.T) {
	m := NewManager[PageLockMode, common.PageID]()

	// Test non-existent record panic
	t.Run("NonExistentRecord", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for non-existent record")
			}
		}()
		m.Unlock(TxnUnlockRequest[common.PageID]{txnID: 1, objectId: 999})
	})

	// Test double unlock panic
	t.Run("DoubleUnlock", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for double unlock")
			}
		}()

		req := TxnLockRequest[PageLockMode, common.PageID]{
			txnID:    1,
			objectId: 200,
			lockMode: PAGE_LOCK_EXCLUSIVE,
		}
		notifier := m.Lock(req)
		expectClosedChannel(t, notifier, "Lock should be granted")
		m.Unlock(TxnUnlockRequest[common.PageID]{txnID: 1, objectId: 200})
		m.Unlock(
			TxnUnlockRequest[common.PageID]{txnID: 1, objectId: 200},
		) // Panic here
	})
}

func TestManagerLockContention(t *testing.T) {
	m := NewManager[PageLockMode, common.PageID]()
	recordID := common.PageID(300)

	// First exclusive lock
	req1 := TxnLockRequest[PageLockMode, common.PageID]{
		txnID:    5,
		objectId: recordID,
		lockMode: PAGE_LOCK_EXCLUSIVE,
	}
	notifier1 := m.Lock(req1)
	expectClosedChannel(t, notifier1, "First exclusive lock should be granted")

	// Second exclusive lock (should block)
	req2 := TxnLockRequest[PageLockMode, common.PageID]{
		txnID:    4,
		objectId: recordID,
		lockMode: PAGE_LOCK_EXCLUSIVE,
	}
	notifier2 := m.Lock(req2)
	expectOpenChannel(t, notifier2, "Second exclusive lock should block")

	// Concurrent shared lock (should also block)
	req3 := TxnLockRequest[PageLockMode, common.PageID]{
		txnID:    3,
		objectId: recordID,
		lockMode: PAGE_LOCK_SHARED,
	}
	notifier3 := m.Lock(req3)
	expectOpenChannel(t, notifier3, "Shared lock should block behind exclusive")

	// Unlock first and verify chain
	m.Unlock(TxnUnlockRequest[common.PageID]{txnID: 5, objectId: recordID})
	expectClosedChannel(
		t,
		notifier2,
		"Second lock should be granted after unlock",
	)
	m.Unlock(TxnUnlockRequest[common.PageID]{txnID: 4, objectId: recordID})
	expectClosedChannel(
		t,
		notifier3,
		"Shared lock should be granted after exclusives",
	)
}

func TestManagerUnlockRetry(t *testing.T) {
	m := NewManager[PageLockMode, common.PageID]()
	recordID := common.PageID(400)

	// Setup lock
	req := TxnLockRequest[PageLockMode, common.PageID]{
		txnID:    1,
		objectId: recordID,
		lockMode: PAGE_LOCK_EXCLUSIVE,
	}
	notifier := m.Lock(req)
	expectClosedChannel(t, notifier, "Lock should be granted")

	// Block the unlock path
	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		// Acquire lock on previous node to force retry
		m.qs[recordID].head.mu.Lock()
		time.Sleep(50 * time.Millisecond) // Hold lock briefly
		m.qs[recordID].head.mu.Unlock()
		wg.Done()
	}()

	// This should retry until successful
	m.Unlock(TxnUnlockRequest[common.PageID]{txnID: 1, objectId: recordID})
	wg.Wait()
}

func TestManagerUnlockAll(t *testing.T) {
	m := NewManager[PageLockMode, common.PageID]()
	defer func() {
		for _, q := range m.qs {
			assertQueueConsistency(t, q)
		}
	}()

	waitingTxn := common.TxnID(0)
	runningTxn := common.TxnID(1)

	notifier1x := m.Lock(TxnLockRequest[PageLockMode, common.PageID]{
		txnID:    runningTxn,
		objectId: 1,
		lockMode: PAGE_LOCK_EXCLUSIVE,
	})
	expectClosedChannel(
		t,
		notifier1x,
		"Txn 1 should have been granted the Exclusive Lock on 1",
	)

	notifier0s := m.Lock(TxnLockRequest[PageLockMode, common.PageID]{
		txnID:    waitingTxn,
		objectId: 1,
		lockMode: PAGE_LOCK_SHARED,
	})
	expectOpenChannel(t, notifier0s, "Txn 0 should be enqueued on record 1")

	m.UnlockAll(runningTxn)
	expectClosedChannel(
		t,
		notifier0s,
		"Txn 0 should have been granted the lock after the running transaction has finished",
	)
}

func waitWithDeadline(
	notifier <-chan struct{},
	condFlag *bool,
	graphBuilderNotifier *sync.Cond,
	timeout time.Duration,
) {
	timer := time.NewTimer(timeout)
	select {
	case <-notifier:
		timer.Stop()
	case <-timer.C:
		graphBuilderNotifier.L.Lock()
		*condFlag = true
		graphBuilderNotifier.Signal()
		graphBuilderNotifier.L.Unlock()
	}
}

func TestManagerConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping slow test in short mode")
	}

	m := NewManager[PageLockMode, common.PageID]()
	defer func() {
		for _, q := range m.qs {
			assertQueueConsistency(t, q)
		}
	}()

	numTxns := 100
	numObjects := 10
	opsPerTxn := 10
	deadline := time.Second * 0

	var wg sync.WaitGroup
	var mu sync.Mutex
	failedTxns := make(map[common.TxnID]bool)

	lockModes := []PageLockMode{
		PAGE_LOCK_SHARED,
		PAGE_LOCK_EXCLUSIVE,
	}

	condMu := sync.Mutex{}
	graphBuildingFlag := false
	cond := sync.NewCond(&mu)

	go func() {
		condMu.Lock()
		for !graphBuildingFlag {
			cond.Wait()
		}
		condMu.Unlock()

		graph := m.GetGraphSnaphot()
		if graph.IsCyclic() {
			t.Logf(
				"Dependency graph should be acyclic, but is not. Graph: %s",
				graph.Dump(),
			)
			t.Fail()
		} else {
			t.Logf("Have been waiting for too long. Graph: %s", graph.Dump())
			t.Fail()
		}
	}()

	for txnID := range numTxns {
		wg.Add(1)

		go func(txnID int) {
			defer wg.Done()

			txn := common.TxnID(txnID)
			lockedObjects := make(map[common.PageID]struct{})

			defer m.UnlockAll(txn)

			for op := range opsPerTxn {
				if len(lockedObjects) > 0 && op%3 == 0 {
					for objectID := range lockedObjects {
						if rand.Intn(2) == 0 {
							continue
						}

						upgradeReq := TxnLockRequest[PageLockMode, common.PageID]{
							txnID:    txn,
							objectId: objectID,
							lockMode: PAGE_LOCK_EXCLUSIVE,
						}

						notifier := m.Upgrade(upgradeReq)
						if notifier == nil {
							mu.Lock()
							failedTxns[txn] = true
							mu.Unlock()
							m.UnlockAll(txn)
							return
						}

						waitWithDeadline(
							notifier,
							&graphBuildingFlag,
							cond,
							deadline,
						)
						break
					}
				} else {
					objectID := common.PageID(rand.Intn(numObjects))
					lockMode := lockModes[txnID%len(lockModes)]

					req := TxnLockRequest[PageLockMode, common.PageID]{
						txnID:    txn,
						objectId: objectID,
						lockMode: lockMode,
					}

					notifier := m.Lock(req)
					if notifier == nil {
						mu.Lock()
						failedTxns[txn] = true
						mu.Unlock()
						return
					}

					waitWithDeadline(
						notifier,
						&graphBuildingFlag,
						cond,
						deadline,
					)
					lockedObjects[objectID] = struct{}{}
				}

				time.Sleep(time.Millisecond * time.Duration(txnID%3))
			}
		}(txnID)
	}
	wg.Wait()

	// Verify final state
	mu.Lock()
	numFailed := len(failedTxns)
	mu.Unlock()

	t.Logf(
		"Concurrency test completed. Failed transactions: %d/%d",
		numFailed,
		numTxns,
	)

	assert.True(
		t,
		m.AreAllQueuesEmpty(),
		"Some queues are not empty after all transactions completed",
	)

	activeTxns := m.GetActiveTransactions()
	assert.Equal(t, len(activeTxns), 0,
		"Expected no active transactions, but found %d",
		len(activeTxns),
	)
}
