package txns

import (
	"fmt"
	"sync"
	"testing"
	"time"

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

			fmt.Printf("before lock %d\n", id)

			notifier := m.Lock(req)
			fmt.Printf("after lock %d\n", id)
			expectClosedChannel(
				t,
				notifier,
				"Concurrent access to different records should work",
			)

			fmt.Printf("before unlock %d\n", id)
			m.Unlock(
				TxnUnlockRequest[common.PageID]{
					txnID:    common.TxnID(id),
					objectId: recordID,
				},
			) //nolint:gosec
			fmt.Printf("after unlock %d\n", id)
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
