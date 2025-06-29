package txns

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestManagerBasicOperation(t *testing.T) {
	m := NewManager[RecordLockMode, RecordID]()

	// Test queue creation on first lock
	req := TxnLockRequest[RecordLockMode, RecordID]{
		txnID:    1,
		recordId: 100,
		lockMode: RECORD_LOCK_SHARED,
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
	m.Unlock(TxnUnlockRequest[RecordID]{txnID: 1, recordId: 100})

	// Verify queue persists after unlock
	m.qsGuard.Lock()
	if _, exists := m.qs[100]; !exists {
		t.Error("Queue should remain after unlock")
	}
	m.qsGuard.Unlock()
}

func TestManagerConcurrentRecordAccess(t *testing.T) {
	m := NewManager[RecordLockMode, RecordID]()

	var wg sync.WaitGroup

	for i := range 50 {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			//nolint:gosec
			recordID := RecordID(id & 1) // Two distinct records
			req := TxnLockRequest[RecordLockMode, RecordID]{
				txnID:    TxnID(id), //nolint:gosec
				recordId: recordID,
				lockMode: RECORD_LOCK_SHARED,
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
				TxnUnlockRequest[RecordID]{
					txnID:    TxnID(id),
					recordId: recordID,
				},
			) //nolint:gosec
			fmt.Printf("after unlock %d\n", id)
		}(i)
	}

	wg.Wait()
}

func TestManagerUnlockPanicScenarios(t *testing.T) {
	m := NewManager[RecordLockMode, RecordID]()

	// Test non-existent record panic
	t.Run("NonExistentRecord", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for non-existent record")
			}
		}()
		m.Unlock(TxnUnlockRequest[RecordID]{txnID: 1, recordId: 999})
	})

	// Test double unlock panic
	t.Run("DoubleUnlock", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for double unlock")
			}
		}()

		req := TxnLockRequest[RecordLockMode, RecordID]{
			txnID:    1,
			recordId: 200,
			lockMode: RECORD_LOCK_EXCLUSIVE,
		}
		notifier := m.Lock(req)
		expectClosedChannel(t, notifier, "Lock should be granted")
		m.Unlock(TxnUnlockRequest[RecordID]{txnID: 1, recordId: 200})
		m.Unlock(
			TxnUnlockRequest[RecordID]{txnID: 1, recordId: 200},
		) // Panic here
	})
}

func TestManagerLockContention(t *testing.T) {
	m := NewManager[RecordLockMode, RecordID]()
	recordID := RecordID(300)

	// First exclusive lock
	req1 := TxnLockRequest[RecordLockMode, RecordID]{
		txnID:    5,
		recordId: recordID,
		lockMode: RECORD_LOCK_EXCLUSIVE,
	}
	notifier1 := m.Lock(req1)
	expectClosedChannel(t, notifier1, "First exclusive lock should be granted")

	// Second exclusive lock (should block)
	req2 := TxnLockRequest[RecordLockMode, RecordID]{
		txnID:    4,
		recordId: recordID,
		lockMode: RECORD_LOCK_EXCLUSIVE,
	}
	notifier2 := m.Lock(req2)
	expectOpenChannel(t, notifier2, "Second exclusive lock should block")

	// Concurrent shared lock (should also block)
	req3 := TxnLockRequest[RecordLockMode, RecordID]{
		txnID:    3,
		recordId: recordID,
		lockMode: RECORD_LOCK_SHARED,
	}
	notifier3 := m.Lock(req3)
	expectOpenChannel(t, notifier3, "Shared lock should block behind exclusive")

	// Unlock first and verify chain
	m.Unlock(TxnUnlockRequest[RecordID]{txnID: 5, recordId: recordID})
	expectClosedChannel(
		t,
		notifier2,
		"Second lock should be granted after unlock",
	)
	m.Unlock(TxnUnlockRequest[RecordID]{txnID: 4, recordId: recordID})
	expectClosedChannel(
		t,
		notifier3,
		"Shared lock should be granted after exclusives",
	)
}

func TestManagerUnlockRetry(t *testing.T) {
	m := NewManager[RecordLockMode, RecordID]()
	recordID := RecordID(400)

	// Setup lock
	req := TxnLockRequest[RecordLockMode, RecordID]{
		txnID:    1,
		recordId: recordID,
		lockMode: RECORD_LOCK_EXCLUSIVE,
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
	m.Unlock(TxnUnlockRequest[RecordID]{txnID: 1, recordId: recordID})
	wg.Wait()
}

func TestManagerUnlockAll(t *testing.T) {
	m := NewManager[RecordLockMode, RecordID]()

	waitingTxn := TxnID(0)
	runningTxn := TxnID(1)

	notifier1x := m.Lock(TxnLockRequest[RecordLockMode, RecordID]{
		txnID:    runningTxn,
		recordId: 1,
		lockMode: RECORD_LOCK_EXCLUSIVE,
	})
	expectClosedChannel(
		t,
		notifier1x,
		"Txn 1 should have been granted the Exclusive Lock on 1",
	)

	notifier0s := m.Lock(TxnLockRequest[RecordLockMode, RecordID]{
		txnID:    waitingTxn,
		recordId: 1,
		lockMode: RECORD_LOCK_SHARED,
	})
	expectOpenChannel(t, notifier0s, "Txn 0 should be enqueued on record 1")

	m.UnlockAll(runningTxn)
	expectClosedChannel(
		t,
		notifier0s,
		"Txn 0 should have been granted the lock after the running transaction has finished",
	)
}

func TestManagerUpgrade(t *testing.T) {
	manager := NewManager[RecordLockMode, RecordID]()

	recordID := RecordID(uint64(1))
	f := manager.Lock(TxnLockRequest[RecordLockMode, RecordID]{
		txnID:    10,
		recordId: recordID,
		lockMode: RECORD_LOCK_SHARED,
	})
	expectClosedChannel(t, f, "should have been granted immediatly")

	s := manager.Lock(TxnLockRequest[RecordLockMode, RecordID]{
		txnID:    9,
		recordId: recordID,
		lockMode: RECORD_LOCK_SHARED,
	})
	expectClosedChannel(
		t,
		s,
		"should have been granted immediatly (the locks are compatible)",
	)

	writer := manager.Lock(TxnLockRequest[RecordLockMode, RecordID]{
		txnID:    8,
		recordId: recordID,
		lockMode: RECORD_LOCK_EXCLUSIVE,
	})
	expectOpenChannel(t, writer, "incompatible locks -> not granted immediatly")

	th := manager.Upgrade(TxnLockRequest[RecordLockMode, RecordID]{
		txnID:    10,
		recordId: recordID,
		lockMode: RECORD_LOCK_EXCLUSIVE,
	})
	expectOpenChannel(
		t,
		th,
		"there is still one more reader still reading a record -> lock isn't granted",
	)

	q := manager.qs[recordID]
	assert.Equal(t, 3, len(q.txnNodes))

	entry := q.txnNodes[TxnID(10)]
	assert.Equal(t, RECORD_LOCK_EXCLUSIVE, entry.r.lockMode)

	manager.Unlock(TxnUnlockRequest[RecordID]{
		txnID:    9,
		recordId: recordID,
	})
	expectClosedChannel(t, th, "upgraded lock should have been acquired first")
	expectOpenChannel(
		t,
		writer,
		"upgraded lock should have been acquired first",
	)
}
