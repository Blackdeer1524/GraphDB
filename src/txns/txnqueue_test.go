package txns

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func expectClosedChannel(t *testing.T, ch <-chan struct{}, mes string) {
	require.NotNil(t, ch)
	select {
	case <-ch:
	case <-time.After(100 * time.Millisecond):
		t.Error(mes)
	}
}

func expectOpenChannel(t *testing.T, ch <-chan struct{}, mes string) {
	require.NotNil(t, ch)
	select {
	case <-ch:
		t.Error(mes)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestSharedLockCompatibility shows proper lock compatibility
func TestSharedLockCompatibility(t *testing.T) {
	q := newTxnQueue[RecordLockMode, RecordID]()
	req1 := TxnLockRequest[RecordLockMode, RecordID]{
		txnID:    1,
		recordId: 1,
		lockMode: RECORD_LOCK_SHARED,
	}
	req2 := TxnLockRequest[RecordLockMode, RecordID]{
		txnID:    2,
		recordId: 1,
		lockMode: RECORD_LOCK_SHARED,
	}

	notifier1 := q.Lock(req1)
	notifier2 := q.Lock(req2)

	expectClosedChannel(
		t,
		notifier1,
		"Compatible shared locks should both be granted immediately",
	)
	expectClosedChannel(
		t,
		notifier2,
		"Compatible shared locks should both be granted immediately",
	)
}

// TestExclusiveBlocking demonstrates lock queueing
func TestExclusiveBlocking(t *testing.T) {
	q := newTxnQueue[RecordLockMode, RecordID]()
	req1 := TxnLockRequest[RecordLockMode, RecordID]{
		txnID:    2,
		recordId: 1,
		lockMode: RECORD_LOCK_SHARED,
	}
	req2 := TxnLockRequest[RecordLockMode, RecordID]{
		txnID:    1,
		recordId: 1,
		lockMode: RECORD_LOCK_EXCLUSIVE,
	}

	notifier1 := q.Lock(req1)
	expectClosedChannel(
		t,
		notifier1,
		"shared lock should have been granted immediately",
	)

	notifier2 := q.Lock(req2)
	expectOpenChannel(t, notifier2, "exclusive lock should have been enqueued")
}

// TestDeadlockPrevention verifies transaction age ordering
func TestDeadlockPrevention(t *testing.T) {
	q := newTxnQueue[RecordLockMode, RecordID]()

	// Older transaction (lower ID) first
	oldReq := TxnLockRequest[RecordLockMode, RecordID]{
		txnID:    1,
		recordId: 1,
		lockMode: RECORD_LOCK_EXCLUSIVE,
	}
	newReq := TxnLockRequest[RecordLockMode, RecordID]{
		txnID:    2,
		recordId: 1,
		lockMode: RECORD_LOCK_SHARED,
	}

	// Older transaction gets blocked (simulated)
	q.Lock(oldReq)

	// Younger transaction should abort
	result := q.Lock(newReq)
	if result != nil {
		t.Error(
			"Younger transaction should abort when blocking older transaction",
		)
	}
}

// TestConcurrentAccess checks for race conditions
func TestConcurrentAccess(t *testing.T) {
	q := newTxnQueue[RecordLockMode, RecordID]()

	var wg sync.WaitGroup

	for i := 1; i <= 10; i++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			req := TxnLockRequest[RecordLockMode, RecordID]{
				txnID:    TxnID(id), //nolint:gosec
				recordId: 1,
				lockMode: RECORD_LOCK_SHARED,
			}

			fmt.Printf("before lock %d\n", id)

			notifier := q.Lock(req)
			fmt.Printf("after lock %d\n", id)

			expectClosedChannel(
				t,
				notifier,
				"shared lock request should have been granted",
			)

			fmt.Printf("before unlock %d\n", id)
			q.Unlock(
				TxnUnlockRequest[RecordID]{txnID: TxnID(id), recordId: 1},
			) //nolint:gosec
			fmt.Printf("after unlock %d\n", id)
		}(i)
	}

	wg.Wait()
}

// TestExclusiveOrdering validates exclusive locks ordering
func TestExclusiveOrdering(t *testing.T) {
	q := newTxnQueue[RecordLockMode, RecordID]()
	req1 := TxnLockRequest[RecordLockMode, RecordID]{
		txnID:    9,
		recordId: 1,
		lockMode: RECORD_LOCK_EXCLUSIVE,
	}
	req2 := TxnLockRequest[RecordLockMode, RecordID]{
		txnID:    8,
		recordId: 1,
		lockMode: RECORD_LOCK_EXCLUSIVE,
	}

	notifier1 := q.Lock(req1)
	notifier2 := q.Lock(req2)

	expectClosedChannel(t, notifier1, "empty queue -> grant the lock")
	expectOpenChannel(
		t,
		notifier2,
		"shouldn't have granted the lock in presence of concurrent exclusive lock",
	)

	if !q.Unlock(TxnUnlockRequest[RecordID]{txnID: 9, recordId: 1}) {
		t.Errorf("no concurrent deleted -> couldn't have failed")
	}

	expectClosedChannel(t, notifier2, "empty queue -> grant the lock")
}

// TestLockFairness tests queue's fairness:
// can't grant a lock automatically if there is
// another transaction that have already requested a lock on a tuple
func TestLockFairness(t *testing.T) {
	q := newTxnQueue[RecordLockMode, RecordID]()
	req1 := TxnLockRequest[RecordLockMode, RecordID]{
		txnID:    9,
		recordId: 1,
		lockMode: RECORD_LOCK_SHARED,
	}
	req2 := TxnLockRequest[RecordLockMode, RecordID]{
		txnID:    8,
		recordId: 1,
		lockMode: RECORD_LOCK_EXCLUSIVE,
	}
	req3 := TxnLockRequest[RecordLockMode, RecordID]{
		txnID:    7,
		recordId: 1,
		lockMode: RECORD_LOCK_SHARED,
	}

	notifier1 := q.Lock(req1)
	notifier2 := q.Lock(req2)
	notifier3 := q.Lock(req3)

	expectClosedChannel(t, notifier1, "empty queue -> grant the lock")
	expectOpenChannel(t, notifier2, "incompatible lock -> wait")
	expectOpenChannel(
		t,
		notifier3,
		"waiting imcompatible lock -> can't grant the lock immediately",
	)
}
