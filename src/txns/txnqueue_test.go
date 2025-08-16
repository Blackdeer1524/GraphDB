package txns

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
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
	q := newTxnQueue[PageLockMode, common.PageID]()
	req1 := TxnLockRequest[PageLockMode, common.PageID]{
		txnID:    1,
		objectId: 1,
		lockMode: PAGE_LOCK_SHARED,
	}
	req2 := TxnLockRequest[PageLockMode, common.PageID]{
		txnID:    2,
		objectId: 1,
		lockMode: PAGE_LOCK_SHARED,
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
	q := newTxnQueue[PageLockMode, common.PageID]()
	req1 := TxnLockRequest[PageLockMode, common.PageID]{
		txnID:    2,
		objectId: 1,
		lockMode: PAGE_LOCK_SHARED,
	}
	req2 := TxnLockRequest[PageLockMode, common.PageID]{
		txnID:    1,
		objectId: 1,
		lockMode: PAGE_LOCK_EXCLUSIVE,
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
	q := newTxnQueue[PageLockMode, common.PageID]()

	// Older transaction (lower ID) first
	oldReq := TxnLockRequest[PageLockMode, common.PageID]{
		txnID:    1,
		objectId: 1,
		lockMode: PAGE_LOCK_EXCLUSIVE,
	}
	newReq := TxnLockRequest[PageLockMode, common.PageID]{
		txnID:    2,
		objectId: 1,
		lockMode: PAGE_LOCK_SHARED,
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
	q := newTxnQueue[PageLockMode, common.PageID]()

	var wg sync.WaitGroup

	for i := 1; i <= 10; i++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			req := TxnLockRequest[PageLockMode, common.PageID]{
				txnID:    common.TxnID(id), //nolint:gosec
				objectId: 1,
				lockMode: PAGE_LOCK_SHARED,
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
				TxnUnlockRequest[common.PageID]{
					txnID:    common.TxnID(id),
					objectId: 1,
				},
			) //nolint:gosec
			fmt.Printf("after unlock %d\n", id)
		}(i)
	}

	wg.Wait()
}

// TestExclusiveOrdering validates exclusive locks ordering
func TestExclusiveOrdering(t *testing.T) {
	q := newTxnQueue[PageLockMode, common.PageID]()
	req1 := TxnLockRequest[PageLockMode, common.PageID]{
		txnID:    9,
		objectId: 1,
		lockMode: PAGE_LOCK_EXCLUSIVE,
	}
	req2 := TxnLockRequest[PageLockMode, common.PageID]{
		txnID:    8,
		objectId: 1,
		lockMode: PAGE_LOCK_EXCLUSIVE,
	}

	notifier1 := q.Lock(req1)
	notifier2 := q.Lock(req2)

	expectClosedChannel(t, notifier1, "empty queue -> grant the lock")
	expectOpenChannel(
		t,
		notifier2,
		"shouldn't have granted the lock in presence of concurrent exclusive lock",
	)

	q.Unlock(TxnUnlockRequest[common.PageID]{txnID: 9, objectId: 1})

	expectClosedChannel(t, notifier2, "empty queue -> grant the lock")
}

// TestLockFairness tests queue's fairness:
// can't grant a lock automatically if there is
// another transaction that have already requested a lock on a tuple
func TestLockFairness(t *testing.T) {
	q := newTxnQueue[PageLockMode, common.PageID]()
	req1 := TxnLockRequest[PageLockMode, common.PageID]{
		txnID:    9,
		objectId: 1,
		lockMode: PAGE_LOCK_SHARED,
	}
	req2 := TxnLockRequest[PageLockMode, common.PageID]{
		txnID:    8,
		objectId: 1,
		lockMode: PAGE_LOCK_EXCLUSIVE,
	}
	req3 := TxnLockRequest[PageLockMode, common.PageID]{
		txnID:    7,
		objectId: 1,
		lockMode: PAGE_LOCK_SHARED,
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

func TestLockcpgradeAlwaysAllowIfSingle(t *testing.T) {
	q := newTxnQueue[PageLockMode, common.PageID]()
	req := TxnLockRequest[PageLockMode, common.PageID]{
		txnID:    10,
		objectId: 1,
		lockMode: PAGE_LOCK_SHARED,
	}

	notifier := q.Lock(req)
	expectClosedChannel(t, notifier, "empty queue -> grant the lock")

	// Upgrade the lock
	req.lockMode = PAGE_LOCK_EXCLUSIVE
	notifier = q.Upgrade(req)
	expectClosedChannel(
		t,
		notifier,
		"single transaction -> upgrade should be allowed",
	)
}

func TestLockUpgradeAllowIfSingleWhenNoPendingUpgrades(t *testing.T) {
	q := newTxnQueue[PageLockMode, common.PageID]()
	req := TxnLockRequest[PageLockMode, common.PageID]{
		txnID:    10,
		objectId: 1,
		lockMode: PAGE_LOCK_SHARED,
	}

	notifier := q.Lock(req)
	expectClosedChannel(t, notifier, "empty queue -> grant the lock")

	req2 := TxnLockRequest[PageLockMode, common.PageID]{
		txnID:    2,
		objectId: 1,
		lockMode: PAGE_LOCK_EXCLUSIVE,
	}
	blockedReqNotifier := q.Lock(req2)
	expectOpenChannel(
		t,
		blockedReqNotifier,
		"incompatible lock -> should be blocked",
	)

	// Upgrade the lock
	req.lockMode = PAGE_LOCK_EXCLUSIVE
	notifier = q.Upgrade(req)
	expectClosedChannel(
		t,
		notifier,
		"single transaction -> upgrade should be allowed",
	)
}

func TestLockUpgradeForbidUpgradeIfDeadlock(t *testing.T) {
	q := newTxnQueue[PageLockMode, common.PageID]()
	req := TxnLockRequest[PageLockMode, common.PageID]{
		txnID:    3,
		objectId: 1,
		lockMode: PAGE_LOCK_SHARED,
	}

	notifier := q.Lock(req)
	expectClosedChannel(t, notifier, "empty queue -> grant the lock")

	req2 := TxnLockRequest[PageLockMode, common.PageID]{
		txnID:    2,
		objectId: 1,
		lockMode: PAGE_LOCK_SHARED,
	}
	blockedReqNotifier := q.Lock(req2)
	expectClosedChannel(
		t,
		blockedReqNotifier,
		"compatible lock -> grant the lock",
	)

	// Upgrade the lock
	req.lockMode = PAGE_LOCK_EXCLUSIVE
	notifier = q.Upgrade(req)
	require.Nil(t, notifier, "deadlock detected -> upgrade should be forbidden")
}

func TestLockUpgradeCompatibleLocks(t *testing.T) {
	q := newTxnQueue[GranularLockMode, common.FileID]()
	req := TxnLockRequest[GranularLockMode, common.FileID]{
		txnID:    4,
		objectId: 1,
		lockMode: GRANULAR_LOCK_SHARED,
	}

	notifier := q.Lock(req)
	expectClosedChannel(t, notifier, "empty queue -> grant the lock")

	req2 := TxnLockRequest[GranularLockMode, common.FileID]{
		txnID:    3,
		objectId: 1,
		lockMode: GRANULAR_LOCK_SHARED,
	}
	notifier2 := q.Lock(req2)
	expectClosedChannel(
		t,
		notifier2,
		"compatible lock -> grant the lock",
	)

	// Upgrade the lock
	req2.lockMode = GRANULAR_LOCK_SHARED_INTENTION_EXCLUSIVE
	notifier2 = q.Upgrade(req2)
	expectOpenChannel(
		t,
		notifier2,
		"no deadlock -> upgrade should be allowed [wait]",
	)

	q.Unlock(TxnUnlockRequest[common.FileID]{
		txnID:    4,
		objectId: 1,
	})

	expectClosedChannel(t, notifier2, "waiter should be woken up after unlock")
}

func TestManagerUpgradeWithUpgradeWaiter(t *testing.T) {
	q := newTxnQueue[PageLockMode, common.FileID]()

	req := TxnLockRequest[PageLockMode, common.FileID]{
		txnID:    4,
		objectId: 1,
		lockMode: PAGE_LOCK_SHARED,
	}

	notifier := q.Lock(req)
	expectClosedChannel(t, notifier, "empty queue -> grant the lock")

	req2 := TxnLockRequest[PageLockMode, common.FileID]{
		txnID:    3,
		objectId: 1,
		lockMode: PAGE_LOCK_SHARED,
	}

	notifier2 := q.Lock(req2)
	expectClosedChannel(t, notifier2, "compatible lock -> grant the lock")

	req2.lockMode = PAGE_LOCK_EXCLUSIVE
	upgradeNotifier2 := q.Upgrade(req2)
	expectOpenChannel(
		t,
		upgradeNotifier2,
		"no deadlock -> upgrade should be allowed [wait]",
	)

	req.lockMode = PAGE_LOCK_EXCLUSIVE
	upgradeNotifier1 := q.Upgrade(req)

	require.Nil(t, upgradeNotifier1)
	q.Unlock(TxnUnlockRequest[common.FileID]{
		txnID:    req.txnID,
		objectId: req.objectId,
	})

	expectClosedChannel(
		t,
		upgradeNotifier2,
		"upgrade should be allowed to run [compatible locks]",
	)
}
