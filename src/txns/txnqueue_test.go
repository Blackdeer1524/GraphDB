package txns

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
	defer assertQueueConsistency(t, q)

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
	defer assertQueueConsistency(t, q)

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
	defer assertQueueConsistency(t, q)

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
	defer assertQueueConsistency(t, q)

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

			notifier := q.Lock(req)
			expectClosedChannel(
				t,
				notifier,
				"shared lock request should have been granted",
			)

			fmt.Printf("before unlock %d\n", id)
			q.unlock(
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
	defer assertQueueConsistency(t, q)

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

	q.unlock(TxnUnlockRequest[common.PageID]{txnID: 9, objectId: 1})

	expectClosedChannel(t, notifier2, "empty queue -> grant the lock")
}

// TestLockFairness tests queue's fairness:
// can't grant a lock automatically if there is
// another transaction that have already requested a lock on a tuple
func TestLockFairness(t *testing.T) {
	q := newTxnQueue[PageLockMode, common.PageID]()
	defer assertQueueConsistency(t, q)

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
	defer assertQueueConsistency(t, q)

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
	defer assertQueueConsistency(t, q)

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
	defer assertQueueConsistency(t, q)

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
	defer assertQueueConsistency(t, q)

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

	q.unlock(TxnUnlockRequest[common.FileID]{
		txnID:    4,
		objectId: 1,
	})

	expectClosedChannel(t, notifier2, "waiter should be woken up after unlock")
}

func TestManagerUpgradeWithUpgradeWaiter(t *testing.T) {
	q := newTxnQueue[PageLockMode, common.FileID]()
	defer assertQueueConsistency(t, q)

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
	q.unlock(TxnUnlockRequest[common.FileID]{
		txnID:    req.txnID,
		objectId: req.objectId,
	})

	expectClosedChannel(
		t,
		upgradeNotifier2,
		"upgrade should be allowed to run [compatible locks]",
	)
}

func TestLockUpgradeIdempotent(t *testing.T) {
	q := newTxnQueue[PageLockMode, common.PageID]()
	defer assertQueueConsistency(t, q)

	// Start with a shared lock
	req := TxnLockRequest[PageLockMode, common.PageID]{
		txnID:    10,
		objectId: 1,
		lockMode: PAGE_LOCK_SHARED,
	}

	notifier := q.Lock(req)
	expectClosedChannel(
		t,
		notifier,
		"shared lock should be granted immediately",
	)

	// First upgrade to exclusive
	req.lockMode = PAGE_LOCK_EXCLUSIVE
	notifier1 := q.Upgrade(req)
	expectClosedChannel(
		t,
		notifier1,
		"first upgrade to exclusive should be granted immediately",
	)

	// Second upgrade to the same exclusive mode (idempotent)
	req.lockMode = PAGE_LOCK_EXCLUSIVE
	notifier2 := q.Upgrade(req)
	expectClosedChannel(
		t,
		notifier2,
		"second upgrade to same mode should be idempotent",
	)

	// Verify both notifiers are the same channel (idempotent behavior)
	require.Equal(
		t,
		notifier1,
		notifier2,
		"idempotent upgrades should return the same notifier",
	)

	// Third upgrade to the same exclusive mode (still idempotent)
	req.lockMode = PAGE_LOCK_EXCLUSIVE
	notifier3 := q.Upgrade(req)
	expectClosedChannel(
		t,
		notifier3,
		"third upgrade to same mode should still be idempotent",
	)

	// All notifiers should be the same
	require.Equal(
		t,
		notifier1,
		notifier2,
		"all idempotent upgrades should return the same notifier",
	)
	require.Equal(
		t,
		notifier2,
		notifier3,
		"all idempotent upgrades should return the same notifier",
	)

	// Verify the lock is still exclusive
	upgradingEntryAny, ok := q.txnNodes.Load(req.txnID)
	upgradingEntry := upgradingEntryAny.(*txnQueueEntry[PageLockMode, common.PageID])
	require.True(t, ok, "transaction should still exist in the queue")
	require.Equal(
		t,
		PAGE_LOCK_EXCLUSIVE,
		upgradingEntry.r.lockMode,
		"lock mode should remain exclusive",
	)
}

func TestLockUpgradeIdempotentWithGranularLocks(t *testing.T) {
	q := newTxnQueue[GranularLockMode, common.FileID]()
	defer assertQueueConsistency(t, q)

	// Start with a shared lock
	req := TxnLockRequest[GranularLockMode, common.FileID]{
		txnID:    15,
		objectId: 1,
		lockMode: GRANULAR_LOCK_SHARED,
	}

	notifier := q.Lock(req)
	expectClosedChannel(
		t,
		notifier,
		"shared lock should be granted immediately",
	)

	// First upgrade to shared intention exclusive
	req.lockMode = GRANULAR_LOCK_SHARED_INTENTION_EXCLUSIVE
	notifier1 := q.Upgrade(req)
	expectClosedChannel(
		t,
		notifier1,
		"first upgrade to SIX should be granted immediately",
	)

	// Second upgrade to the same SIX mode (idempotent)
	req.lockMode = GRANULAR_LOCK_SHARED_INTENTION_EXCLUSIVE
	notifier2 := q.Upgrade(req)
	expectClosedChannel(
		t,
		notifier2,
		"second upgrade to same SIX mode should be idempotent",
	)

	// Verify both notifiers are the same channel (idempotent behavior)
	require.Equal(
		t,
		notifier1,
		notifier2,
		"idempotent upgrades should return the same notifier",
	)

	// Third upgrade to exclusive
	req.lockMode = GRANULAR_LOCK_EXCLUSIVE
	notifier3 := q.Upgrade(req)
	expectClosedChannel(
		t,
		notifier3,
		"upgrade to exclusive should be granted immediately",
	)

	// Fourth upgrade to the same exclusive mode (idempotent)
	req.lockMode = GRANULAR_LOCK_EXCLUSIVE
	notifier4 := q.Upgrade(req)
	expectClosedChannel(
		t,
		notifier4,
		"upgrade to same exclusive mode should be idempotent",
	)

	// Verify the exclusive upgrade notifiers are the same
	require.Equal(
		t,
		notifier3,
		notifier4,
		"idempotent exclusive upgrades should return the same notifier",
	)

	// Verify the lock is now exclusive
	upgradingEntryAny, ok := q.txnNodes.Load(req.txnID)
	upgradingEntry := upgradingEntryAny.(*txnQueueEntry[GranularLockMode, common.FileID])
	require.True(t, ok, "transaction should still exist in the queue")
	require.Equal(
		t,
		GRANULAR_LOCK_EXCLUSIVE,
		upgradingEntry.r.lockMode,
		"lock mode should be exclusive",
	)
}

func assertQueueConsistency[LockModeType GranularLock[LockModeType], ObjectIDType comparable](
	t *testing.T,
	q *txnQueue[LockModeType, ObjectIDType],
) {
	txnNodes := make(map[*txnQueueEntry[LockModeType, ObjectIDType]]struct{})

	qLen := 0
	q.head.mu.Lock()
	defer q.head.mu.Unlock()

	cur := q.head.next
	cur.mu.Lock()
	for ; cur != q.tail; cur = cur.SafeNext() {
		qLen++
		txnNodes[cur] = struct{}{}
	}
	cur.mu.Unlock()

	qNodesLen := 0
	q.txnNodes.Range(func(key, value any) bool {
		_, ok := txnNodes[value.(*txnQueueEntry[LockModeType, ObjectIDType])]
		assert.True(t, ok, "transaction should exist in the queue")
		qNodesLen++
		return true
	})
	assert.Equal(t, qLen, qNodesLen, "queue length should match")
}

func TestAllowWeakerLock(t *testing.T) {
	q := newTxnQueue[PageLockMode, common.PageID]()
	defer assertQueueConsistency(t, q)

	recordID := common.PageID(500)

	req := TxnLockRequest[PageLockMode, common.PageID]{
		txnID:    1,
		objectId: recordID,
		lockMode: PAGE_LOCK_EXCLUSIVE,
	}
	notifier := q.Lock(req)
	expectClosedChannel(t, notifier, "Lock should be granted")

	req.lockMode = PAGE_LOCK_SHARED
	notifier2 := q.Lock(req)
	expectClosedChannel(t, notifier, "Lock should be granted")
	assert.Equal(t, notifier, notifier2)
}

func TestReinterpretLockAsUpgrade(t *testing.T) {
	q := newTxnQueue[PageLockMode, common.PageID]()
	defer assertQueueConsistency(t, q)

	req := TxnLockRequest[PageLockMode, common.PageID]{
		txnID:    1,
		objectId: 1,
		lockMode: PAGE_LOCK_SHARED,
	}

	notifier := q.Lock(req)
	expectClosedChannel(t, notifier, "Lock should be granted")

	req.lockMode = PAGE_LOCK_EXCLUSIVE
	notifier2 := q.Lock(req)
	expectClosedChannel(t, notifier2, "Lock should be granted")
	assert.Equal(t, notifier, notifier2)
}
