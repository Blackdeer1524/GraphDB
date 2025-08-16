package txns

import (
	"math"
	"sync"

	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
)

type entryStatus byte

const (
	entryStatusRunning entryStatus = iota
	entryStatusWaitUpgrade
	entryStatusWaitAcquire
)

type txnQueueEntry[LockModeType GranularLock[LockModeType], ObjectIDType comparable] struct {
	r        TxnLockRequest[LockModeType, ObjectIDType]
	notifier chan struct{}
	status   entryStatus

	mu   sync.Mutex
	next *txnQueueEntry[LockModeType, ObjectIDType]
	prev *txnQueueEntry[LockModeType, ObjectIDType]
}

// SafeNext safely advances to the next txnQueueEntry in the queue.
// It acquires the lock on the next entry before releasing the lock on the
// current entry, ensuring that the transition between entries is thread-safe
// and prevents race conditions.
// Returns a pointer to the next txnQueueEntry.
func (lockedEntry *txnQueueEntry[LockModeType, ObjectIDType]) SafeNext() *txnQueueEntry[LockModeType, ObjectIDType] {
	next := lockedEntry.next
	assert.Assert(next != nil, "precondition is violated")

	next.mu.Lock()
	lockedEntry.mu.Unlock()

	return next
}

func (lockedEntry *txnQueueEntry[LockModeType, ObjectIDType]) SafeDeleteNext() {
	cur := lockedEntry.next
	cur.mu.Lock()

	next := cur.next
	next.mu.Lock()
	defer next.mu.Unlock()
	cur.mu.Unlock()

	lockedEntry.next = next
	next.prev = lockedEntry
}

// SafeInsert inserts the given txnQueueEntry 'n' immediately after the current
// (locked) entry in the queue. It updates the necessary pointers to maintain
// the doubly-linked list structure. The method assumes that the current entry
// is already locked, and it locks the 'next' entry to safely update its 'prev'
// pointer, ensuring thread safety during the insertion.
func (lockedEntry *txnQueueEntry[LockModeType, ObjectIDType]) SafeInsert(
	n *txnQueueEntry[LockModeType, ObjectIDType],
) {
	next := lockedEntry.next

	n.prev = lockedEntry
	n.next = next

	lockedEntry.next = n

	next.mu.Lock()
	next.prev = n
	next.mu.Unlock()
}

type txnQueue[LockModeType GranularLock[LockModeType], ObjectIDType comparable] struct {
	head *txnQueueEntry[LockModeType, ObjectIDType]
	tail *txnQueueEntry[LockModeType, ObjectIDType]

	mu       sync.Mutex
	txnNodes map[common.TxnID]*txnQueueEntry[LockModeType, ObjectIDType]
}

// processBatch processes a batch of transactions in the txnQueue starting from
// the given muGuardedHead entry. It iterates through the queue, granting locks
// to transactions whose lock modes are compatible with all previously granted
// lock modes in the current batch. For each compatible transaction, it marks
// the entry as acquired and notifies the waiting transaction by closing its
// notifier channel. The function ensures that only a prefix of the queue is in
// the locked state at any time, and stops processing when an incompatible
// lock mode is encountered or the end of the queue is reached.
//
// Preconditions:
//   - muGuardedHead must not be already acquired (isAcquired == false).
//   - muGuardedHead.mu must be locked by the caller.
//
// Postconditions:
// - All compatible transactions in the batch are granted the lock and notified.
//   - Only the processed prefix of the queue is in the locked state.
func (q *txnQueue[LockModeType, ObjectIDType]) processBatch(
	muGuardedHead *txnQueueEntry[LockModeType, ObjectIDType],
) {
	assert.Assert(
		muGuardedHead.status != entryStatusRunning,
		"processBatch contract is violated",
	)

	cur := muGuardedHead
	defer func() { cur.mu.Unlock() }()

	if cur == q.tail {
		return
	}

	seenLockModes := make(map[GranularLock[LockModeType]]struct{})
outer:
	for {
		for seenMode := range seenLockModes {
			if !seenMode.Compatible(cur.r.lockMode) {
				break outer
			}
		}

		seenLockModes[cur.r.lockMode] = struct{}{}

		cur.status = entryStatusRunning
		close(cur.notifier) // grants the lock to the transaction

		if cur.next == q.tail {
			break
		}

		cur = cur.SafeNext()
		assert.Assert(cur.status == entryStatusWaitAcquire, "only list prefix is allowed to be in the locked state")
	}
}

func newTxnQueue[LockModeType GranularLock[LockModeType], ObjectIDType comparable]() *txnQueue[LockModeType, ObjectIDType] {
	head := &txnQueueEntry[LockModeType, ObjectIDType]{
		r: TxnLockRequest[LockModeType, ObjectIDType]{
			txnID: math.MaxUint64, // Needed for the deadlock prevention policy
		},
	}
	tail := &txnQueueEntry[LockModeType, ObjectIDType]{
		r: TxnLockRequest[LockModeType, ObjectIDType]{
			txnID: 0, // Needed for the deadlock prevention policy
		},
	}
	head.next = tail
	tail.prev = head

	q := &txnQueue[LockModeType, ObjectIDType]{
		head: head,
		tail: tail,

		mu:       sync.Mutex{},
		txnNodes: map[common.TxnID]*txnQueueEntry[LockModeType, ObjectIDType]{},
	}

	return q
}

func checkDeadlockCondition(
	enqueuedTxnID common.TxnID,
	requestingTxnID common.TxnID,
) bool {
	// Deadlock prevention policy
	// Only older transactions can wait for younger ones.
	// Ohterwise, a younger transaction is aborted
	return enqueuedTxnID < requestingTxnID
}

// Lock attempts to acquire a lock for the given transaction lock request `r` in
// the transaction queue. It enforces a deadlock prevention policy where only
// older transactions can wait for younger ones; if a younger transaction
// attempts to wait for an older one, it is aborted (returns nil).
//
// If the requested lock is compatible with all currently held locks, the lock
// is granted immediately and a closed channel is returned. Otherwise, the
// request is queued and a channel is returned that will be closed when the lock
// is eventually granted. The returned channel can be used to wait for
// lock acquisition.
func (q *txnQueue[LockModeType, ObjectIDType]) Lock(
	r TxnLockRequest[LockModeType, ObjectIDType],
) <-chan struct{} {
	// Fast path - locks are compatible
	cur := q.head
	cur.mu.Lock()
	defer func() { cur.mu.Unlock() }()

	if cur.next == q.tail {
		notifier := make(chan struct{})
		close(notifier) // Grant the lock immediately
		newNode := &txnQueueEntry[LockModeType, ObjectIDType]{
			r:        r,
			notifier: notifier,
			status:   entryStatusRunning,
		}
		cur.SafeInsert(newNode)

		q.mu.Lock()
		q.txnNodes[r.txnID] = newNode
		q.mu.Unlock()

		return notifier
	}

	cur = cur.SafeNext()
	locksAreCompatible := true
	deadlockCondition := false
	for cur.status == entryStatusRunning {
		if cur.r.txnID == r.txnID {
			// the current transaction already owns the lock
			// grant the lock immediately
			return cur.notifier
		}

		deadlockCondition = deadlockCondition ||
			checkDeadlockCondition(cur.r.txnID, r.txnID)
		locksAreCompatible = locksAreCompatible &&
			r.lockMode.Compatible(cur.r.lockMode)
		if !locksAreCompatible {
			break
		}

		if cur.next == q.tail {
			notifier := make(chan struct{})
			close(notifier) // Grant the lock immediately
			newNode := &txnQueueEntry[LockModeType, ObjectIDType]{
				r:        r,
				notifier: notifier,
				status:   entryStatusRunning,
			}
			cur.SafeInsert(newNode)

			q.mu.Lock()
			q.txnNodes[r.txnID] = newNode
			q.mu.Unlock()

			return notifier
		}
		cur = cur.SafeNext()
	}

	if deadlockCondition {
		return nil
	}

	for cur.next != q.tail {
		cur = cur.SafeNext()
		assert.Assert(
			cur.r.txnID != r.txnID,
			"trying to lock already locked transaction. %+v",
			r,
		)

		if checkDeadlockCondition(cur.r.txnID, r.txnID) {
			return nil
		}
	}

	notifier := make(chan struct{})
	newNode := &txnQueueEntry[LockModeType, ObjectIDType]{
		r:        r,
		notifier: notifier,
		status:   entryStatusWaitAcquire,
	}
	cur.SafeInsert(newNode)

	q.mu.Lock()
	q.txnNodes[r.txnID] = newNode
	q.mu.Unlock()

	return notifier
}

// WARN: if it is unsuccessfull, don't forget to UNLOCK all the transaction's
// locks
func (q *txnQueue[LockModeType, ObjectIDType]) Upgrade(
	r TxnLockRequest[LockModeType, ObjectIDType],
) <-chan struct{} {
	q.mu.Lock()
	upgradingEntry, ok := q.txnNodes[r.txnID]
	q.mu.Unlock()
	assert.Assert(ok, "can't find upgrading entry")

	upgradingEntry.mu.Lock()
	assert.Assert(
		upgradingEntry.status == entryStatusRunning,
		"can only upgrade running transactions",
	)
	oldLockMode := upgradingEntry.r.lockMode
	upgradingEntry.mu.Unlock()

	assert.Assert(
		oldLockMode.Upgradable(r.lockMode),
		"can only upgrade to a compatible lock mode. given: %v -> %v",
		oldLockMode,
		r.lockMode)

	deadlockCond := false
	compatible := true

	var entryBeforeUpgradingOne *txnQueueEntry[LockModeType, ObjectIDType] = nil
	cur := q.head
	cur.mu.Lock()
	for {
		entryBeforeUpgradingOne = cur
		cur = cur.next
		cur.mu.Lock()
		if cur.r.txnID == r.txnID {
			break
		}
		entryBeforeUpgradingOne.mu.Unlock()

		deadlockCond = deadlockCond ||
			checkDeadlockCondition(cur.r.txnID, r.txnID)
		compatible = compatible && cur.r.lockMode.Compatible(r.lockMode)
	}
	assert.Assert(entryBeforeUpgradingOne != nil)
	defer entryBeforeUpgradingOne.mu.Unlock()

	var next = cur.next
	next.mu.Lock()
	for next != q.tail && next.status == entryStatusRunning {
		deadlockCond = deadlockCond ||
			checkDeadlockCondition(next.r.txnID, r.txnID)
		compatible = compatible && next.r.lockMode.Compatible(r.lockMode)

		cur.mu.Unlock()
		cur = next
		next = next.next
		next.mu.Lock()
	}

	assert.Assert(cur.status == entryStatusRunning)
	if next == q.tail || next.status == entryStatusWaitAcquire {
		// no upgrades pending
		if compatible {
			cur.mu.Unlock()
			next.mu.Unlock()

			upgradingEntry.mu.Lock()
			upgradingEntry.r.lockMode = r.lockMode
			upgradingEntry.mu.Unlock()

			return upgradingEntry.notifier
		}

		if deadlockCond {
			cur.mu.Unlock()
			next.mu.Unlock()
			return nil
		}

		newEntry := &txnQueueEntry[LockModeType, ObjectIDType]{
			r:        r,
			notifier: make(chan struct{}),
			status:   entryStatusWaitUpgrade,
			next:     next,
			prev:     cur,
		}
		cur.next = newEntry
		next.prev = newEntry
		cur.mu.Unlock()
		next.mu.Unlock()

		entryBeforeUpgradingOne.SafeDeleteNext()

		q.mu.Lock()
		q.txnNodes[r.txnID] = newEntry
		q.mu.Unlock()
		return newEntry.notifier
	}

	assert.Assert(next.status == entryStatusWaitUpgrade)

	cur.mu.Unlock()
	cur = next
	defer func() { cur.mu.Unlock() }()

	for {
		if !cur.r.lockMode.Compatible(r.lockMode) {
			return nil
		}
		next = cur.next
		next.mu.Lock()

		if next == q.tail || next.status == entryStatusWaitAcquire {
			newEntry := &txnQueueEntry[LockModeType, ObjectIDType]{
				r:        r,
				notifier: make(chan struct{}),
				status:   entryStatusWaitUpgrade,
				next:     next,
				prev:     cur,
			}
			cur.next = newEntry
			next.prev = newEntry
			next.mu.Unlock()

			entryBeforeUpgradingOne.SafeDeleteNext()

			q.mu.Lock()
			q.txnNodes[r.txnID] = newEntry
			q.mu.Unlock()
			return newEntry.notifier
		}
		cur.mu.Unlock()
		cur = next
	}
}

func (q *txnQueue[LockModeType, ObjectIDType]) Unlock(
	r TxnUnlockRequest[ObjectIDType],
) {
	q.mu.Lock()
	deletingNode, present := q.txnNodes[r.txnID]
	q.mu.Unlock()

	assert.Assert(present, "node not found. %+v", r)

	cur := q.head
	cur.mu.Lock()
	for cur.next != deletingNode {
		cur = cur.SafeNext()
	}

	deletingNode.mu.Lock()
	next := deletingNode.next
	next.mu.Lock()
	next.prev = cur
	cur.next = next
	deletingNodeStatus := deletingNode.status
	deletingNode.mu.Unlock()
	cur.mu.Unlock()

	if deletingNodeStatus == entryStatusRunning && cur == q.head &&
		next.status != entryStatusRunning {
		q.processBatch(next)
		return
	}
	next.mu.Unlock()
}
