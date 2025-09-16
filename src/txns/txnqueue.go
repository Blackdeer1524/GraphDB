package txns

import (
	"math"
	"sync"

	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
)

type entryStatus byte

const (
	entryStatusAcquired entryStatus = iota
	entryStatusWaitUpgrade
	entryStatusWaitAcquire
)

func (s entryStatus) String() string {
	switch s {
	case entryStatusAcquired:
		return "acquired"
	case entryStatusWaitUpgrade:
		return "wait-upgrade"
	case entryStatusWaitAcquire:
		return "wait-acquire"
	}
	panic("invalid entry status")
}

type txnQueueEntry[LockModeType GranularLock[LockModeType], ObjectIDType comparable] struct {
	r        TxnLockRequest[LockModeType, ObjectIDType]
	notifier chan struct{}
	status   entryStatus

	mu   sync.Mutex
	next *txnQueueEntry[LockModeType, ObjectIDType]
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
	deletingEntry := lockedEntry.next
	deletingEntry.mu.Lock()

	after := deletingEntry.next
	deletingEntry.mu.Unlock()

	lockedEntry.next = after
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
	n.next = next
	lockedEntry.next = n
}

// Is thread-safe across multiple transactions
// Note: you should NOT create multiple goroutines within a single transactions
type txnQueue[LockModeType GranularLock[LockModeType], ObjectIDType comparable] struct {
	head *txnQueueEntry[LockModeType, ObjectIDType]
	tail *txnQueueEntry[LockModeType, ObjectIDType]

	txnNodes sync.Map // map[common.TxnID]*txnQueueEntry[LockModeType, ObjectIDType]
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
		muGuardedHead.status != entryStatusAcquired,
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

		cur.status = entryStatusAcquired
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

	q := &txnQueue[LockModeType, ObjectIDType]{
		head: head,
		tail: tail,

		txnNodes: sync.Map{},
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
func (q *txnQueue[LockModeType, ObjectIDType]) Lock(
	r TxnLockRequest[LockModeType, ObjectIDType],
) <-chan struct{} {
	// log.Printf(
	// 	"[%v:%v:%v] Starting lock request",
	// 	r.txnID,
	// 	r.objectId,
	// 	r.lockMode,
	// )

	if upgradingEntry, ok := q.txnNodes.Load(r.txnID); ok {
		upgradingEntry := upgradingEntry.(*txnQueueEntry[LockModeType, ObjectIDType])
		upgradingEntry.mu.Lock()
		assert.Assert(
			upgradingEntry.status == entryStatusAcquired,
			"can only upgrade acquired transactions",
		)
		upgradingEntry.mu.Unlock()
		// log.Printf(
		// 	"[%v:%v:%v] Found existing entry, performing upgrade",
		// 	r.txnID,
		// 	r.objectId,
		// 	r.lockMode,
		// )
		return q.Upgrade(r)
	}

	cur := q.head
	cur.mu.Lock()
	defer func() { cur.mu.Unlock() }()

	if cur.next == q.tail {
		// log.Printf(
		// 	"[%v:%v:%v] Queue empty, granting lock immediately",
		// 	r.txnID,
		// 	r.objectId,
		// 	r.lockMode,
		// )
		notifier := make(chan struct{})
		close(notifier)
		newNode := &txnQueueEntry[LockModeType, ObjectIDType]{
			r:        r,
			notifier: notifier,
			status:   entryStatusAcquired,
		}
		cur.SafeInsert(newNode)

		q.txnNodes.Store(r.txnID, newNode)
		return notifier
	}

	cur = cur.SafeNext()
	locksAreCompatible := true
	deadlock := false
	// log.Printf(
	// 	"[%v:%v:%v] Checking compatibility with acquired locks",
	// 	r.txnID,
	// 	r.objectId,
	// 	r.lockMode,
	// )

	for cur.status == entryStatusAcquired {
		assert.Assert(
			cur.r.txnID != r.txnID,
			"impossible, because it should have been upgraded earlier. request: %+v",
			r,
		)

		deadlock = deadlock || checkDeadlockCondition(cur.r.txnID, r.txnID)
		locksAreCompatible = locksAreCompatible && r.lockMode.Compatible(cur.r.lockMode)
		if !locksAreCompatible {
			// log.Printf(
			// 	"[%v:%v:%v] Lock incompatible with existing lock mode %v from txn %v",
			// 	r.txnID,
			// 	r.objectId,
			// 	r.lockMode,
			// 	cur.r.lockMode,
			// 	cur.r.txnID,
			// )
			break
		}

		if cur.next == q.tail {
			// log.Printf(
			// 	"[%v:%v:%v] Compatible with all acquired locks & queue is empty, granting lock",
			// 	r.txnID,
			// 	r.objectId,
			// 	r.lockMode,
			// )
			notifier := make(chan struct{})
			close(notifier) // Grant the lock immediately
			newNode := &txnQueueEntry[LockModeType, ObjectIDType]{
				r:        r,
				notifier: notifier,
				status:   entryStatusAcquired,
			}
			cur.SafeInsert(newNode)

			q.txnNodes.Store(r.txnID, newNode)
			return notifier
		}
		cur = cur.SafeNext()
	}

	deadlock = deadlock || checkDeadlockCondition(cur.r.txnID, r.txnID)
	if deadlock {
		// log.Printf(
		// 	"[%v:%v:%v] Deadlock condition detected, aborting lock request",
		// 	r.txnID,
		// 	r.objectId,
		// 	r.lockMode,
		// )
		return nil
	}

	// log.Printf(
	// 	"[%v:%v:%v] Checking for deadlocks with waiting transactions",
	// 	r.txnID,
	// 	r.objectId,
	// 	r.lockMode,
	// )
	for cur.next != q.tail {
		cur = cur.SafeNext()
		assert.Assert(
			cur.r.txnID != r.txnID,
			"trying to lock already locked transaction. %+v",
			r,
		)

		if checkDeadlockCondition(cur.r.txnID, r.txnID) {
			// log.Printf(
			// 	"[%v:%v:%v] Deadlock condition detected with txn %v, aborting lock request",
			// 	r.txnID,
			// 	r.objectId,
			// 	r.lockMode,
			// 	cur.r.txnID,
			// )
			return nil
		}
	}

	// log.Printf(
	// 	"[%v:%v:%v] Enqueueing lock request for waiting",
	// 	r.txnID,
	// 	r.objectId,
	// 	r.lockMode,
	// )
	notifier := make(chan struct{})
	newNode := &txnQueueEntry[LockModeType, ObjectIDType]{
		r:        r,
		notifier: notifier,
		status:   entryStatusWaitAcquire,
	}
	cur.SafeInsert(newNode)
	q.txnNodes.Store(r.txnID, newNode)
	return notifier
}

// lock acquisition.

func (q *txnQueue[LockModeType, ObjectIDType]) Upgrade(
	r TxnLockRequest[LockModeType, ObjectIDType],
) <-chan struct{} {
	// log.Printf(
	// 	"[%v:%v:%v] Starting lock upgrade request",
	// 	r.txnID,
	// 	r.objectId,
	// 	r.lockMode,
	// )

	upgradingEntryAny, ok := q.txnNodes.Load(r.txnID)
	assert.Assert(ok, "can't find upgrading entry")
	upgradingEntry := upgradingEntryAny.(*txnQueueEntry[LockModeType, ObjectIDType])

	upgradingEntry.mu.Lock()
	assert.Assert(
		upgradingEntry.status == entryStatusAcquired,
		"can only upgrade acquired transactions",
	)
	acquiredLockMode := upgradingEntry.r.lockMode

	if r.lockMode.WeakerOrEqual(acquiredLockMode) {
		// log.Printf(
		// 	"[%v:%v:%v] Current lock mode is sufficient for requested mode, returning existing
		// notifier",
		// 	r.txnID,
		// 	r.objectId,
		// 	r.lockMode,
		// )
		upgradingEntry.mu.Unlock()
		return upgradingEntry.notifier
	}

	r.lockMode = r.lockMode.Combine(acquiredLockMode)
	// log.Printf(
	// 	"[%v:%v:%v] Combined lock mode calculated",
	// 	r.txnID,
	// 	r.objectId,
	// 	r.lockMode,
	// )
	upgradingEntry.mu.Unlock()

	deadlock := false
	compatible := true

	var entryBeforeUpgradingOne *txnQueueEntry[LockModeType, ObjectIDType] = nil
	cur := q.head
	cur.mu.Lock()
	for {
		assert.Assert(
			cur.next != q.tail,
			"should have found the upgrading entry",
		)
		entryBeforeUpgradingOne = cur
		cur = cur.next
		cur.mu.Lock()
		if cur.r.txnID == r.txnID {
			// log.Printf(
			// 	"[%v:%v:%v] Found upgrading entry in queue",
			// 	r.txnID,
			// 	r.objectId,
			// 	r.lockMode,
			// )
			break
		}
		entryBeforeUpgradingOne.mu.Unlock()

		deadlock = deadlock || checkDeadlockCondition(cur.r.txnID, r.txnID)
		compatible = compatible && cur.r.lockMode.Compatible(r.lockMode)
	}
	assert.Assert(entryBeforeUpgradingOne != nil)
	defer entryBeforeUpgradingOne.mu.Unlock()

	var next = cur.next
	next.mu.Lock()
	for next != q.tail && next.status == entryStatusAcquired {
		deadlock = deadlock || checkDeadlockCondition(next.r.txnID, r.txnID)
		compatible = compatible && next.r.lockMode.Compatible(r.lockMode)

		cur.mu.Unlock()
		cur = next
		next = next.next
		next.mu.Lock()
	}

	assert.Assert(cur.status == entryStatusAcquired)
	if next == q.tail || next.status == entryStatusWaitAcquire {
		// no upgrades pending
		if compatible {
			// log.Printf(
			// 	"[%v:%v:%v] Lock upgrade is compatible, granting immediately",
			// 	r.txnID,
			// 	r.objectId,
			// 	r.lockMode,
			// )
			cur.mu.Unlock()
			next.mu.Unlock()

			upgradingEntry.mu.Lock()
			upgradingEntry.r.lockMode = r.lockMode
			upgradingEntry.mu.Unlock()

			return upgradingEntry.notifier
		}

		if deadlock {
			// 	log.Printf(
			// 		"[%v:%v:%v] Deadlock detected, aborting upgrade",
			// 		r.txnID,
			// 		r.objectId,
			// 		r.lockMode,
			// 	)
			cur.mu.Unlock()
			next.mu.Unlock()
			return nil
		}

		// log.Printf(
		// 	"[%v:%v:%v] Creating new wait upgrade entry",
		// 	r.txnID,
		// 	r.objectId,
		// 	r.lockMode,
		// )
		newEntry := &txnQueueEntry[LockModeType, ObjectIDType]{
			r:        r,
			notifier: make(chan struct{}),
			status:   entryStatusWaitUpgrade,
			next:     next,
		}
		cur.next = newEntry
		cur.mu.Unlock()
		next.mu.Unlock()

		entryBeforeUpgradingOne.SafeDeleteNext()

		q.txnNodes.Store(r.txnID, newEntry)
		return newEntry.notifier
	}

	assert.Assert(next.status == entryStatusWaitUpgrade)
	if deadlock {
		// log.Printf(
		// 	"[%v:%v:%v] Deadlock detected with existing upgrade, aborting upgrade",
		// 	r.txnID,
		// 	r.objectId,
		// 	r.lockMode,
		// )
		cur.mu.Unlock()
		next.mu.Unlock()
		return nil
	}

	// log.Printf(
	// 	"[%v:%v:%v] Checking compatibility with existing upgrade entries",
	// 	r.txnID,
	// 	r.objectId,
	// 	r.lockMode,
	// )
	cur.mu.Unlock()
	cur = next
	defer func() { cur.mu.Unlock() }()

	for {
		if !cur.r.lockMode.Compatible(r.lockMode) {
			// log.Printf(
			// 	"[%v:%v:%v] Incompatible with existing upgrade entry, aborting upgrade",
			// 	r.txnID,
			// 	r.objectId,
			// 	r.lockMode,
			// )
			return nil
		}
		if checkDeadlockCondition(cur.r.txnID, r.txnID) {
			// log.Printf(
			// 	"[%v:%v:%v] Deadlock condition with upgrade entry, aborting upgrade",
			// 	r.txnID,
			// 	r.objectId,
			// 	r.lockMode,
			// )
			return nil
		}
		next = cur.next
		next.mu.Lock()

		if next == q.tail || next.status == entryStatusWaitAcquire {
			// log.Printf(
			// 	"[%v:%v:%v] Inserting upgrade entry at end of upgrade queue after existing upgrade
			// entry %v",
			// 	r.txnID,
			// 	r.objectId,
			// 	r.lockMode,
			// 	cur.r.txnID,
			// )
			newEntry := &txnQueueEntry[LockModeType, ObjectIDType]{
				r:        r,
				notifier: make(chan struct{}),
				status:   entryStatusWaitUpgrade,
				next:     next,
			}
			cur.next = newEntry
			next.mu.Unlock()

			entryBeforeUpgradingOne.SafeDeleteNext()

			q.txnNodes.Store(r.txnID, newEntry)
			return newEntry.notifier
		}
		cur.mu.Unlock()
		cur = next
	}
}

func (q *txnQueue[LockModeType, ObjectIDType]) unlock(
	r TxnUnlockRequest[ObjectIDType],
) {
	deletingNodeAny, present := q.txnNodes.Load(r.txnID)
	assert.Assert(present, "node not found. %+v", r)
	deletingNode := deletingNodeAny.(*txnQueueEntry[LockModeType, ObjectIDType])

	cur := q.head
	cur.mu.Lock()
	for cur.next != deletingNode {
		cur = cur.SafeNext()
	}

	deletingNode.mu.Lock()
	deletingNodeStatus := deletingNode.status
	after := deletingNode.next
	cur.next = after
	deletingNode.mu.Unlock()
	cur.mu.Unlock()

	q.txnNodes.Delete(r.txnID)

	after.mu.Lock()
	if cur == q.head && deletingNodeStatus == entryStatusAcquired &&
		after.status != entryStatusAcquired {
		q.processBatch(after)
		return
	}
	after.mu.Unlock()
}

func (q *txnQueue[LockModeType, ObjectIDType]) IsEmpty() bool {
	q.head.mu.Lock()
	defer q.head.mu.Unlock()
	return q.head.next == q.tail
}
