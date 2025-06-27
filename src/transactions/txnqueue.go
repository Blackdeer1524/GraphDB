package transactions

import (
	"math"
	"sync"
)

type txnQueueEntry struct {
	r        txnLockRequest
	notifier chan struct{}
	isLocked bool

	mu   sync.Mutex
	next *txnQueueEntry
	prev *txnQueueEntry
}

func (lockedEntry *txnQueueEntry) SafeNext() *txnQueueEntry {
	next := lockedEntry.next
	next.mu.Lock()
	lockedEntry.mu.Unlock()

	return next
}

func (lockedEntry *txnQueueEntry) SafeInsert(n *txnQueueEntry) {
	next := lockedEntry.next

	n.prev = lockedEntry
	n.next = next

	lockedEntry.next = n

	next.mu.Lock()
	next.prev = n
	next.mu.Unlock()
}

type txnQueue struct {
	head *txnQueueEntry
	tail *txnQueueEntry

	mu       sync.Mutex
	txnNodes map[TxnID]*txnQueueEntry
}

func (q *txnQueue) processBatch(lockedHead *txnQueueEntry) {
	seenLockModes := make(map[LockMode]struct{})

	cur := lockedHead.SafeNext()
	defer func() {
		cur.mu.Unlock()
	}()

	if cur == q.tail {
		return
	}

	Assert(!cur.isLocked, "processBatch contract is violated")

outer:
	for {
		for seenMode := range seenLockModes {
			if !compatibleLockModes(seenMode, cur.r.lockMode) {
				break outer
			}
		}
		seenLockModes[cur.r.lockMode] = struct{}{}

		cur.isLocked = true
		close(cur.notifier) // grants the lock to a transaction

		if cur.next == q.tail {
			break
		}

		cur = cur.SafeNext()
		Assert(!cur.isLocked, "only list prefix is allowed to be in the locked state")
	}
}

func newTxnQueue() *txnQueue {
	head := &txnQueueEntry{
		r: txnLockRequest{
			TransactionID: math.MaxUint64, // Needed for the deadlock prevention policy
			lockMode:      helper_ALLOW_ALL,
		},
	}
	tail := &txnQueueEntry{
		r: txnLockRequest{
			TransactionID: 0, // Needed for the deadlock prevention policy
			lockMode:      helper_FORBID_ALL,
		},
	}
	head.next = tail
	tail.prev = head

	q := &txnQueue{
		head: head,
		tail: tail,

		mu:       sync.Mutex{},
		txnNodes: map[TxnID]*txnQueueEntry{},
	}

	return q
}

func (q *txnQueue) Lock(r txnLockRequest) <-chan struct{} {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.head.mu.Lock()

	cur := q.head
	defer func(c **txnQueueEntry) { (*c).mu.Unlock() }(&cur)

	locksAreCompatible := true

	for {
		Assert(
			cur.r.TransactionID != r.TransactionID,
			"trying to lock already locked transaction. %+v",
			r,
		)

		locksAreCompatible = locksAreCompatible &&
			compatibleLockModes(r.lockMode, cur.r.lockMode)
		if !locksAreCompatible && cur.r.TransactionID < r.TransactionID {
			// Deadlock prevention policy
			// Only an older transaction can wait for a younger one.
			// Ohterwise, a younger transaction is aborted
			return nil
		}

		if cur.next == q.tail {
			break
		}

		cur = cur.SafeNext()
	}

	notifier := make(chan struct{})
	if locksAreCompatible {
		close(notifier) // Grant the lock immediately
		newNode := &txnQueueEntry{
			r:        r,
			notifier: nil,
			isLocked: true,
		}
		cur.SafeInsert(newNode)

		q.txnNodes[r.TransactionID] = newNode

		return notifier
	}

	newNode := &txnQueueEntry{
		r:        r,
		notifier: notifier,
		isLocked: false,
	}
	cur.SafeInsert(newNode)

	q.txnNodes[r.TransactionID] = newNode

	return notifier
}

func (q *txnQueue) Unlock(r txnUnlockRequest) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	deletingNode, present := q.txnNodes[r.TransactionID]
	Assert(present, "node not found. %+v", r)

	deletingNode.mu.Lock()
	defer deletingNode.mu.Unlock()

	prev := deletingNode.prev
	// TODO: rework this into something NOT using retries
	// Potential solution: tombstone marker.
	// deleted nodes then would be cleaned up during the
	// next insert operation
	if !prev.mu.TryLock() {
		return false
	}

	delete(q.txnNodes, r.TransactionID)

	next := deletingNode.next
	next.mu.Lock()
	next.prev = prev
	next.mu.Unlock()

	prev.next = next
	if deletingNode.isLocked && prev == q.head && !next.isLocked {
		q.processBatch(prev)
		return true
	}
	prev.mu.Unlock()

	return true
}
