package txns

import (
	"sync"

	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
)

type lockManager[LockModeType GranularLock[LockModeType], ID comparable] struct {
	qsGuard sync.Mutex
	qs      map[ID]*txnQueue[LockModeType, ID]

	lockedRecordsGuard sync.Mutex
	lockedRecords      map[common.TxnID]map[ID]struct{}
}

func NewManager[LockModeType GranularLock[LockModeType], ObjectID comparable]() *lockManager[LockModeType, ObjectID] {
	return &lockManager[LockModeType, ObjectID]{
		qsGuard:            sync.Mutex{},
		qs:                 map[ObjectID]*txnQueue[LockModeType, ObjectID]{},
		lockedRecordsGuard: sync.Mutex{},
		lockedRecords:      map[common.TxnID]map[ObjectID]struct{}{},
	}
}

// Lock attempts to acquire a lock on the record specified in the
// TxnLockRequest.
// It ensures that a transaction does not lock the same record multiple times.
// If the lock is available, it returns a channel that will be closed when the
// lock is acquired. If the lock cannot be acquired immediately, the channel
// will be closed once the lock is available. Returns nil if the lock cannot be
// acquired due to a deadlock prevention policy.
func (m *lockManager[LockModeType, ObjectID]) Lock(
	r TxnLockRequest[LockModeType, ObjectID],
) <-chan struct{} {
	q := func() *txnQueue[LockModeType, ObjectID] {
		m.qsGuard.Lock()
		defer m.qsGuard.Unlock()

		q, ok := m.qs[r.objectId]
		if !ok {
			q = newTxnQueue[LockModeType, ObjectID]()
			m.qs[r.objectId] = q
		}

		return q
	}()

	notifier := q.Lock(r)
	if notifier == nil {
		return nil
	}

	func() {
		m.lockedRecordsGuard.Lock()
		defer m.lockedRecordsGuard.Unlock()

		alreadyLockedRecords, ok := m.lockedRecords[r.txnID]
		if !ok {
			alreadyLockedRecords = make(map[ObjectID]struct{})
			m.lockedRecords[r.txnID] = alreadyLockedRecords
		}

		alreadyLockedRecords[r.objectId] = struct{}{}
	}()

	return notifier
}

// Upgrade attempts to upgrade the lock held by the transaction specified in the
// txnLockRequest `r`. It checks that the lock is currently held and that the
// transaction is eligible for an upgrade. If the upgrade cannot be performed
// immediately (due to lock contention), it returns nil and the caller should
// retry. If the upgrade can proceed, it inserts a new entry into the
// transaction queue and returns a channel that will be closed when the upgrade
// is granted. The function ensures proper synchronization and queue
// manipulation to maintain lock order and safety.
//
// Parameters:
// - r: txnLockRequest containing the transaction and record identifiers for the
// upgrade request.
//
// Returns:
// - <-chan struct{}: A channel that will be closed when the lock upgrade is
// granted, or nil if the upgrade cannot be performed immediately.
func (m *lockManager[LockModeType, ObjectID]) Upgrade(
	r TxnLockRequest[LockModeType, ObjectID],
) <-chan struct{} {
	q := func() *txnQueue[LockModeType, ObjectID] {
		m.qsGuard.Lock()
		defer m.qsGuard.Unlock()

		q, present := m.qs[r.objectId]
		assert.Assert(present,
			"trying to upgrade a lock on the unlocked tuple. request: %+v",
			r)

		return q
	}()

	n := q.Upgrade(r)
	return n
}

// Unlock releases the lock held by a transaction on a specific record.
// It first retrieves the transaction queue associated with the record ID,
// ensuring that the record is currently locked. It then attempts to unlock
// the record, retrying if necessary until successful. After unlocking,
// it removes the record from the set of records locked by the transaction.
// Panics if the record is not currently locked or if the transaction does not
// have any locked records.
func (m *lockManager[LockModeType, ObjectID]) Unlock(
	r TxnUnlockRequest[ObjectID],
) {
	q := func() *txnQueue[LockModeType, ObjectID] {
		m.qsGuard.Lock()
		defer m.qsGuard.Unlock()

		q, present := m.qs[r.objectId]
		assert.Assert(present,
			"trying to unlock already unlocked tuple. recordID: %+v",
			r.objectId)

		return q
	}()

	q.Unlock(r)

	func() {
		m.lockedRecordsGuard.Lock()
		defer m.lockedRecordsGuard.Unlock()

		lockedRecords, lockedRecordsExist := m.lockedRecords[r.txnID]
		assert.Assert(lockedRecordsExist,
			"expected a set of locked records for the transaction %+v to exist",
			r.txnID,
		)
		delete(lockedRecords, r.objectId)
	}()
}

func (m *lockManager[LockModeType, ObjectID]) UnlockAll(
	TransactionID common.TxnID,
) {
	lockedRecords := func() map[ObjectID]struct{} {
		m.lockedRecordsGuard.Lock()
		defer m.lockedRecordsGuard.Unlock()

		lockedRecords, ok := m.lockedRecords[TransactionID]
		if !ok {
			return make(map[ObjectID]struct{})
		}

		assert.Assert(ok,
			"expected a set of locked records for the transaction %+v to exist",
			TransactionID)
		delete(m.lockedRecords, TransactionID)
		return lockedRecords
	}()

	unlockRequest := TxnUnlockRequest[ObjectID]{
		txnID: TransactionID,
	}

	for r := range lockedRecords {
		q := func() *txnQueue[LockModeType, ObjectID] {
			m.qsGuard.Lock()
			defer m.qsGuard.Unlock()

			q, present := m.qs[r]
			assert.Assert(
				present,
				"trying to unlock a transaction on an unlocked tuple. recordID: %+v",
				r,
			)

			return q
		}()

		unlockRequest.objectId = r
		q.Unlock(unlockRequest)
	}
}

func (m *lockManager[LockModeType, ObjectID]) GetActiveTransactions() map[common.TxnID]struct{} {
	m.lockedRecordsGuard.Lock()
	defer m.lockedRecordsGuard.Unlock()

	activeTxns := make(map[common.TxnID]struct{})
	for txnID := range m.lockedRecords {
		activeTxns[txnID] = struct{}{}
	}
	return activeTxns
}
