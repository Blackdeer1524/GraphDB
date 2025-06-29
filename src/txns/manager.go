package txns

import (
	"runtime"
	"sync"

	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
)

type Manager[LockModeType LockMode[LockModeType], ObjectIDType comparable] struct {
	qsGuard sync.Mutex
	qs      map[ObjectIDType]*txnQueue[LockModeType, ObjectIDType]

	lockedRecordsGuard sync.Mutex
	lockedRecords      map[TxnID]map[ObjectIDType]struct{}
}

func NewManager[LockModeType LockMode[LockModeType], ObjectIDType comparable]() *Manager[LockModeType, ObjectIDType] {
	return &Manager[LockModeType, ObjectIDType]{
		qsGuard:            sync.Mutex{},
		qs:                 map[ObjectIDType]*txnQueue[LockModeType, ObjectIDType]{},
		lockedRecordsGuard: sync.Mutex{},
		lockedRecords:      map[TxnID]map[ObjectIDType]struct{}{},
	}
}

// Lock attempts to acquire a lock on the record specified in the
// TxnLockRequest.
// It ensures that a transaction does not lock the same record multiple times.
// If the lock is available, it returns a channel that will be closed when the
// lock is acquired. If the lock cannot be acquired immediately, the channel
// will be closed once the lock is available. Returns nil if the lock cannot be
// acquired due to a deadlock prevention policy.
func (m *Manager[LockModeType, ObjectIDType]) Lock(
	r TxnLockRequest[LockModeType, ObjectIDType],
) <-chan struct{} {
	q := func() *txnQueue[LockModeType, ObjectIDType] {
		m.qsGuard.Lock()
		defer m.qsGuard.Unlock()

		q, ok := m.qs[r.recordId]
		if !ok {
			q = newTxnQueue[LockModeType, ObjectIDType]()
			m.qs[r.recordId] = q
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
			alreadyLockedRecords = make(map[ObjectIDType]struct{})
			m.lockedRecords[r.txnID] = alreadyLockedRecords
		}

		_, isAleadyLocked := alreadyLockedRecords[r.recordId]
		assert.Assert(!isAleadyLocked,
			"Didn't expect the record %+v to be locked by a transaction %+v",
			r.recordId,
			r.txnID)

		alreadyLockedRecords[r.recordId] = struct{}{}
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
func (m *Manager[LockModeType, ObjectIDType]) Upgrade(
	r TxnLockRequest[LockModeType, ObjectIDType],
) <-chan struct{} {
	q := func() *txnQueue[LockModeType, ObjectIDType] {
		m.qsGuard.Lock()
		defer m.qsGuard.Unlock()

		q, present := m.qs[r.recordId]
		assert.Assert(present,
			"trying to upgrade a lock on the unlocked tuple. request: %+v",
			r)

		return q
	}()

	return q.Upgrade(r)
}

// Unlock releases the lock held by a transaction on a specific record.
// It first retrieves the transaction queue associated with the record ID,
// ensuring that the record is currently locked. It then attempts to unlock
// the record, retrying if necessary until successful. After unlocking,
// it removes the record from the set of records locked by the transaction.
// Panics if the record is not currently locked or if the transaction does not
// have any locked records.
func (m *Manager[LockModeType, ObjectIDType]) Unlock(
	r TxnUnlockRequest[ObjectIDType],
) {
	q := func() *txnQueue[LockModeType, ObjectIDType] {
		m.qsGuard.Lock()
		defer m.qsGuard.Unlock()

		q, present := m.qs[r.recordId]
		assert.Assert(present,
			"trying to unlock the already unlocked tuple. recordID: %+v",
			r.recordId)

		return q
	}()

	for !q.Unlock(r) {
		// TODO: rething the retries
		runtime.Gosched()
	}

	func() {
		m.lockedRecordsGuard.Lock()
		defer m.lockedRecordsGuard.Unlock()

		lockedRecords, lockedRecordsExist := m.lockedRecords[r.txnID]
		assert.Assert(lockedRecordsExist,
			"expected a set of locked records for the transaction %+v to exist",
			r.txnID,
		)
		delete(lockedRecords, r.recordId)
	}()
}

func (m *Manager[LockModeType, ObjectIDType]) UnlockAll(TransactionID TxnID) {
	lockedRecords := func() map[ObjectIDType]struct{} {
		m.lockedRecordsGuard.Lock()
		defer m.lockedRecordsGuard.Unlock()

		lockedRecords, ok := m.lockedRecords[TransactionID]
		assert.Assert(ok,
			"expected a set of locked records for the transaction %+v to exist",
			TransactionID)
		delete(m.lockedRecords, TransactionID)

		return lockedRecords
	}()

	unlockRequest := TxnUnlockRequest[ObjectIDType]{
		txnID: TransactionID,
	}

	for r := range lockedRecords {
		q := func() *txnQueue[LockModeType, ObjectIDType] {
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

		unlockRequest.recordId = r
		for !q.Unlock(unlockRequest) {
			runtime.Gosched()
		}
	}
}
