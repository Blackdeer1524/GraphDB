package txns

import (
	"fmt"
	"strings"
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

type txnDependencyGraph[LockModeType GranularLock[LockModeType]] map[common.TxnID][]edgeInfo[LockModeType]

type edgeInfo[LockModeType GranularLock[LockModeType]] struct {
	dst      common.TxnID
	lockMode LockModeType
}

func (g txnDependencyGraph[LockModeType]) IsCyclic() bool {
	visited := make(map[common.TxnID]bool)
	recStack := make(map[common.TxnID]bool)

	var dfs func(txnID common.TxnID) bool
	dfs = func(txnID common.TxnID) bool {
		if recStack[txnID] {
			return true
		}

		if visited[txnID] {
			return false
		}

		visited[txnID] = true
		recStack[txnID] = true

		for _, edge := range g[txnID] {
			if dfs(edge.dst) {
				return true
			}
		}

		recStack[txnID] = false
		return false
	}

	for txnID := range g {
		if !visited[txnID] {
			if dfs(txnID) {
				return true
			}
		}
	}

	return false
}

func (g txnDependencyGraph[LockModeType]) Dump() string {
	var result strings.Builder

	result.WriteString("digraph TransactionDependencyGraph {\n")
	result.WriteString("\trankdir=LR;\n")
	result.WriteString("\tnode [shape=box];\n")

	for txnID := range g {
		result.WriteString(
			fmt.Sprintf("\t\"txn_%d\" [label=\"Txn %d\"];\n", txnID, txnID),
		)
	}
	result.WriteString("\n")

	colorset := map[string]struct{}{
		"red":     {},
		"blue":    {},
		"green":   {},
		"yellow":  {},
		"purple":  {},
		"orange":  {},
		"brown":   {},
		"pink":    {},
		"cyan":    {},
		"magenta": {},
		"lime":    {},
		"teal":    {},
	}

	lock2color := map[string]string{}
	for txnID, deps := range g {
		for _, edge := range deps {
			lockModeStr := fmt.Sprintf("%v", edge.lockMode)
			var edgeColor string
			if edgeColor, ok := lock2color[lockModeStr]; !ok {
				assert.Assert(
					len(colorset) > 0,
					"expected a color set to exist",
				)
				for edgeColor = range colorset {
					break
				}
				lock2color[lockModeStr] = edgeColor
			}

			result.WriteString(
				fmt.Sprintf(
					"\t\"txn_%d\" -> \"txn_%d\" [label=\"%s\", color=\"%s\"];\n",
					txnID,
					edge.dst,
					lockModeStr,
					edgeColor,
				),
			)
		}
	}

	result.WriteString("}\n")
	return result.String()
}

func (m *lockManager[LockModeType, ID]) GetGraphSnaphot() txnDependencyGraph[LockModeType] {
	m.qsGuard.Lock()
	defer m.qsGuard.Unlock()

	for range m.lockedRecords {
		for _, q := range m.qs {
			q.mu.Lock()
			defer q.mu.Unlock() // да, нужно анлокнуть по выходу из функции
		}
	}

	graph := map[common.TxnID][]edgeInfo[LockModeType]{}
	for txnID := range m.lockedRecords {
		for _, q := range m.qs {
			cur := q.head
			cur.mu.Lock()
			runningSet := map[common.TxnID]struct{}{}
			for cur = cur.SafeNext(); cur != q.tail && cur.status == entryStatusRunning; cur = cur.SafeNext() {
				runningSet[cur.r.txnID] = struct{}{}
			}

			for runner := range runningSet {
				graph[runner] = []edgeInfo[LockModeType]{}
			}
			if cur == q.tail {
				cur.mu.Unlock()
				continue
			}

			assert.Assert(
				len(runningSet) != 0,
				"expected a running set to exist for the transaction %+v",
				txnID,
			)

			for runnerID := range runningSet {
				graph[cur.r.txnID] = append(
					graph[cur.r.txnID],
					edgeInfo[LockModeType]{
						dst:      runnerID,
						lockMode: cur.r.lockMode,
					},
				)
			}

			prev := cur
			cur = cur.next
			cur.mu.Lock()
			for cur != q.tail {
				assert.Assert(
					cur.status != entryStatusRunning,
					"only queue prefix can be running",
				)
				graph[cur.r.txnID] = append(
					graph[cur.r.txnID],
					edgeInfo[LockModeType]{
						dst:      prev.r.txnID,
						lockMode: cur.r.lockMode,
					},
				)

				cur = cur.SafeNext()
				prev = prev.SafeNext()
			}
			prev.mu.Unlock()
			cur.mu.Unlock()
		}
	}

	return graph
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

	q.unlock(r)

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
		q.unlock(unlockRequest)
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

func (m *lockManager[LockModeType, ObjectID]) AreAllQueuesEmpty() bool {
	m.qsGuard.Lock()
	defer m.qsGuard.Unlock()

	for _, q := range m.qs {
		if !q.IsEmpty() {
			return false
		}
	}

	return true
}
