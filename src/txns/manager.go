package txns

import (
	"fmt"
	"strings"
	"sync"

	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
)

type lockManager[LockModeType GranularLock[LockModeType], ID comparable] struct {
	qs sync.Map // map[ID]*txnQueue[LockModeType, ID]

	lockedRecordsGuard sync.Mutex
	lockedRecords      map[common.TxnID]map[ID]struct{}
}

type txnDependencyGraph[LockModeType GranularLock[LockModeType], ID comparable] map[common.TxnID][]edgeInfo[LockModeType, ID]

type edgeInfo[LockModeType GranularLock[LockModeType], ID comparable] struct {
	txnDst   common.TxnID
	status   entryStatus
	isPage   bool
	lockMode LockModeType
	pageDst  ID
}

func newEdgeInfo[LockModeType GranularLock[LockModeType], ID comparable](
	txnDst common.TxnID,
	status entryStatus,
	isPage bool,
	lockMode LockModeType,
	pageDst ID,
) edgeInfo[LockModeType, ID] {
	return edgeInfo[LockModeType, ID]{
		txnDst:   txnDst,
		status:   status,
		isPage:   isPage,
		lockMode: lockMode,
		pageDst:  pageDst,
	}
}

func (g txnDependencyGraph[LockModeType, ID]) IsCyclic() bool {
	visited := make(map[common.TxnID]struct{})
	recStack := make(map[common.TxnID]struct{})

	var dfs func(txnID common.TxnID) bool
	dfs = func(txnID common.TxnID) bool {
		if _, ok := recStack[txnID]; ok {
			return true
		}

		if _, ok := visited[txnID]; ok {
			return false
		}

		visited[txnID] = struct{}{}
		recStack[txnID] = struct{}{}

		for _, edge := range g[txnID] {
			if !edge.isPage && dfs(edge.txnDst) {
				return true
			}
		}

		delete(recStack, txnID)
		return false
	}

	for txnID := range g {
		if _, ok := visited[txnID]; !ok {
			if dfs(txnID) {
				return true
			}
		}
	}

	return false
}

func (g txnDependencyGraph[LockModeType, ID]) Dump() string {
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
			lockModeStr := edge.lockMode.String()
			var edgeColor string
			var ok bool
			if edgeColor, ok = lock2color[lockModeStr]; !ok {
				assert.Assert(
					len(colorset) > 0,
					"expected a color set to exist",
				)
				for edgeColor = range colorset {
					break
				}
				delete(colorset, edgeColor)
				lock2color[lockModeStr] = edgeColor
			}

			if !edge.isPage {
				result.WriteString(
					fmt.Sprintf(
						"\t\"txn_%d\" -> \"txn_%d\" [label=\"%+v [%s:%s]\", color=\"%s\"];\n",
						txnID,
						edge.txnDst,
						edge.pageDst,
						lockModeStr,
						edge.status,
						edgeColor,
					),
				)
			} else {
				result.WriteString(
					fmt.Sprintf(
						"\t\"txn_%d\" -> \"object_%+v\" [label=\"[%s:%s]\", color=\"%s\"];\n",
						txnID,
						edge.pageDst,
						lockModeStr,
						edge.status,
						edgeColor,
					),
				)
			}
		}
	}

	result.WriteString("}\n")
	return result.String()
}

func (m *lockManager[LockModeType, ID]) GetGraphSnaphot() txnDependencyGraph[LockModeType, ID] {
	qs := map[ID]*txnQueue[LockModeType, ID]{}
	m.qs.Range(func(key, value any) bool {
		qs[key.(ID)] = value.(*txnQueue[LockModeType, ID])
		qs[key.(ID)].head.mu.Lock()
		return true
	})

	defer func() {
		for _, q := range qs {
			q.head.mu.Unlock()
		}
	}()

	graph := map[common.TxnID][]edgeInfo[LockModeType, ID]{}
	for _, q := range qs {
		cur := q.head.next
		cur.mu.Lock()
		runningSet := map[common.TxnID]struct{}{}
		for ; cur != q.tail && cur.status == entryStatusRunning; cur = cur.SafeNext() {
			runningSet[cur.r.txnID] = struct{}{}
			graph[cur.r.txnID] = append(
				graph[cur.r.txnID],
				newEdgeInfo(
					0,
					cur.status,
					true,
					cur.r.lockMode,
					cur.r.objectId,
				),
			)
		}

		if cur == q.tail {
			cur.mu.Unlock()
			continue
		}

		assert.Assert(
			len(runningSet) != 0,
			"expected a running set to exist for the transaction %+v",
			cur.r.txnID,
		)
		assert.Assert(
			cur.status != entryStatusRunning,
			"only queue prefix can be running. txnID: %d, status: %s",
			cur.r.txnID,
			cur.status,
		)

		for runnerID := range runningSet {
			graph[cur.r.txnID] = append(
				graph[cur.r.txnID],
				newEdgeInfo(
					runnerID,
					cur.status,
					false,
					cur.r.lockMode,
					cur.r.objectId,
				),
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
				newEdgeInfo(
					prev.r.txnID,
					cur.status,
					false,
					cur.r.lockMode,
					cur.r.objectId,
				),
			)

			cur = cur.SafeNext()
			prev = prev.SafeNext()
		}
		prev.mu.Unlock()
		cur.mu.Unlock()
	}

	return graph
}

func NewManager[LockModeType GranularLock[LockModeType], ObjectID comparable]() *lockManager[LockModeType, ObjectID] {
	return &lockManager[LockModeType, ObjectID]{
		qs:                 sync.Map{},
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
	qAny, _ := m.qs.LoadOrStore(
		r.objectId,
		newTxnQueue[LockModeType, ObjectID](),
	)
	q := qAny.(*txnQueue[LockModeType, ObjectID])

	func() {
		m.lockedRecordsGuard.Lock()
		defer m.lockedRecordsGuard.Unlock()

		_, ok := m.lockedRecords[r.txnID]
		if !ok {
			m.lockedRecords[r.txnID] = make(map[ObjectID]struct{})
		}
	}()

	notifier := q.Lock(r)
	if notifier == nil {
		return nil
	}

	func() {
		m.lockedRecordsGuard.Lock()
		defer m.lockedRecordsGuard.Unlock()
		m.lockedRecords[r.txnID][r.objectId] = struct{}{}
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
		qAny, present := m.qs.Load(r.objectId)
		assert.Assert(present,
			"trying to upgrade a lock on the unlocked tuple. request: %+v",
			r)

		q := qAny.(*txnQueue[LockModeType, ObjectID])
		return q
	}()

	n := q.Upgrade(r)
	return n
}

// unlock releases the lock held by a transaction on a specific record.
// It first retrieves the transaction queue associated with the record ID,
// ensuring that the record is currently locked. It then attempts to unlock
// the record, retrying if necessary until successful. After unlocking,
// it removes the record from the set of records locked by the transaction.
// Panics if the record is not currently locked or if the transaction does not
// have any locked records.
func (m *lockManager[LockModeType, ObjectID]) unlock(
	r TxnUnlockRequest[ObjectID],
) {
	q := func() *txnQueue[LockModeType, ObjectID] {
		qAny, present := m.qs.Load(r.objectId)
		assert.Assert(present,
			"trying to unlock already unlocked tuple. recordID: %+v",
			r)

		q := qAny.(*txnQueue[LockModeType, ObjectID])
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
	txnID common.TxnID,
) {
	lockedRecords := func() map[ObjectID]struct{} {
		m.lockedRecordsGuard.Lock()
		defer m.lockedRecordsGuard.Unlock()

		lockedRecords, ok := m.lockedRecords[txnID]
		if !ok {
			return make(map[ObjectID]struct{})
		}

		delete(m.lockedRecords, txnID)
		return lockedRecords
	}()

	unlockRequest := TxnUnlockRequest[ObjectID]{
		txnID: txnID,
	}

	for r := range lockedRecords {
		q := func() *txnQueue[LockModeType, ObjectID] {
			qAny, present := m.qs.Load(r)
			assert.Assert(
				present,
				"trying to unlock a transaction on an unlocked tuple. recordID: %+v",
				r,
			)
			q := qAny.(*txnQueue[LockModeType, ObjectID])
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
	isEmpty := true
	m.qs.Range(func(key, value any) bool {
		q := value.(*txnQueue[LockModeType, ObjectID])
		if !q.IsEmpty() {
			isEmpty = false
			return false
		}
		return true
	})
	return isEmpty
}
