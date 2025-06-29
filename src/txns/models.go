package txns

import "github.com/Blackdeer1524/GraphDB/src/pkg/assert"

type RecordID uint64
type TableID uint64

/* a monotonically increasing counter. It is guaranteed to be unique between transactions
 * WARN: there might be problems with synchronization
 *       in distributed systems that use this kind of transaction IDs */
type TxnID uint64

type TaggedLockMode[TypeTag any] int

type RecordLockMode TaggedLockMode[RecordID]
type TableLockMode TaggedLockMode[TableID]

type LockMode[T any] interface {
	Compatible(T) bool
}

const (
	RECORD_LOCK_SHARED RecordLockMode = iota
	RECORD_LOCK_EXCLUSIVE
)

const (
	TABLE_LOCK_INTENTION_SHARED TableLockMode = iota
	TABLE_LOCK_INTENTION_EXCLUSIVE
	TABLE_LOCK_SHARED
	TABLE_LOCK_SHARED_INTENTION_EXCLUSIVE
	TABLE_LOCK_EXCLUSIVE
)

func (m RecordLockMode) Compatible(other RecordLockMode) bool {
	if m == RECORD_LOCK_SHARED && other == RECORD_LOCK_SHARED {
		return true
	}
	return false
}

func (m TableLockMode) Compatible(other TableLockMode) bool {
	switch m {
	case TABLE_LOCK_INTENTION_SHARED:
		switch other {
		case TABLE_LOCK_INTENTION_SHARED:
			return true
		case TABLE_LOCK_INTENTION_EXCLUSIVE:
			return true
		case TABLE_LOCK_SHARED:
			return true
		case TABLE_LOCK_SHARED_INTENTION_EXCLUSIVE:
			return true
		case TABLE_LOCK_EXCLUSIVE:
			return false
		}
	case TABLE_LOCK_INTENTION_EXCLUSIVE:
		switch other {
		case TABLE_LOCK_INTENTION_SHARED:
			return true
		case TABLE_LOCK_INTENTION_EXCLUSIVE:
			return true
		case TABLE_LOCK_SHARED:
			return false
		case TABLE_LOCK_SHARED_INTENTION_EXCLUSIVE:
			return false
		case TABLE_LOCK_EXCLUSIVE:
			return false
		}
	case TABLE_LOCK_SHARED:
		switch other {
		case TABLE_LOCK_INTENTION_SHARED:
			return true
		case TABLE_LOCK_INTENTION_EXCLUSIVE:
			return false
		case TABLE_LOCK_SHARED:
			return true
		case TABLE_LOCK_SHARED_INTENTION_EXCLUSIVE:
			return false
		case TABLE_LOCK_EXCLUSIVE:
			return false
		}
	case TABLE_LOCK_SHARED_INTENTION_EXCLUSIVE:
		switch other {
		case TABLE_LOCK_INTENTION_SHARED:
			return true
		case TABLE_LOCK_INTENTION_EXCLUSIVE:
			return false
		case TABLE_LOCK_SHARED:
			return false
		case TABLE_LOCK_SHARED_INTENTION_EXCLUSIVE:
			return false
		case TABLE_LOCK_EXCLUSIVE:
			return false
		}
	case TABLE_LOCK_EXCLUSIVE:
		switch other {
		case TABLE_LOCK_INTENTION_SHARED:
			return false
		case TABLE_LOCK_INTENTION_EXCLUSIVE:
			return false
		case TABLE_LOCK_SHARED:
			return false
		case TABLE_LOCK_SHARED_INTENTION_EXCLUSIVE:
			return false
		case TABLE_LOCK_EXCLUSIVE:
			return false
		}
	}

	assert.Assert(false, "unreachable")
	return false
}

type TxnLockRequest[LockModeType LockMode[LockModeType], ObjectIDType comparable] struct {
	txnID    TxnID
	recordId ObjectIDType
	lockMode LockModeType
}

func NewTxnLockRequest[LockModeType LockMode[LockModeType], ObjectIDType comparable](
	txnID TxnID,
	recordId ObjectIDType,
	lockMode LockModeType,
) *TxnLockRequest[LockModeType, ObjectIDType] {
	return &TxnLockRequest[LockModeType, ObjectIDType]{
		txnID:    txnID,
		recordId: recordId,
		lockMode: lockMode,
	}
}

type TxnUnlockRequest[ObjectIDType comparable] struct {
	txnID    TxnID
	recordId ObjectIDType
}

func NewTxnUnlockRequest[ObjectIDType comparable](
	txnID TxnID,
	recordId ObjectIDType,
) *TxnUnlockRequest[ObjectIDType] {
	return &TxnUnlockRequest[ObjectIDType]{
		txnID:    txnID,
		recordId: recordId,
	}
}
