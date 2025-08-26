package txns

import (
	"fmt"

	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
)

type TaggedType[T any] struct{ v T } // this trick forbids casting one lock mode to another

type PageLockMode TaggedType[uint8]
type GranularLockMode TaggedType[uint16]

type GranularLock[Lock any] interface {
	fmt.Stringer
	Compatible(Lock) bool
	Upgradable(Lock) bool
	Equal(Lock) bool
}

var (
	PAGE_LOCK_SHARED    PageLockMode = PageLockMode{0}
	PAGE_LOCK_EXCLUSIVE PageLockMode = PageLockMode{1}
)

var (
	GRANULAR_LOCK_INTENTION_SHARED GranularLockMode = GranularLockMode{
		0,
	}
	GRANULAR_LOCK_INTENTION_EXCLUSIVE GranularLockMode = GranularLockMode{
		1,
	}
	GRANULAR_LOCK_SHARED GranularLockMode = GranularLockMode{
		2,
	}
	GRANULAR_LOCK_SHARED_INTENTION_EXCLUSIVE GranularLockMode = GranularLockMode{
		3,
	}
	GRANULAR_LOCK_EXCLUSIVE GranularLockMode = GranularLockMode{
		4,
	}
)

var (
	_ GranularLock[PageLockMode]     = PageLockMode{0}
	_ GranularLock[GranularLockMode] = GranularLockMode{0}
)

func (m PageLockMode) String() string {
	switch m {
	case PAGE_LOCK_SHARED:
		return "SHARED"
	case PAGE_LOCK_EXCLUSIVE:
		return "EXCLUSIVE"
	default:
		return fmt.Sprintf("PageLockMode(%d)", m.v)
	}
}

func (m GranularLockMode) String() string {
	switch m {
	case GRANULAR_LOCK_INTENTION_SHARED:
		return "INTENTION_SHARED"
	case GRANULAR_LOCK_INTENTION_EXCLUSIVE:
		return "INTENTION_EXCLUSIVE"
	case GRANULAR_LOCK_SHARED:
		return "SHARED"
	case GRANULAR_LOCK_SHARED_INTENTION_EXCLUSIVE:
		return "SHARED_INTENTION_EXCLUSIVE"
	case GRANULAR_LOCK_EXCLUSIVE:
		return "EXCLUSIVE"
	default:
		return fmt.Sprintf("GranularLockMode(%d)", m.v)
	}
}

func (m PageLockMode) Compatible(other PageLockMode) bool {
	if m == PAGE_LOCK_SHARED && other == PAGE_LOCK_SHARED {
		return true
	}
	return false
}

func (m PageLockMode) Upgradable(to PageLockMode) bool {
	switch m {
	case PAGE_LOCK_SHARED:
		switch to {
		case PAGE_LOCK_SHARED:
			return false
		case PAGE_LOCK_EXCLUSIVE:
			return true
		}
	case PAGE_LOCK_EXCLUSIVE:
		return false
	}
	return false
}

func (m PageLockMode) Equal(other PageLockMode) bool {
	return m == other
}

// https://www.geeksforgeeks.org/dbms/multiple-granularity-locking-in-dbms/
func (m GranularLockMode) Compatible(other GranularLockMode) bool {
	switch m {
	case GRANULAR_LOCK_INTENTION_SHARED:
		switch other {
		case GRANULAR_LOCK_INTENTION_SHARED:
			return true
		case GRANULAR_LOCK_INTENTION_EXCLUSIVE:
			return true
		case GRANULAR_LOCK_SHARED:
			return true
		case GRANULAR_LOCK_SHARED_INTENTION_EXCLUSIVE:
			return true
		case GRANULAR_LOCK_EXCLUSIVE:
			return false
		}
	case GRANULAR_LOCK_INTENTION_EXCLUSIVE:
		switch other {
		case GRANULAR_LOCK_INTENTION_SHARED:
			return true
		case GRANULAR_LOCK_INTENTION_EXCLUSIVE:
			return true
		case GRANULAR_LOCK_SHARED:
			return false
		case GRANULAR_LOCK_SHARED_INTENTION_EXCLUSIVE:
			return false
		case GRANULAR_LOCK_EXCLUSIVE:
			return false
		}
	case GRANULAR_LOCK_SHARED:
		switch other {
		case GRANULAR_LOCK_INTENTION_SHARED:
			return true
		case GRANULAR_LOCK_INTENTION_EXCLUSIVE:
			return false
		case GRANULAR_LOCK_SHARED:
			return true
		case GRANULAR_LOCK_SHARED_INTENTION_EXCLUSIVE:
			return false
		case GRANULAR_LOCK_EXCLUSIVE:
			return false
		}
	case GRANULAR_LOCK_SHARED_INTENTION_EXCLUSIVE:
		switch other {
		case GRANULAR_LOCK_INTENTION_SHARED:
			return true
		case GRANULAR_LOCK_INTENTION_EXCLUSIVE:
			return false
		case GRANULAR_LOCK_SHARED:
			return false
		case GRANULAR_LOCK_SHARED_INTENTION_EXCLUSIVE:
			return false
		case GRANULAR_LOCK_EXCLUSIVE:
			return false
		}
	case GRANULAR_LOCK_EXCLUSIVE:
		switch other {
		case GRANULAR_LOCK_INTENTION_SHARED:
			return false
		case GRANULAR_LOCK_INTENTION_EXCLUSIVE:
			return false
		case GRANULAR_LOCK_SHARED:
			return false
		case GRANULAR_LOCK_SHARED_INTENTION_EXCLUSIVE:
			return false
		case GRANULAR_LOCK_EXCLUSIVE:
			return false
		}
	}

	assert.Assert(false, "unreachable")
	return false
}

func (m GranularLockMode) Upgradable(to GranularLockMode) bool {
	switch m {
	case GRANULAR_LOCK_INTENTION_SHARED:
		switch to {
		case GRANULAR_LOCK_INTENTION_EXCLUSIVE:
			return true
		case GRANULAR_LOCK_SHARED:
			return true
		case GRANULAR_LOCK_SHARED_INTENTION_EXCLUSIVE:
			return true
		case GRANULAR_LOCK_EXCLUSIVE:
			return true
		default:
			return false
		}
	case GRANULAR_LOCK_INTENTION_EXCLUSIVE:
		return false // Cannot upgrade from intention exclusive in 2PL
	case GRANULAR_LOCK_SHARED:
		switch to {
		case GRANULAR_LOCK_SHARED_INTENTION_EXCLUSIVE:
			return true
		case GRANULAR_LOCK_EXCLUSIVE:
			return true
		default:
			return false
		}
	case GRANULAR_LOCK_SHARED_INTENTION_EXCLUSIVE:
		switch to {
		case GRANULAR_LOCK_EXCLUSIVE:
			return true
		default:
			return false
		}
	case GRANULAR_LOCK_EXCLUSIVE:
		return false // Already exclusive, cannot upgrade
	default:
		return false
	}
}

func (m GranularLockMode) Equal(other GranularLockMode) bool {
	return m == other
}

type TxnLockRequest[LockModeType GranularLock[LockModeType], ObjectIDType comparable] struct {
	txnID    common.TxnID
	objectId ObjectIDType
	lockMode LockModeType
}

func NewTxnLockRequest[LockModeType GranularLock[LockModeType], ObjectIDType comparable](
	txnID common.TxnID,
	objectId ObjectIDType,
	lockMode LockModeType,
) *TxnLockRequest[LockModeType, ObjectIDType] {
	return &TxnLockRequest[LockModeType, ObjectIDType]{
		txnID:    txnID,
		objectId: objectId,
		lockMode: lockMode,
	}
}

type TxnUnlockRequest[ObjectIDType comparable] struct {
	txnID    common.TxnID
	objectId ObjectIDType
}

func NewTxnUnlockRequest[ObjectIDType comparable](
	txnID common.TxnID,
	objectId ObjectIDType,
) *TxnUnlockRequest[ObjectIDType] {
	return &TxnUnlockRequest[ObjectIDType]{
		txnID:    txnID,
		objectId: objectId,
	}
}

type PageLockRequest struct {
	TxnID    common.TxnID
	LockMode PageLockMode
	PageID   uint64
}
