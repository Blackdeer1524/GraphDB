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
	Combine(Lock) Lock
	WeakerOrEqual(Lock) bool
}

var (
	PageLockShared    PageLockMode = PageLockMode{0}
	PageLockExclusive PageLockMode = PageLockMode{1}
)

var (
	GranularLockIntentionShared          GranularLockMode = GranularLockMode{0}
	GranularLockIntentionExclusive       GranularLockMode = GranularLockMode{1}
	GranularLockShared                   GranularLockMode = GranularLockMode{2}
	GranularLockSharedIntentionExclusive GranularLockMode = GranularLockMode{3}
	GranularLockExclusive                GranularLockMode = GranularLockMode{4}
)

var (
	_ GranularLock[PageLockMode]     = PageLockMode{0}
	_ GranularLock[GranularLockMode] = GranularLockMode{0}
)

func (m PageLockMode) String() string {
	switch m {
	case PageLockShared:
		return "SHARED"
	case PageLockExclusive:
		return "EXCLUSIVE"
	default:
		return fmt.Sprintf("PageLockMode(%d)", m.v)
	}
}

func (m GranularLockMode) String() string {
	switch m {
	case GranularLockIntentionShared:
		return "INTENTION_SHARED"
	case GranularLockIntentionExclusive:
		return "INTENTION_EXCLUSIVE"
	case GranularLockShared:
		return "SHARED"
	case GranularLockSharedIntentionExclusive:
		return "SHARED_INTENTION_EXCLUSIVE"
	case GranularLockExclusive:
		return "EXCLUSIVE"
	default:
		return fmt.Sprintf("GranularLockMode(%d)", m.v)
	}
}

func (m PageLockMode) Compatible(other PageLockMode) bool {
	if m == PageLockShared && other == PageLockShared {
		return true
	}
	return false
}

func (m PageLockMode) Combine(to PageLockMode) PageLockMode {
	switch m {
	case PageLockShared:
		switch to {
		case PageLockShared:
			return PageLockShared
		case PageLockExclusive:
			return PageLockExclusive
		}
	case PageLockExclusive:
		return PageLockExclusive
	}
	panic("unreachable")
}

func (m PageLockMode) WeakerOrEqual(other PageLockMode) bool {
	switch m {
	case PageLockShared:
		switch other {
		case PageLockShared:
			return true
		case PageLockExclusive:
			return true
		}
	case PageLockExclusive:
		switch other {
		case PageLockShared:
			return false
		case PageLockExclusive:
			return true
		}
	}
	panic("unreachable")
}

// https://www.geeksforgeeks.org/dbms/multiple-granularity-locking-in-dbms/
func (m GranularLockMode) Compatible(other GranularLockMode) bool {
	switch m {
	case GranularLockIntentionShared:
		switch other {
		case GranularLockIntentionShared:
			return true
		case GranularLockIntentionExclusive:
			return true
		case GranularLockShared:
			return true
		case GranularLockSharedIntentionExclusive:
			return true
		case GranularLockExclusive:
			return false
		}
	case GranularLockIntentionExclusive:
		switch other {
		case GranularLockIntentionShared:
			return true
		case GranularLockIntentionExclusive:
			return true
		case GranularLockShared:
			return false
		case GranularLockSharedIntentionExclusive:
			return false
		case GranularLockExclusive:
			return false
		}
	case GranularLockShared:
		switch other {
		case GranularLockIntentionShared:
			return true
		case GranularLockIntentionExclusive:
			return false
		case GranularLockShared:
			return true
		case GranularLockSharedIntentionExclusive:
			return false
		case GranularLockExclusive:
			return false
		}
	case GranularLockSharedIntentionExclusive:
		switch other {
		case GranularLockIntentionShared:
			return true
		case GranularLockIntentionExclusive:
			return false
		case GranularLockShared:
			return false
		case GranularLockSharedIntentionExclusive:
			return false
		case GranularLockExclusive:
			return false
		}
	case GranularLockExclusive:
		switch other {
		case GranularLockIntentionShared:
			return false
		case GranularLockIntentionExclusive:
			return false
		case GranularLockShared:
			return false
		case GranularLockSharedIntentionExclusive:
			return false
		case GranularLockExclusive:
			return false
		}
	}

	assert.Assert(false, "unreachable")
	return false
}

func (m GranularLockMode) Combine(another GranularLockMode) GranularLockMode {
	switch m {
	case GranularLockIntentionShared:
		switch another {
		case GranularLockIntentionShared:
			return GranularLockIntentionShared
		case GranularLockIntentionExclusive:
			return GranularLockIntentionExclusive
		case GranularLockShared:
			return GranularLockShared
		case GranularLockSharedIntentionExclusive:
			return GranularLockSharedIntentionExclusive
		case GranularLockExclusive:
			return GranularLockExclusive
		}
	case GranularLockIntentionExclusive:
		switch another {
		case GranularLockIntentionShared:
			return GranularLockIntentionExclusive
		case GranularLockIntentionExclusive:
			return GranularLockIntentionExclusive
		case GranularLockShared:
			return GranularLockSharedIntentionExclusive
		case GranularLockSharedIntentionExclusive:
			return GranularLockSharedIntentionExclusive
		case GranularLockExclusive:
			return GranularLockExclusive
		}
	case GranularLockShared:
		switch another {
		case GranularLockIntentionShared:
			return GranularLockShared
		case GranularLockIntentionExclusive:
			return GranularLockSharedIntentionExclusive
		case GranularLockShared:
			return GranularLockShared
		case GranularLockSharedIntentionExclusive:
			return GranularLockSharedIntentionExclusive
		case GranularLockExclusive:
			return GranularLockExclusive
		}
	case GranularLockSharedIntentionExclusive:
		switch another {
		case GranularLockIntentionShared:
			return GranularLockSharedIntentionExclusive
		case GranularLockIntentionExclusive:
			return GranularLockSharedIntentionExclusive
		case GranularLockShared:
			return GranularLockSharedIntentionExclusive
		case GranularLockSharedIntentionExclusive:
			return GranularLockSharedIntentionExclusive
		case GranularLockExclusive:
			return GranularLockExclusive
		}
	case GranularLockExclusive:
		return GranularLockExclusive
	}
	panic("unreachable")
}

func (m GranularLockMode) WeakerOrEqual(other GranularLockMode) bool {
	switch m {
	case GranularLockIntentionShared:
		switch other {
		case GranularLockIntentionShared:
			return true
		case GranularLockIntentionExclusive:
			return true
		case GranularLockShared:
			return true
		case GranularLockSharedIntentionExclusive:
			return true
		case GranularLockExclusive:
			return true
		}
	case GranularLockIntentionExclusive:
		switch other {
		case GranularLockIntentionShared:
			return false
		case GranularLockIntentionExclusive:
			return true
		case GranularLockShared:
			return false
		case GranularLockSharedIntentionExclusive:
			return true
		case GranularLockExclusive:
			return true
		}
	case GranularLockShared:
		switch other {
		case GranularLockIntentionShared:
			return false
		case GranularLockIntentionExclusive:
			return false
		case GranularLockShared:
			return true
		case GranularLockSharedIntentionExclusive:
			return true
		case GranularLockExclusive:
			return true
		}
	case GranularLockSharedIntentionExclusive:
		switch other {
		case GranularLockIntentionShared:
			return false
		case GranularLockIntentionExclusive:
			return false
		case GranularLockShared:
			return false
		case GranularLockSharedIntentionExclusive:
			return true
		case GranularLockExclusive:
			return true
		}
	case GranularLockExclusive:
		switch other {
		case GranularLockIntentionShared:
			return false
		case GranularLockIntentionExclusive:
			return false
		case GranularLockShared:
			return false
		case GranularLockSharedIntentionExclusive:
			return false
		case GranularLockExclusive:
			return true
		}
	}
	panic("unreachable")
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
