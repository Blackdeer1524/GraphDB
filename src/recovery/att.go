package recovery

import (
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
)

type txnStatus byte

const (
	TxnStatusUndo txnStatus = iota
	TxnStatusCommit
)

type ATTEntry struct {
	status          txnStatus
	logLocationInfo common.LogRecordLocInfo
}

func NewATTEntry(
	status txnStatus,
	location common.LogRecordLocInfo,
) ATTEntry {
	return ATTEntry{
		status:          status,
		logLocationInfo: location,
	}
}

// Lecture #21: Database Crash Recovery @CMU
type ActiveTransactionsTable struct {
	table map[common.TxnID]ATTEntry
}

func NewActiveTransactionsTable() ActiveTransactionsTable {
	return ActiveTransactionsTable{
		table: map[common.TxnID]ATTEntry{},
	}
}

// returns true iff it is the first record for the transaction
func (att *ActiveTransactionsTable) Insert(
	id common.TxnID,
	tag LogRecordTypeTag,
	entry ATTEntry,
) bool {
	if tag == TypeTxnEnd {
		delete(att.table, id)
		return false
	}

	prevEntry, alreadyExists := att.table[id]
	if !alreadyExists {
		att.table[id] = entry
		return true
	}

	// https://stackoverflow.com/questions/42605337/cannot-assign-to-struct-field-in-a-map
	if prevEntry.status == TxnStatusUndo {
		prevEntry.status = entry.status
	}

	if !entry.logLocationInfo.IsNil() {
		prevEntry.logLocationInfo = entry.logLocationInfo
	}

	att.table[id] = prevEntry
	return false
}
