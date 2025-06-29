package recovery

import "github.com/Blackdeer1524/GraphDB/src/txns"

type txnStatus byte

const (
	TxnStatusUndo txnStatus = iota
	TxnStatusCommit
)

type ATTEntry struct {
	status          txnStatus
	logLocationInfo LogRecordLocationInfo
}

func NewATTEntry(
	status txnStatus,
	location LogRecordLocationInfo,
) ATTEntry {
	return ATTEntry{
		status:          status,
		logLocationInfo: location,
	}
}

type ActiveTransactionsTable struct {
	table map[txns.TxnID]ATTEntry
}

func NewActiveTransactionsTable() ActiveTransactionsTable {
	return ActiveTransactionsTable{
		table: map[txns.TxnID]ATTEntry{},
	}
}

// returns true iff it is the first record for the transaction
func (att *ActiveTransactionsTable) Insert(
	id txns.TxnID,
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

	if !entry.logLocationInfo.isNil() {
		prevEntry.logLocationInfo = entry.logLocationInfo
	}

	att.table[id] = prevEntry

	return false
}
