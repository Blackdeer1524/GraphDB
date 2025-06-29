package recovery

import (
	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

type LSN uint64

var NIL_LSN LSN = LSN(0)

type BeginLogRecord struct {
	lsn           LSN
	TransactionID txns.TxnID
}

func NewBeginLogRecord(lsn LSN, TransactionID txns.TxnID) BeginLogRecord {
	return BeginLogRecord{
		lsn:           lsn,
		TransactionID: TransactionID,
	}
}

type FileLocation struct {
	PageID  uint64
	SlotNum uint16
}

// is considered NIL iff lsn is NIL_LSN
type LogRecordLocationInfo struct {
	Lsn      LSN
	Location FileLocation
}

func NewNilLogRecordLocation() LogRecordLocationInfo {
	return LogRecordLocationInfo{
		Lsn:      NIL_LSN,
		Location: FileLocation{},
	}
}

func (p *LogRecordLocationInfo) isNil() bool {
	return p.Lsn == NIL_LSN
}

type UpdateLogRecord struct {
	lsn                  LSN
	TransactionID        txns.TxnID
	parentLogLocation    LogRecordLocationInfo
	modifiedPageIdentity bufferpool.PageIdentity
	modifiedSlotNumber   uint16
	beforeValue          []byte
	afterValue           []byte
}

func (r *UpdateLogRecord) Undo(
	lsn LSN,
	parentLogLocation LogRecordLocationInfo,
) CompensationLogRecord {
	return NewCompensationLogRecord(
		lsn,
		r.TransactionID,
		parentLogLocation,
		r.modifiedPageIdentity,
		r.modifiedSlotNumber,
		false,
		r.parentLogLocation.Lsn,
		r.afterValue,
		r.beforeValue,
	)
}

func NewUpdateLogRecord(
	lsn LSN,
	TransactionID txns.TxnID,
	parentLogLocation LogRecordLocationInfo,
	modifiedPageIdentity bufferpool.PageIdentity,
	modifiedSlotNumber uint16,
	beforeValue []byte,
	afterValue []byte,
) UpdateLogRecord {
	return UpdateLogRecord{
		lsn:                  lsn,
		TransactionID:        TransactionID,
		parentLogLocation:    parentLogLocation,
		modifiedPageIdentity: modifiedPageIdentity,
		modifiedSlotNumber:   modifiedSlotNumber,
		beforeValue:          beforeValue,
		afterValue:           afterValue,
	}
}

type InsertLogRecord struct {
	lsn                  LSN
	TransactionID        txns.TxnID
	parentLogLocation    LogRecordLocationInfo
	modifiedPageIdentity bufferpool.PageIdentity
	modifiedSlotNumber   uint16
	value                []byte
}

func NewInsertLogRecord(
	lsn LSN,
	TransactionID txns.TxnID,
	parentLogLocation LogRecordLocationInfo,
	modifiedPageIdentity bufferpool.PageIdentity,
	modifiedSlotNumber uint16,
	value []byte,
) InsertLogRecord {
	return InsertLogRecord{
		lsn:                  lsn,
		TransactionID:        TransactionID,
		parentLogLocation:    parentLogLocation,
		modifiedPageIdentity: modifiedPageIdentity,
		modifiedSlotNumber:   modifiedSlotNumber,
		value:                value,
	}
}

func (r *InsertLogRecord) Undo(
	lsn LSN,
	parentLogLocation LogRecordLocationInfo,
) CompensationLogRecord {
	return NewCompensationLogRecord(
		lsn,
		r.TransactionID,
		parentLogLocation,
		r.modifiedPageIdentity,
		r.modifiedSlotNumber,
		true,
		r.parentLogLocation.Lsn,
		r.value,
		make([]byte, len(r.value)),
	)
}

type CommitLogRecord struct {
	lsn               LSN
	TransactionID     txns.TxnID
	parentLogLocation LogRecordLocationInfo
}

func NewCommitLogRecord(
	lsn LSN,
	TransactionID txns.TxnID,
	parentLogLocation LogRecordLocationInfo,
) CommitLogRecord {
	return CommitLogRecord{
		lsn:               lsn,
		TransactionID:     TransactionID,
		parentLogLocation: parentLogLocation,
	}
}

type AbortLogRecord struct {
	lsn               LSN
	TransactionID     txns.TxnID
	parentLogLocation LogRecordLocationInfo
}

func NewAbortLogRecord(lsn LSN, TransactionID txns.TxnID,
	parentLogLocation LogRecordLocationInfo,
) AbortLogRecord {
	return AbortLogRecord{
		lsn:               lsn,
		TransactionID:     TransactionID,
		parentLogLocation: parentLogLocation,
	}
}

type TxnEndLogRecord struct {
	lsn               LSN
	TransactionID     txns.TxnID
	parentLogLocation LogRecordLocationInfo
}

func NewTxnEndLogRecord(lsn LSN, TransactionID txns.TxnID,
	parentLogLocation LogRecordLocationInfo) TxnEndLogRecord {
	return TxnEndLogRecord{
		lsn:               lsn,
		TransactionID:     TransactionID,
		parentLogLocation: parentLogLocation,
	}
}

type CompensationLogRecord struct {
	lsn                  LSN
	TransactionID        txns.TxnID
	parentLogLocation    LogRecordLocationInfo
	nextUndoLSN          LSN
	isDelete             bool
	modifiedPageIdentity bufferpool.PageIdentity
	modifiedSlotNumber   uint16
	beforeValue          []byte
	afterValue           []byte
}

func NewCompensationLogRecord(
	lsn LSN,
	TransactionID txns.TxnID,
	parentLogLocation LogRecordLocationInfo,
	modifiedPageIdentity bufferpool.PageIdentity,
	modifiedSlotNumber uint16,
	isDelete bool,
	nextUndoLSN LSN,
	beforeValue []byte,
	afterValue []byte,
) CompensationLogRecord {
	return CompensationLogRecord{
		lsn:                  lsn,
		TransactionID:        TransactionID,
		parentLogLocation:    parentLogLocation,
		modifiedPageIdentity: modifiedPageIdentity,
		modifiedSlotNumber:   modifiedSlotNumber,
		isDelete:             isDelete,
		nextUndoLSN:          nextUndoLSN,
		beforeValue:          beforeValue,
		afterValue:           afterValue,
	}
}

type CheckpointBeginLogRecord struct {
	lsn LSN
}

func NewCheckpointBegin(lsn LSN) CheckpointBeginLogRecord {
	return CheckpointBeginLogRecord{lsn: lsn}
}

type CheckpointEndLogRecord struct {
	lsn                LSN
	activeTransactions []txns.TxnID
	dirtyPageTable     map[bufferpool.PageIdentity]LogRecordLocationInfo
}

func NewCheckpointEnd(
	lsn LSN,
	activeTransacitons []txns.TxnID,
	dirtyPageTable map[bufferpool.PageIdentity]LogRecordLocationInfo,
) CheckpointEndLogRecord {
	return CheckpointEndLogRecord{
		lsn:                lsn,
		activeTransactions: activeTransacitons,
		dirtyPageTable:     dirtyPageTable,
	}
}
