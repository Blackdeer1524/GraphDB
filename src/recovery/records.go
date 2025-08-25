package recovery

import (
	"encoding"
	"fmt"

	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
)

type LogRecord interface {
	LSN() common.LSN
	String() string
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

type RevertableLogRecord interface {
	LogRecord
	Undo(
		newLSN common.LSN,
		parentLogLocation common.LogRecordLocInfo,
	) CompensationLogRecord
}

var (
	_ LogRecord           = &BeginLogRecord{}
	_ RevertableLogRecord = &InsertLogRecord{}
	_ RevertableLogRecord = &UpdateLogRecord{}
	_ RevertableLogRecord = &DeleteLogRecord{}
	_ LogRecord           = &CommitLogRecord{}
	_ LogRecord           = &AbortLogRecord{}
	_ LogRecord           = &CheckpointBeginLogRecord{}
	_ LogRecord           = &CheckpointEndLogRecord{}
	_ LogRecord           = &CompensationLogRecord{}
	_ LogRecord           = &TxnEndLogRecord{}
)

type BeginLogRecord struct {
	lsn   common.LSN
	txnID common.TxnID
}

func NewBeginLogRecord(lsn common.LSN, txnID common.TxnID) BeginLogRecord {
	return BeginLogRecord{
		lsn:   lsn,
		txnID: txnID,
	}
}

func (l *BeginLogRecord) LSN() common.LSN {
	return l.lsn
}

func (l *BeginLogRecord) String() string {
	return fmt.Sprintf("BEGIN lsn:%d txn:%d", l.lsn, l.txnID)
}

type UpdateLogRecord struct {
	lsn               common.LSN
	txnID             common.TxnID
	parentLogLocation common.LogRecordLocInfo
	modifiedRecordID  common.RecordID
	beforeValue       []byte
	afterValue        []byte
}

func (r *UpdateLogRecord) LSN() common.LSN {
	return r.lsn
}

func (r *UpdateLogRecord) String() string {
	return fmt.Sprintf(
		"UPDATE lsn:%d txn:%d parent:%+v modified:%+v before:%+v after:%+v",
		r.lsn,
		r.txnID,
		r.parentLogLocation,
		r.modifiedRecordID,
		r.beforeValue,
		r.afterValue,
	)
}

func (r *UpdateLogRecord) Undo(
	lsn common.LSN,
	parentLogLocation common.LogRecordLocInfo,
) CompensationLogRecord {
	return NewCompensationLogRecord(
		lsn,
		r.txnID,
		parentLogLocation,
		r.modifiedRecordID,
		CLRtypeUpdate,
		r.parentLogLocation.Lsn,
		r.afterValue,
		r.beforeValue,
	)
}

func NewUpdateLogRecord(
	lsn common.LSN,
	txnID common.TxnID,
	parentLogLocation common.LogRecordLocInfo,
	modifiedRecordID common.RecordID,
	beforeValue []byte,
	afterValue []byte,
) UpdateLogRecord {
	assert.Assert(len(afterValue) <= len(beforeValue))

	return UpdateLogRecord{
		lsn:               lsn,
		txnID:             txnID,
		parentLogLocation: parentLogLocation,
		modifiedRecordID:  modifiedRecordID,
		beforeValue:       beforeValue,
		afterValue:        afterValue,
	}
}

type InsertLogRecord struct {
	lsn               common.LSN
	txnID             common.TxnID
	parentLogLocation common.LogRecordLocInfo
	modifiedRecordID  common.RecordID
	value             []byte
}

func (r *InsertLogRecord) LSN() common.LSN {
	return r.lsn
}

func (r *InsertLogRecord) String() string {
	return fmt.Sprintf(
		"INSERT lsn:%d txn:%d parent:%+v modified:%+v value:%+v",
		r.lsn,
		r.txnID,
		r.parentLogLocation,
		r.modifiedRecordID,
		r.value,
	)
}

func NewInsertLogRecord(
	lsn common.LSN,
	txnID common.TxnID,
	parentLogLocation common.LogRecordLocInfo,
	modifiedRecordID common.RecordID,
	value []byte,
) InsertLogRecord {
	return InsertLogRecord{
		lsn:               lsn,
		txnID:             txnID,
		parentLogLocation: parentLogLocation,
		modifiedRecordID:  modifiedRecordID,
		value:             value,
	}
}

func (r *InsertLogRecord) Undo(
	lsn common.LSN,
	parentLogLocation common.LogRecordLocInfo,
) CompensationLogRecord {
	return NewCompensationLogRecord(
		lsn,
		r.txnID,
		parentLogLocation,
		r.modifiedRecordID,
		CLRtypeInsert,
		r.parentLogLocation.Lsn,
		r.value,
		make([]byte, len(r.value)),
	)
}

type DeleteLogRecord struct {
	lsn               common.LSN
	txnID             common.TxnID
	parentLogLocation common.LogRecordLocInfo
	modifiedRecordID  common.RecordID
}

func NewDeleteLogRecord(
	lsn common.LSN,
	txnID common.TxnID,
	parentLogLocation common.LogRecordLocInfo,
	modifiedRecordID common.RecordID,
) DeleteLogRecord {
	return DeleteLogRecord{
		lsn:               lsn,
		txnID:             txnID,
		parentLogLocation: parentLogLocation,
		modifiedRecordID:  modifiedRecordID,
	}
}

func (r *DeleteLogRecord) LSN() common.LSN {
	return r.lsn
}

func (r *DeleteLogRecord) String() string {
	return fmt.Sprintf(
		"DELETE lsn:%d txn:%d parent:%+v modified:%+v",
		r.lsn,
		r.txnID,
		r.parentLogLocation,
		r.modifiedRecordID,
	)
}

func (r *DeleteLogRecord) Undo(
	lsn common.LSN,
	parentLogLocation common.LogRecordLocInfo,
) CompensationLogRecord {
	return NewCompensationLogRecord(
		lsn,
		r.txnID,
		parentLogLocation,
		r.modifiedRecordID,
		CLRtypeDelete,
		r.parentLogLocation.Lsn,
		make([]byte, 0),
		make([]byte, 0),
	)
}

type CommitLogRecord struct {
	lsn               common.LSN
	txnID             common.TxnID
	parentLogLocation common.LogRecordLocInfo
}

func (r *CommitLogRecord) LSN() common.LSN {
	return r.lsn
}

func NewCommitLogRecord(
	lsn common.LSN,
	txnID common.TxnID,
	parentLogLocation common.LogRecordLocInfo,
) CommitLogRecord {
	return CommitLogRecord{
		lsn:               lsn,
		txnID:             txnID,
		parentLogLocation: parentLogLocation,
	}
}

func (r *CommitLogRecord) String() string {
	return fmt.Sprintf(
		"COMMIT lsn:%d txn:%d parent:%+v",
		r.lsn,
		r.txnID,
		r.parentLogLocation,
	)
}

type AbortLogRecord struct {
	lsn               common.LSN
	txnID             common.TxnID
	parentLogLocation common.LogRecordLocInfo
}

func (r *AbortLogRecord) LSN() common.LSN {
	return r.lsn
}

func (r *AbortLogRecord) String() string {
	return fmt.Sprintf(
		"ABORT lsn:%d txn:%d parent:%+v",
		r.lsn,
		r.txnID,
		r.parentLogLocation,
	)
}

func NewAbortLogRecord(lsn common.LSN, txnID common.TxnID,
	parentLogLocation common.LogRecordLocInfo,
) AbortLogRecord {
	return AbortLogRecord{
		lsn:               lsn,
		txnID:             txnID,
		parentLogLocation: parentLogLocation,
	}
}

type TxnEndLogRecord struct {
	lsn               common.LSN
	txnID             common.TxnID
	parentLogLocation common.LogRecordLocInfo
}

func (r *TxnEndLogRecord) LSN() common.LSN {
	return r.lsn
}

func NewTxnEndLogRecord(lsn common.LSN, txnID common.TxnID,
	parentLogLocation common.LogRecordLocInfo) TxnEndLogRecord {
	return TxnEndLogRecord{
		lsn:               lsn,
		txnID:             txnID,
		parentLogLocation: parentLogLocation,
	}
}

func (r *TxnEndLogRecord) String() string {
	return fmt.Sprintf(
		"TXN_END lsn:%d txn:%d parent:%+v",
		r.lsn,
		r.txnID,
		r.parentLogLocation,
	)
}

type CLRtype byte

const (
	CLRtypeInsert CLRtype = iota
	CLRtypeUpdate
	CLRtypeDelete
)

type CompensationLogRecord struct {
	lsn               common.LSN
	txnID             common.TxnID
	parentLogLocation common.LogRecordLocInfo
	nextUndoLSN       common.LSN
	clrType           CLRtype
	modifiedRecordID  common.RecordID
	beforeValue       []byte
	afterValue        []byte
}

func (r *CompensationLogRecord) LSN() common.LSN {
	return r.lsn
}

func (r *CompensationLogRecord) String() string {
	return fmt.Sprintf(
		"CLR lsn:%d txn:%d parent:%+v next_undo:%+v type:%+v modified:%+v before:%+v after:%+v",
		r.lsn,
		r.txnID,
		r.parentLogLocation,
		r.nextUndoLSN,
		r.clrType,
		r.modifiedRecordID,
		r.beforeValue,
		r.afterValue,
	)
}

func NewCompensationLogRecord(
	lsn common.LSN,
	txnID common.TxnID,
	parentLogLocation common.LogRecordLocInfo,
	modifiedRecordID common.RecordID,
	clrType CLRtype,
	nextUndoLSN common.LSN,
	beforeValue []byte,
	afterValue []byte,
) CompensationLogRecord {
	return CompensationLogRecord{
		lsn:               lsn,
		txnID:             txnID,
		parentLogLocation: parentLogLocation,
		modifiedRecordID:  modifiedRecordID,
		clrType:           clrType,
		nextUndoLSN:       nextUndoLSN,
		beforeValue:       beforeValue,
		afterValue:        afterValue,
	}
}

func (r *CompensationLogRecord) Activate() {

}

type CheckpointBeginLogRecord struct {
	lsn common.LSN
}

func NewCheckpointBegin(lsn common.LSN) CheckpointBeginLogRecord {
	return CheckpointBeginLogRecord{lsn: lsn}
}

func (l *CheckpointBeginLogRecord) LSN() common.LSN {
	return l.lsn
}

func (l *CheckpointBeginLogRecord) String() string {
	return fmt.Sprintf("CHECKPOINT_BEGIN lsn:%d", l.lsn)
}

type CheckpointEndLogRecord struct {
	lsn                common.LSN
	activeTransactions map[common.TxnID]common.LogRecordLocInfo
	dirtyPageTable     map[common.PageIdentity]common.LogRecordLocInfo
}

func NewCheckpointEnd(
	lsn common.LSN,
	activeTransactions map[common.TxnID]common.LogRecordLocInfo,
	dirtyPageTable map[common.PageIdentity]common.LogRecordLocInfo,
) CheckpointEndLogRecord {
	return CheckpointEndLogRecord{
		lsn:                lsn,
		activeTransactions: activeTransactions,
		dirtyPageTable:     dirtyPageTable,
	}
}

func (l *CheckpointEndLogRecord) LSN() common.LSN {
	return l.lsn
}

func (l *CheckpointEndLogRecord) ActiveTransactions() map[common.TxnID]common.LogRecordLocInfo {
	return l.activeTransactions
}

func (l *CheckpointEndLogRecord) DirtyPageTable() map[common.PageIdentity]common.LogRecordLocInfo {
	return l.dirtyPageTable
}

func (l *CheckpointEndLogRecord) String() string {
	return fmt.Sprintf("CHECKPOINT_END lsn:%d active_txns:%v dirty_pages:%v",
		l.lsn, l.activeTransactions, l.dirtyPageTable)
}
