package recovery

import (
	"errors"
	"math"
	"sync"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

type TxnLogger struct {
	pool bufferpool.BufferPool[*page.SlottedPage]

	// ================
	// лок на запись логов. Нужно для четкой упорядоченности
	// номеров записей и записей на диск
	mu sync.Mutex
	// "где-то" лежит на диске
	logRecordsCount uint64
	logfileID       uint64
	lastLogLocation LogRecordLocationInfo
	// ================

	getActiveTransactions func() []txns.TxnID // Прийдет из лок менеджера

}

func (l *TxnLogger) Iter(start FileLocation) (*LogRecordsIter, error) {
	p, err := l.pool.GetPageNoCreate(bufferpool.PageIdentity{
		FileID: l.logfileID,
		PageID: start.PageID,
	})
	if err != nil {
		return nil, err
	}

	p.RLock()
	iter := newLogRecordIter(l.logfileID, start, l.pool, p)

	return iter, nil
}

/*
 * TODO: Разобраться где именн хранить
 * 1. точку начала (№ страницы лог файла) последнего чекпоинта
 *    Не обязательно сразу флашить на диск. Обязательно флашим
 *    точку оканчания чекпоинта <---- откуда восстанавливаться
 * 2. № страницы последней записи <---- куда начать писать
 *    после инициализации (флашить НЕ обязательно)
 */
func NewTxnLogger() {
	panic("not implemented")
}

func (l *TxnLogger) Recover(checkpointLocation FileLocation) {
	ATT, DPT := l.recoverAnalyze(checkpointLocation)
	earliestLog := l.recoverPrepareCLRs(ATT, DPT)
	l.recoverRedo(earliestLog.Location)
}

func (l *TxnLogger) recoverAnalyze(
	checkpointLocation FileLocation,
) (ActiveTransactionsTable, map[bufferpool.PageIdentity]LogRecordLocationInfo) {
	iter, err := l.Iter(FileLocation{
		PageID:  checkpointLocation.PageID,
		SlotNum: checkpointLocation.SlotNum,
	})
	assert.Assert(err == nil, "couldn't recover. reason: %+v", err)

	ATT := NewActiveTransactionsTable()
	DPT := map[bufferpool.PageIdentity]LogRecordLocationInfo{}

	for {
		tag, untypedRecord, err := iter.ReadRecord()
		assert.Assert(err == nil, "couldn't read a record. reason: %+v", err)

		switch tag {
		case TypeBegin:
			record, ok := untypedRecord.(BeginLogRecord)
			assert.Assert(ok, "couldn't type cast the record")

			assert.Assert(ATT.Insert(
				record.TransactionID,
				tag,
				NewATTEntry(
					TxnStatusUndo,
					LogRecordLocationInfo{
						Lsn:      record.lsn,
						Location: iter.Location(),
					}),
			), "Found a `begin` record for the already running transaction. TransactionID: %d", record.TransactionID)
		case TypeInsert:
			record, ok := untypedRecord.(InsertLogRecord)
			assert.Assert(ok, "couldn't type cast the record")

			ATT.Insert(
				record.TransactionID,
				tag,
				NewATTEntry(
					TxnStatusUndo,
					LogRecordLocationInfo{
						Lsn:      record.lsn,
						Location: iter.Location(),
					}),
			)

			_, alreadyExists := DPT[record.modifiedPageIdentity]
			if !alreadyExists {
				DPT[record.modifiedPageIdentity] = LogRecordLocationInfo{
					Lsn:      record.lsn,
					Location: iter.Location(),
				}
			}
		case TypeUpdate:
			record, ok := untypedRecord.(UpdateLogRecord)
			recordLocation := LogRecordLocationInfo{
				Lsn:      record.lsn,
				Location: iter.Location(),
			}

			assert.Assert(ok, "couldn't type cast the record")

			ATT.Insert(
				record.TransactionID,
				tag,
				NewATTEntry(
					TxnStatusUndo,
					recordLocation,
				),
			)

			_, alreadyExists := DPT[record.modifiedPageIdentity]
			if !alreadyExists {
				DPT[record.modifiedPageIdentity] = recordLocation
			}
		case TypeCommit:
			record, ok := untypedRecord.(CommitLogRecord)
			assert.Assert(ok, "couldn't type cast the record")
			ATT.Insert(
				record.TransactionID,
				tag,
				NewATTEntry(
					TxnStatusCommit,
					LogRecordLocationInfo{
						Lsn:      record.lsn,
						Location: iter.Location(),
					}),
			)
		case TypeAbort:
			record, ok := untypedRecord.(AbortLogRecord)
			assert.Assert(ok, "couldn't type cast the record")

			ATT.Insert(
				record.TransactionID,
				tag,
				NewATTEntry(
					TxnStatusUndo,
					LogRecordLocationInfo{
						Lsn:      record.lsn,
						Location: iter.Location(),
					}),
			)
		case TypeTxnEnd:
			record, ok := untypedRecord.(TxnEndLogRecord)
			assert.Assert(ok, "couldn't type cast the record")
			delete(ATT.table, record.TransactionID)
		case TypeCheckpointBegin:
		case TypeCheckpointEnd:
			record, ok := untypedRecord.(CheckpointEndLogRecord)
			assert.Assert(ok, "couldn't type cast the record")

			// Lecture #21: Database Crash Recovery @CMU
			// Active Transactions Table
			// Monitors the last
			for _, TransactionID := range record.activeTransactions {
				ATT.Insert(TransactionID, TypeBegin, NewATTEntry(
					TxnStatusUndo,
					NewNilLogRecordLocation(),
				))
			}

			// Dirty Page Table (DPT):
			// The DPT contains information about the pages in the buffer pool
			// that were
			// modified by uncommitted txns. There is one entry per dirty page
			// containing the recLSN (i.e., the LSN of the log record that first
			// caused the page to be dirty).
			//
			// The DPT contains all pages that are dirty in the buffer pool.
			// It doesn’t matter if the changes were caused
			// by a transaction that is running, committed, or aborted.
			for pageInfo, firstDirtyLSN := range record.dirtyPageTable {
				if _, alreadyExists := DPT[pageInfo]; !alreadyExists {
					DPT[pageInfo] = firstDirtyLSN
				}
			}
		case TypeCompensation:
			record, ok := untypedRecord.(CompensationLogRecord)
			assert.Assert(ok, "couldn't type cast the record")

			ATT.Insert(
				record.TransactionID,
				tag,
				NewATTEntry(
					TxnStatusUndo,
					LogRecordLocationInfo{
						Lsn:      record.lsn,
						Location: iter.Location(),
					}),
			)
		default:
			assert.Assert(
				tag < TypeUnknown,
				"unexpected log record type: %d",
				tag,
			)
			panic("unreachable")
		}

		success, err := iter.MoveForward()
		assert.Assert(err == nil, "%+v", err)

		if !success {
			break
		}
	}

	return ATT, DPT
}

func (l *TxnLogger) recoverPrepareCLRs(
	ATT ActiveTransactionsTable,
	DPT map[bufferpool.PageIdentity]LogRecordLocationInfo,
) LogRecordLocationInfo {
	earliestLogLocation := LogRecordLocationInfo{
		Lsn:      LSN(math.MaxUint64),
		Location: FileLocation{},
	}

	for _, entry := range ATT.table {
		if entry.status != TxnStatusUndo {
			continue
		}

		recordLocation := entry.logLocationInfo.Location
		clrsFound := 0
	outer:
		for {
			tag, record, err := l.readLogRecord(recordLocation)
			assert.Assert(err == nil, "todo")
			switch tag {
			case TypeBegin:
				record, ok := record.(BeginLogRecord)
				assert.Assert(ok, "todo")

				if earliestLogLocation.Lsn > record.lsn {
					earliestLogLocation.Lsn = record.lsn
					earliestLogLocation.Location = recordLocation
				}
				assert.Assert(clrsFound == 0, "CLRs aren't balanced out")

				_, err := l.AppendTxnEnd(record.TransactionID, entry.logLocationInfo)
				assert.Assert(err == nil, "todo")
				break outer
			case TypeInsert:
				record, ok := record.(InsertLogRecord)
				assert.Assert(ok, "todo")

				DPT[record.modifiedPageIdentity] = LogRecordLocationInfo{
					Lsn:      record.lsn,
					Location: recordLocation,
				}
				recordLocation = record.parentLogLocation.Location
				if clrsFound > 0 {
					clrsFound--
					continue
				}
				_, err := l.undoInsert(&record)
				assert.Assert(err == nil, "todo")
			case TypeUpdate:
				record, ok := record.(UpdateLogRecord)
				assert.Assert(ok, "todo")

				DPT[record.modifiedPageIdentity] = LogRecordLocationInfo{
					Lsn:      record.lsn,
					Location: recordLocation,
				}
				recordLocation = record.parentLogLocation.Location
				if clrsFound > 0 {
					clrsFound--
					continue
				}
				_, err := l.undoUpdate(&record)
				assert.Assert(err == nil, "todo")
			case TypeCommit:
				assert.Assert(clrsFound == 0, "found CLRs for a commited txn")
				break outer
			case TypeAbort:
				record, ok := record.(AbortLogRecord)
				assert.Assert(ok, "todo")

				recordLocation = record.parentLogLocation.Location
			case TypeTxnEnd:
				assert.Assert(tag != TypeTxnEnd, "unreachable: ATT shouldn't have log records with TxnEnd logs")
			case TypeCompensation:
				record, ok := record.(CompensationLogRecord)
				assert.Assert(ok, "todo")

				clrsFound++
				recordLocation = record.parentLogLocation.Location
			case TypeCheckpointBegin:
				assert.Assert(tag != TypeCheckpointBegin, "unreachable: ATT shouldn't have CheckpointBegin records")
			case TypeCheckpointEnd:
				assert.Assert(tag != TypeCheckpointEnd, "unreachable: ATT shouldn't have CheckpointBegin records")
			default:
				assert.Assert(false, "unexpected record type: %d", tag)
				panic("unreachable")
			}
		}
	}

	return earliestLogLocation
}

func getSlotFromPage(
	pool bufferpool.BufferPool[*page.SlottedPage],
	pageID bufferpool.PageIdentity,
	slotNum uint16,
) (data []byte, err error) {
	p, err := pool.GetPage(pageID)
	defer func() { err = pool.Unpin(pageID) }()
	assert.Assert(err == nil, "todo")

	p.Lock()
	defer p.Unlock()

	data, err = p.Get(slotNum)

	return data, err
}

func (l *TxnLogger) recoverRedo(earliestLog FileLocation) {
	iter, err := l.Iter(earliestLog)
	assert.Assert(err == nil, "todo")

	for {
		tag, record, err := iter.ReadRecord()
		assert.Assert(err == nil, "todo")

		switch tag {
		case TypeInsert:
			record, ok := record.(InsertLogRecord)
			assert.Assert(ok, "todo")

			slotData, err := getSlotFromPage(
				l.pool,
				record.modifiedPageIdentity,
				record.modifiedSlotNumber,
			)
			assert.Assert(
				!errors.Is(err, page.ErrInvalidSlotID),
				"(invariant) slot number should have been correct",
			)
			assert.Assert(err == nil, "todo")
			assert.Assert(
				len(record.value) <= len(slotData),
				"new item len should be at most len of the old one",
			)

			clear(slotData)
			copy(slotData, record.value)
		case TypeUpdate:
			record, ok := record.(UpdateLogRecord)
			assert.Assert(ok, "todo")

			slotData, err := getSlotFromPage(
				l.pool,
				record.modifiedPageIdentity,
				record.modifiedSlotNumber,
			)
			assert.Assert(
				!errors.Is(err, page.ErrInvalidSlotID),
				"(invariant) slot number must be correct",
			)
			assert.Assert(err == nil, "todo")
			assert.Assert(
				len(record.afterValue) <= len(slotData),
				"length should be the same",
			)

			clear(slotData)
			copy(slotData, record.afterValue)
		case TypeCompensation:
			record, ok := record.(CompensationLogRecord)
			assert.Assert(ok, "todo")

			slotData, err := getSlotFromPage(
				l.pool,
				record.modifiedPageIdentity,
				record.modifiedSlotNumber,
			)
			assert.Assert(
				!errors.Is(err, page.ErrInvalidSlotID),
				"(invariant) slot number must be correct",
			)
			assert.Assert(err == nil, "todo")
			assert.Assert(
				len(record.afterValue) <= len(slotData),
				"length should be the same",
			)

			clear(slotData)
			copy(slotData, record.afterValue)
		}

		success, err := iter.MoveForward()
		assert.Assert(err == nil, "todo")

		if !success {
			break
		}
	}
}

func (l *TxnLogger) readLogRecord(
	recordLocation FileLocation,
) (tag LogRecordTypeTag, r any, err error) {
	pageIdent := bufferpool.PageIdentity{
		FileID: l.logfileID,
		PageID: recordLocation.PageID,
	}
	page, err := l.pool.GetPage(pageIdent)

	defer func() { err = errors.Join(err, l.pool.Unpin(pageIdent)) }()

	if err != nil {
		return TypeUnknown, nil, err
	}

	page.RLock()
	record, err := page.Get(recordLocation.SlotNum)
	page.RUnlock()

	if err != nil {
		return TypeUnknown, nil, err
	}

	tag, r, err = readLogRecord(record)

	return tag, r, err
}

// writeLogRecord writes a serialized log record to the log file managed by the
// TxnLogger. It attempts to insert the record into the current log page. If
// there is not enough space on the current page, it advances to the next page
// and retries the insertion. The function returns the location information of
// the written log record or an error if the operation fails.
//
// Parameters:
//
//	serializedRecord []byte - The serialized log record to be written.
//
// Returns:
//
//	LogRecordLocationInfo - The location information of the written log record.
//	error - An error if the operation fails, otherwise nil.
func (lockedLogger *TxnLogger) writeLogRecord(
	serializedRecord []byte,
) (LogRecordLocationInfo, error) {
	pageInfo := bufferpool.PageIdentity{
		FileID: lockedLogger.logfileID,
		PageID: lockedLogger.lastLogLocation.Location.PageID,
	}

	p, err := lockedLogger.pool.GetPage(pageInfo)
	if err != nil {
		return NewNilLogRecordLocation(), err
	}

	p.Lock()
	slotNumber, err := p.Insert(serializedRecord)
	p.Unlock()

	if err == nil {
		lockedLogger.lastLogLocation.Location.SlotNum = slotNumber
		return lockedLogger.lastLogLocation, nil
	}

	assert.Assert(
		errors.Is(err, page.ErrNoEnoughSpace),
		"SlottedPage.Insert contract violation",
	)

	lockedLogger.lastLogLocation.Location.PageID++
	pageInfo = bufferpool.PageIdentity{
		FileID: lockedLogger.logfileID,
		PageID: lockedLogger.lastLogLocation.Location.PageID,
	}

	p, err = lockedLogger.pool.GetPage(pageInfo)
	if err != nil {
		return NewNilLogRecordLocation(), err
	}

	p.Lock()
	slotNumber, err = p.Insert(serializedRecord)
	p.Unlock()
	assert.Assert(!errors.Is(err, page.ErrNoEnoughSpace), "very strange")
	assert.Assert(err == nil, "SlottedPage.Insert contract violation")

	err = lockedLogger.pool.Unpin(pageInfo)
	lockedLogger.lastLogLocation.Location.SlotNum = slotNumber

	return lockedLogger.lastLogLocation, err
}

func (l *TxnLogger) AppendBegin(
	TransactionID txns.TxnID,
) (LogRecordLocationInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lsn := LSN(l.logRecordsCount)
	l.logRecordsCount++

	r := NewBeginLogRecord(lsn, TransactionID)

	bytes, err := r.MarshalBinary()
	if err != nil {
		return NewNilLogRecordLocation(), err
	}

	loc, err := l.writeLogRecord(bytes)
	if err != nil {
		return NewNilLogRecordLocation(), err
	}

	return loc, nil
}

func (l *TxnLogger) AppendUpdate(
	TransactionID txns.TxnID,
	prevLog LogRecordLocationInfo,
	pageInfo bufferpool.PageIdentity,
	slotNumber uint16,
	beforeValue []byte,
	afterValue []byte,
) (LogRecordLocationInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lsn := LSN(l.logRecordsCount)
	l.logRecordsCount++

	r := NewUpdateLogRecord(
		lsn,
		TransactionID,
		prevLog,
		pageInfo,
		slotNumber,
		beforeValue,
		afterValue,
	)

	bytes, err := r.MarshalBinary()
	if err != nil {
		return NewNilLogRecordLocation(), err
	}

	loc, err := l.writeLogRecord(bytes)
	if err != nil {
		return NewNilLogRecordLocation(), err
	}

	return loc, nil
}

func (l *TxnLogger) undoUpdate(
	updateRecord *UpdateLogRecord,
) (LogRecordLocationInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lsn := LSN(l.logRecordsCount)
	l.logRecordsCount++

	r := NewCompensationLogRecord(
		lsn,
		updateRecord.TransactionID,
		l.lastLogLocation,
		updateRecord.modifiedPageIdentity,
		updateRecord.modifiedSlotNumber,
		false,
		updateRecord.parentLogLocation.Lsn,
		updateRecord.afterValue,
		updateRecord.beforeValue,
	)

	bytes, err := r.MarshalBinary()
	if err != nil {
		return NewNilLogRecordLocation(), err
	}

	loc, err := l.writeLogRecord(bytes)
	if err != nil {
		return NewNilLogRecordLocation(), err
	}

	return loc, nil
}

func (l *TxnLogger) AppendInsert(
	TransactionID txns.TxnID,
	prevLog LogRecordLocationInfo,
	pageInfo bufferpool.PageIdentity,
	slotNumber uint16,
	value []byte,
) (LogRecordLocationInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lsn := LSN(l.logRecordsCount)
	l.logRecordsCount++

	r := NewInsertLogRecord(
		lsn,
		TransactionID,
		prevLog,
		pageInfo,
		slotNumber,
		value,
	)

	bytes, err := r.MarshalBinary()
	if err != nil {
		return NewNilLogRecordLocation(), err
	}

	loc, err := l.writeLogRecord(bytes)
	if err != nil {
		return NewNilLogRecordLocation(), err
	}

	return loc, nil
}

func (l *TxnLogger) undoInsert(
	insertRecord *InsertLogRecord,
) (LogRecordLocationInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lsn := LSN(l.logRecordsCount)
	l.logRecordsCount++

	r := NewCompensationLogRecord(
		lsn,
		insertRecord.TransactionID,
		l.lastLogLocation,
		insertRecord.modifiedPageIdentity,
		insertRecord.modifiedSlotNumber,
		true,
		insertRecord.parentLogLocation.Lsn,
		insertRecord.value,
		[]byte{},
	)

	bytes, err := r.MarshalBinary()
	if err != nil {
		return NewNilLogRecordLocation(), err
	}

	loc, err := l.writeLogRecord(bytes)
	if err != nil {
		return NewNilLogRecordLocation(), err
	}

	return loc, nil
}

func (l *TxnLogger) AppendCommit(
	TransactionID txns.TxnID,
	prevLog LogRecordLocationInfo,
) (LogRecordLocationInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lsn := LSN(l.logRecordsCount)
	l.logRecordsCount++

	r := NewCommitLogRecord(lsn, TransactionID, prevLog)

	bytes, err := r.MarshalBinary()
	if err != nil {
		return NewNilLogRecordLocation(), err
	}

	loc, err := l.writeLogRecord(bytes)
	if err != nil {
		return NewNilLogRecordLocation(), err
	}

	return loc, nil
}

func (l *TxnLogger) AppendAbort(
	TransactionID txns.TxnID,
	prevLog LogRecordLocationInfo,
) (LogRecordLocationInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lsn := LSN(l.logRecordsCount)
	l.logRecordsCount++

	r := NewAbortLogRecord(lsn, TransactionID, prevLog)

	bytes, err := r.MarshalBinary()
	if err != nil {
		return NewNilLogRecordLocation(), err
	}

	loc, err := l.writeLogRecord(bytes)
	if err != nil {
		return NewNilLogRecordLocation(), err
	}

	return loc, nil
}

func (l *TxnLogger) AppendTxnEnd(
	TransactionID txns.TxnID,
	prevLog LogRecordLocationInfo,
) (LogRecordLocationInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lsn := LSN(l.logRecordsCount)
	l.logRecordsCount++

	r := NewTxnEndLogRecord(lsn, TransactionID, prevLog)

	bytes, err := r.MarshalBinary()
	if err != nil {
		return NewNilLogRecordLocation(), err
	}

	loc, err := l.writeLogRecord(bytes)
	if err != nil {
		return NewNilLogRecordLocation(), err
	}

	return loc, nil
}

func (l *TxnLogger) AppendCheckpointBegin() (LogRecordLocationInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lsn := LSN(l.logRecordsCount)
	l.logRecordsCount++

	r := NewCheckpointBegin(lsn)

	bytes, err := r.MarshalBinary()
	if err != nil {
		return NewNilLogRecordLocation(), err
	}

	loc, err := l.writeLogRecord(bytes)
	if err != nil {
		return NewNilLogRecordLocation(), err
	}

	return loc, nil
}

func (l *TxnLogger) AppendCheckpointEnd(
	activeTransacitons []txns.TxnID,
	dirtyPageTable map[bufferpool.PageIdentity]LogRecordLocationInfo,
) (LogRecordLocationInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lsn := LSN(l.logRecordsCount)
	l.logRecordsCount++

	r := NewCheckpointEnd(lsn, activeTransacitons, dirtyPageTable)

	bytes, err := r.MarshalBinary()
	if err != nil {
		return NewNilLogRecordLocation(), err
	}

	loc, err := l.writeLogRecord(bytes)
	if err != nil {
		return NewNilLogRecordLocation(), err
	}

	return loc, nil
}
