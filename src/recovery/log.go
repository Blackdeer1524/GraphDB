package recovery

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
	"github.com/Blackdeer1524/GraphDB/src/storage/disk"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
)

type loggerInfoPage page.SlottedPage

const (
	loggerCheckpointLocationSlot = iota
)

func (p *loggerInfoPage) GetCheckpointLocation() common.LogRecordLocInfo {
	o := (*page.SlottedPage)(p)
	return utils.FromBytes[common.LogRecordLocInfo](o.LockedRead(loggerCheckpointLocationSlot))
}

func (p *loggerInfoPage) setCheckpointLocation(loc common.LogRecordLocInfo) {
	o := (*page.SlottedPage)(p)

	data, err := loc.MarshalBinary()
	assert.NoError(err)
	o.UnsafeUpdateNoLogs(loggerCheckpointLocationSlot, data)
}

func (p *loggerInfoPage) Setup() {
	o := (*page.SlottedPage)(p)
	o.UnsafeClear()

	dummyRecord := common.LogRecordLocInfo{
		Lsn: common.NilLSN,
		Location: common.FileLocation{
			PageID:  common.CheckpointInfoPageID + 1,
			SlotNum: 0,
		},
	}
	slotOpt := page.InsertSerializable[*common.LogRecordLocInfo](o, &dummyRecord)
	assert.Assert(slotOpt.IsSome())
	assert.Assert(slotOpt.Unwrap() == loggerCheckpointLocationSlot)
}

type txnLogger struct {
	pool       bufferpool.BufferPool
	logfileID  common.FileID
	masterPage *loggerInfoPage

	// ================
	// лок на запись логов. Нужно для четкой упорядоченности
	// номеров записей и записей на диск
	seqMu           sync.Mutex
	logRecordsCount uint64

	// ================
	flushLSN       common.LSN
	firstDirtyPage common.PageID
	curPage        common.PageID
}

/*
 * TODO: Разобраться где именно хранить
 * 1. точку начала (№ страницы лог файла) последнего чекпоинта
 *    Не обязательно сразу флашить на диск. Обязательно флашим
 *    точку оканчания чекпоинта <---- откуда восстанавливаться
 * 2. № страницы последней записи <---- куда начать писать
 *    после инициализации (флашить НЕ обязательно)
 */
func NewTxnLogger(
	pool bufferpool.BufferPool,
	logFileID common.FileID,
) *txnLogger {
	l := &txnLogger{
		pool:            pool,
		logfileID:       logFileID,
		masterPage:      &loggerInfoPage{},
		seqMu:           sync.Mutex{},
		logRecordsCount: 0,
		firstDirtyPage:  0,
		curPage:         0,
	}

	pool.SetLogger(l)
	masterRecordIdent := common.PageIdentity{
		FileID: logFileID,
		PageID: common.CheckpointInfoPageID,
	}

	// this will load master log record's page into memory
	// note that we don't call `Unpin()`. We are going to need this
	// page during replacement.
	var err error
	pg, err := pool.GetPageNoCreate(masterRecordIdent)

	if errors.Is(err, disk.ErrNoSuchPage) {
		pg, err = pool.GetPage(masterRecordIdent)
		assert.NoError(err)
		pool.MarkDirtyNoLogsAssumeLocked(masterRecordIdent)
		l.masterPage = (*loggerInfoPage)(pg)
		l.masterPage.Setup()
	} else {
		assert.NoError(err)
		l.masterPage = (*loggerInfoPage)(pg)
	}

	checkpointLocation := l.masterPage.GetCheckpointLocation()
	l.firstDirtyPage = checkpointLocation.Location.PageID
	l.curPage = checkpointLocation.Location.PageID

	checkpointPageIdent := common.PageIdentity{
		FileID: l.logfileID,
		PageID: checkpointLocation.Location.PageID,
	}
	checkpointPage, err := pool.GetPageNoCreate(checkpointPageIdent)
	if errors.Is(err, disk.ErrNoSuchPage) {
		return l
	}
	if checkpointPage.NumSlots() == 0 {
		pool.Unpin(checkpointPageIdent)
		return l
	}
	pool.Unpin(checkpointPageIdent)
	l.Recover()
	return l
}

type txnLoggerWithContext struct {
	logger                *txnLogger
	txnID                 common.TxnID
	lastLogRecordLocation common.LogRecordLocInfo
}

func newTxnLoggerWithContext(
	logger *txnLogger,
	txnID common.TxnID,
) *txnLoggerWithContext {
	return &txnLoggerWithContext{
		logger: logger,
		txnID:  txnID,
	}
}

var (
	_ common.ITxnLogger            = &txnLogger{}
	_ common.ITxnLoggerWithContext = &txnLoggerWithContext{}
)

func (l *txnLogger) WithContext(
	txnID common.TxnID,
) common.ITxnLoggerWithContext {
	return newTxnLoggerWithContext(l, txnID)
}

func (l *txnLogger) iter(
	start common.FileLocation,
) (*LogRecordsIter, error) {
	p, err := l.pool.GetPageNoCreate(common.PageIdentity{
		FileID: l.logfileID,
		PageID: start.PageID,
	})
	if err != nil {
		return nil, err
	}

	iter := newLogRecordIter(
		l.logfileID,
		start,
		l.pool,
		p,
	)

	return iter, nil
}

func logRecordToString(tag LogRecordTypeTag, untypedRecord any) string {
	switch tag {
	case TypeBegin:
		r := assert.Cast[BeginLogRecord](untypedRecord)
		return r.String()
	case TypeInsert:
		r := assert.Cast[InsertLogRecord](untypedRecord)
		return r.String()
	case TypeUpdate:
		r := assert.Cast[UpdateLogRecord](untypedRecord)
		return r.String()
	case TypeDelete:
		r := assert.Cast[DeleteLogRecord](untypedRecord)
		return r.String()
	case TypeCommit:
		r := assert.Cast[CommitLogRecord](untypedRecord)
		return r.String()
	case TypeAbort:
		r := assert.Cast[AbortLogRecord](untypedRecord)
		return r.String()
	case TypeTxnEnd:
		r := assert.Cast[TxnEndLogRecord](untypedRecord)
		return r.String()
	case TypeCheckpointBegin:
		r := assert.Cast[CheckpointBeginLogRecord](untypedRecord)
		return r.String()
	case TypeCheckpointEnd:
		r := assert.Cast[CheckpointEndLogRecord](untypedRecord)
		return r.String()
	case TypeCompensation:
		r := assert.Cast[CompensationLogRecord](untypedRecord)
		return r.String()
	default:
		assert.Assert(
			tag < TypeUnknown,
			"unknown log record type tag: %#v",
			tag,
		)
		panic("unreachable")
	}
}

func (l *txnLogger) Dump(start common.FileLocation, b *strings.Builder) {
	iter, err := l.iter(start)
	if err != nil {
		return
	}

	for {
		tag, record, err := iter.ReadRecord()
		if err != nil {
			return
		}
		loc := iter.Location()
		fmt.Fprintf(b, "[%d@%d]: ", loc.PageID, loc.SlotNum)
		b.WriteString(logRecordToString(tag, record))
		b.WriteString("\n")
		success, err := iter.MoveForward()
		if err != nil || !success {
			break
		}
	}
}

func (l *txnLogger) GetFlushInfo() (common.FileID, common.PageID, common.PageID, common.LSN) {
	// no parrallel flushing is possible, so no need for locks
	firstDP := atomic.LoadUint64((*uint64)(&l.firstDirtyPage))
	curPage := atomic.LoadUint64((*uint64)(&l.curPage))
	logRecordsCount := atomic.LoadUint64((*uint64)(&l.logRecordsCount))

	return l.logfileID, common.PageID(firstDP), common.PageID(curPage), common.LSN(logRecordsCount)
}

func (l *txnLogger) GetFlushLSN() common.LSN {
	// no parrallel flushing is possible, so no need for locks
	return common.LSN(atomic.LoadUint64(&l.logRecordsCount))
}

func (l *txnLogger) UpdateFirstUnflushedPage(pageID common.PageID) {
	// no parrallel flushing is possible, so no need for locks
	atomic.StoreUint64((*uint64)(&l.firstDirtyPage), uint64(pageID))
}

func (l *txnLogger) UpdateFlushLSN(lsn common.LSN) {
	// no parrallel flushing is possible, so no need for locks
	atomic.StoreUint64((*uint64)(&l.flushLSN), uint64(lsn))
}

func (l *txnLogger) Recover() {
	checkpointLocation := l.masterPage.GetCheckpointLocation()

	lastRecordLSN, ATT, DPT := l.recoverAnalyze(checkpointLocation)
	l.logRecordsCount = uint64(lastRecordLSN)

	earliestLogLocation := l.recoverPrepareCLRs(ATT, DPT)
	l.recoverRedo(earliestLogLocation)
}

func (l *txnLogger) recoverAnalyze(
	checkpointLocation common.LogRecordLocInfo,
) (common.LSN, ActiveTransactionsTable, map[common.PageIdentity]common.LogRecordLocInfo) {
	iter, err := l.iter(checkpointLocation.Location)
	assert.Assert(err == nil, "couldn't recover. reason: %+v", err)

	lastRecordLSN := common.LSN(0)
	ATT := NewActiveTransactionsTable()
	DPT := map[common.PageIdentity]common.LogRecordLocInfo{}

	for {
		tag, untypedRecord, err := iter.ReadRecord()
		assert.Assert(err == nil, "couldn't read a record. reason: %+v", err)

		switch tag {
		case TypeBegin:
			record := assert.Cast[BeginLogRecord](untypedRecord)
			lastRecordLSN = record.lsn
			recordLocation := common.LogRecordLocInfo{
				Lsn:      record.lsn,
				Location: iter.Location(),
			}
			assert.Assert(ATT.Insert(
				record.txnID,
				tag,
				NewATTEntry(TxnStatusUndo, recordLocation),
			), "Found a `begin` record for already running transaction. TransactionID: %d", record.txnID)
		case TypeInsert:
			record := assert.Cast[InsertLogRecord](untypedRecord)
			lastRecordLSN = record.lsn
			recordLocation := common.LogRecordLocInfo{
				Lsn:      record.lsn,
				Location: iter.Location(),
			}

			ATT.Insert(
				record.txnID,
				tag,
				NewATTEntry(TxnStatusUndo, recordLocation),
			)

			pageID := record.modifiedRecordID.PageIdentity()
			_, alreadyExists := DPT[pageID]
			if !alreadyExists {
				DPT[pageID] = recordLocation
			}
		case TypeUpdate:
			record := assert.Cast[UpdateLogRecord](untypedRecord)
			lastRecordLSN = record.lsn
			recordLocation := common.LogRecordLocInfo{
				Lsn:      record.lsn,
				Location: iter.Location(),
			}

			ATT.Insert(
				record.txnID,
				tag,
				NewATTEntry(TxnStatusUndo, recordLocation),
			)

			pageID := record.modifiedRecordID.PageIdentity()
			_, alreadyExists := DPT[pageID]
			if !alreadyExists {
				DPT[pageID] = recordLocation
			}
		case TypeDelete:
			record := assert.Cast[DeleteLogRecord](untypedRecord)
			lastRecordLSN = record.lsn
			recordLocation := common.LogRecordLocInfo{
				Lsn:      record.lsn,
				Location: iter.Location(),
			}

			ATT.Insert(
				record.txnID,
				tag,
				NewATTEntry(TxnStatusUndo, recordLocation),
			)

			pageID := record.modifiedRecordID.PageIdentity()
			_, alreadyExists := DPT[pageID]
			if !alreadyExists {
				DPT[pageID] = recordLocation
			}
		case TypeCommit:
			record := assert.Cast[CommitLogRecord](untypedRecord)
			lastRecordLSN = record.lsn
			recordLocation := common.LogRecordLocInfo{
				Lsn:      record.lsn,
				Location: iter.Location(),
			}
			ATT.Insert(
				record.txnID,
				tag,
				NewATTEntry(TxnStatusCommit, recordLocation),
			)
		case TypeAbort:
			record := assert.Cast[AbortLogRecord](untypedRecord)
			lastRecordLSN = record.lsn
			recordLocation := common.LogRecordLocInfo{
				Lsn:      record.lsn,
				Location: iter.Location(),
			}
			ATT.Insert(
				record.txnID,
				tag,
				NewATTEntry(TxnStatusUndo, recordLocation),
			)
		case TypeTxnEnd:
			record := assert.Cast[TxnEndLogRecord](untypedRecord)
			lastRecordLSN = record.lsn
			delete(ATT.table, record.txnID)
		case TypeCheckpointBegin:
			record := assert.Cast[CheckpointBeginLogRecord](untypedRecord)
			lastRecordLSN = record.lsn
		case TypeCheckpointEnd:
			record := assert.Cast[CheckpointEndLogRecord](
				untypedRecord,
			)
			lastRecordLSN = record.lsn

			for txnID, logInfo := range record.activeTransactions {
				ATT.Insert(txnID, TypeBegin, NewATTEntry(
					TxnStatusUndo,
					logInfo,
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
			for pageInfo, firstLogInfo := range record.dirtyPageTable {
				if _, alreadyExists := DPT[pageInfo]; !alreadyExists {
					DPT[pageInfo] = firstLogInfo
				}
			}
		case TypeCompensation:
			record := assert.Cast[CompensationLogRecord](untypedRecord)
			lastRecordLSN = record.lsn
			recordLocation := common.LogRecordLocInfo{
				Lsn:      record.lsn,
				Location: iter.Location(),
			}

			ATT.Insert(
				record.txnID,
				tag,
				NewATTEntry(TxnStatusUndo, recordLocation),
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
		assert.NoError(err)

		if !success {
			break
		}
	}

	return lastRecordLSN, ATT, DPT
}

func (l *txnLogger) recoverPrepareCLRs(
	ATT ActiveTransactionsTable,
	DPT map[common.PageIdentity]common.LogRecordLocInfo,
) common.FileLocation {
	earliestLogLocation := l.masterPage.GetCheckpointLocation()
	if earliestLogLocation.Lsn == common.NilLSN {
		earliestLogLocation.Lsn = common.LSN(math.MaxUint64)
	}

	for _, entry := range ATT.table {
		if entry.status != TxnStatusUndo {
			continue
		}

		recordLocation := entry.logLocationInfo
		lastInsertedRecordLocation := recordLocation
		clrsFound := 0
	outer:
		for {
			tag, record, err := l.readLogRecord(recordLocation.Location)
			assert.NoError(err)
			switch tag {
			case TypeBegin:
				record := assert.Cast[BeginLogRecord](record)

				if earliestLogLocation.Lsn > record.lsn {
					earliestLogLocation = recordLocation
				}
				assert.Assert(clrsFound == 0, "CLRs aren't balanced out")

				_, err := l.AppendTxnEnd(record.txnID, entry.logLocationInfo)
				assert.NoError(err)
				break outer
			case TypeInsert:
				record := assert.Cast[InsertLogRecord](record)

				DPT[record.modifiedRecordID.PageIdentity()] = recordLocation
				recordLocation = record.parentLogLocation
				if clrsFound > 0 {
					clrsFound--
					continue
				}
				_, lastInsertedRecordLocation, err = loggerUndoRecord(l, &record, lastInsertedRecordLocation)
				assert.NoError(err)
			case TypeUpdate:
				record := assert.Cast[UpdateLogRecord](record)

				DPT[record.modifiedRecordID.PageIdentity()] = recordLocation
				recordLocation = record.parentLogLocation
				if clrsFound > 0 {
					clrsFound--
					continue
				}
				_, lastInsertedRecordLocation, err = loggerUndoRecord(l, &record, lastInsertedRecordLocation)
				assert.NoError(err)
			case TypeDelete:
				record := assert.Cast[DeleteLogRecord](record)

				DPT[record.modifiedRecordID.PageIdentity()] = recordLocation
				recordLocation = record.parentLogLocation
				if clrsFound > 0 {
					clrsFound--
					continue
				}
				_, lastInsertedRecordLocation, err = loggerUndoRecord(l, &record, lastInsertedRecordLocation)
				assert.NoError(err)
			case TypeCommit:
				_ = assert.Cast[CommitLogRecord](record)

				assert.Assert(clrsFound == 0, "found CLRs for a commited txn")
				break outer
			case TypeAbort:
				record := assert.Cast[AbortLogRecord](record)
				recordLocation = record.parentLogLocation
			case TypeTxnEnd:
				_ = assert.Cast[TxnEndLogRecord](record)
				assert.Assert(tag != TypeTxnEnd, "unreachable: ATT shouldn't have log records with TxnEnd logs")
			case TypeCompensation:
				record := assert.Cast[CompensationLogRecord](record)
				clrsFound++
				recordLocation = record.parentLogLocation
			case TypeCheckpointBegin:
				_ = assert.Cast[CheckpointBeginLogRecord](record)
				assert.Assert(tag != TypeCheckpointBegin, "unreachable: ATT shouldn't have CheckpointBegin records")
			case TypeCheckpointEnd:
				_ = assert.Cast[CheckpointEndLogRecord](record)
				assert.Assert(tag != TypeCheckpointEnd, "unreachable: ATT shouldn't have CheckpointBegin records")
			default:
				assert.Assert(false, "unexpected record type: %d", tag)
				panic("unreachable")
			}
		}
	}

	return earliestLogLocation.Location
}

func (l *txnLogger) recoverRedo(earliestLog common.FileLocation) {
	iter, err := l.iter(earliestLog)
	assert.NoError(err)

	for {
		tag, record, err := iter.ReadRecord()
		assert.NoError(err)

		switch tag {
		case TypeInsert:
			record := assert.Cast[InsertLogRecord](record)
			func() {
				modifiedPage, err := l.pool.GetPage(record.modifiedRecordID.PageIdentity())
				assert.NoError(err)
				defer l.pool.Unpin(record.modifiedRecordID.PageIdentity())

				if record.modifiedRecordID.SlotNum >= modifiedPage.NumSlots() {
					assert.Assert(
						record.modifiedRecordID.SlotNum == modifiedPage.NumSlots(),
						"don't know how to recover when slotNum > numSlots. SlotNum: %d, NumSlots: %d",
						record.modifiedRecordID.SlotNum,
						modifiedPage.NumSlots(),
					)

					assert.NoError(l.pool.WithMarkDirty(
						common.NilTxnID,
						record.modifiedRecordID.PageIdentity(),
						modifiedPage,
						func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
							slotOpt := lockedPage.UnsafeInsertNoLogs(record.value)
							assert.Assert(slotOpt.IsSome())
							assert.Assert(slotOpt.Unwrap() == record.modifiedRecordID.SlotNum)
							return common.NewNilLogRecordLocation(), nil
						},
					))
				} else {
					assert.NoError(l.pool.WithMarkDirty(
						common.NilTxnID,
						record.modifiedRecordID.PageIdentity(),
						modifiedPage,
						func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
							lockedPage.UnsafeOverrideSlotStatus(
								record.modifiedRecordID.SlotNum,
								page.SlotStatusInserted,
							)
							lockedPage.UnsafeUpdateNoLogs(record.modifiedRecordID.SlotNum, record.value)
							return common.NewNilLogRecordLocation(), nil
						},
					))
				}
			}()
		case TypeUpdate:
			record := assert.Cast[UpdateLogRecord](record)
			func() {
				pageIdent := record.modifiedRecordID.PageIdentity()
				modifiedPage, err := l.pool.GetPageNoCreate(pageIdent)
				defer l.pool.Unpin(pageIdent)

				assert.NoError(err)
				assert.NoError(l.pool.WithMarkDirty(
					common.NilTxnID,
					pageIdent,
					modifiedPage,
					func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
						lockedPage.UnsafeUpdateNoLogs(
							record.modifiedRecordID.SlotNum,
							record.afterValue,
						)
						return common.NewNilLogRecordLocation(), nil
					},
				))
			}()
		case TypeDelete:
			record := assert.Cast[DeleteLogRecord](record)
			func() {
				pageIdent := record.modifiedRecordID.PageIdentity()
				modifiedPage, err := l.pool.GetPageNoCreate(pageIdent)
				assert.NoError(err)
				defer l.pool.Unpin(pageIdent)

				assert.NoError(l.pool.WithMarkDirty(
					common.NilTxnID,
					pageIdent,
					modifiedPage,
					func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
						lockedPage.UnsafeOverrideSlotStatus(
							record.modifiedRecordID.SlotNum,
							page.SlotStatusDeleted,
						)
						return common.NewNilLogRecordLocation(), nil
					},
				))
			}()
		case TypeCompensation:
			record := assert.Cast[CompensationLogRecord](record)
			assert.NoError(l.activateCLR(&record))
		}

		success, err := iter.MoveForward()
		assert.NoError(err)

		if !success {
			break
		}
	}

	l.curPage = iter.PageID()
	l.firstDirtyPage = iter.PageID()
}

func (l *txnLogger) readLogRecord(
	recordLocation common.FileLocation,
) (tag LogRecordTypeTag, r any, err error) {
	pageIdent := common.PageIdentity{
		FileID: l.logfileID,
		PageID: recordLocation.PageID,
	}
	page, err := l.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return TypeUnknown, nil, err
	}

	defer func() { l.pool.Unpin(pageIdent) }()

	if err != nil {
		return TypeUnknown, nil, err
	}

	record := page.LockedRead(recordLocation.SlotNum)

	tag, r, err = parseLogRecord(record)
	return tag, r, err
}

func (lockedLogger *txnLogger) writeLogRecordAssumePoolLocked(
	serializedRecord []byte,
) (common.FileLocation, error) {
	pageInfo := common.PageIdentity{
		FileID: lockedLogger.logfileID,
		PageID: lockedLogger.curPage,
	}

	p, err := lockedLogger.pool.GetPageAssumeLocked(pageInfo)
	if err != nil {
		return common.FileLocation{}, err
	}

	p.Lock()
	slotNumberOpt := p.UnsafeInsertNoLogs(serializedRecord)
	lockedLogger.pool.MarkDirtyNoLogsAssumeLocked(pageInfo)
	p.Unlock()
	lockedLogger.pool.UnpinAssumeLocked(pageInfo)

	if slotNumberOpt.IsSome() {
		slotNumber := slotNumberOpt.Unwrap()
		loc := common.FileLocation{
			PageID:  lockedLogger.curPage,
			SlotNum: slotNumber,
		}
		return loc, nil
	}

	lockedLogger.curPage++
	pageInfo.PageID++

	p, err = lockedLogger.pool.GetPageAssumeLocked(pageInfo)
	if err != nil {
		return common.FileLocation{}, err
	}

	p.Lock()
	slotNumberOpt = p.UnsafeInsertNoLogs(serializedRecord)
	assert.Assert(
		slotNumberOpt.IsSome(),
		"impossible, because (1) the logger is locked [no concurrent writes are possible] "+
			"and (2) the newly allocated page should be empty",
	)
	lockedLogger.pool.MarkDirtyNoLogsAssumeLocked(pageInfo)
	p.Unlock()
	lockedLogger.pool.UnpinAssumeLocked(pageInfo)

	loc := common.FileLocation{
		PageID:  lockedLogger.curPage,
		SlotNum: slotNumberOpt.Unwrap(),
	}
	return loc, err
}

func (lockedLogger *txnLogger) newLSN() common.LSN {
	lockedLogger.logRecordsCount++
	lsn := common.LSN(lockedLogger.logRecordsCount)
	return lsn
}

func marshalRecordAndWriteAssumePoolLocked[T LogRecord](
	lockedLogger *txnLogger,
	record T,
) (common.LogRecordLocInfo, error) {
	bytes, err := record.MarshalBinary()
	if err != nil {
		return common.LogRecordLocInfo{}, err
	}

	loc, err := lockedLogger.writeLogRecordAssumePoolLocked(bytes)
	if err != nil {
		return common.LogRecordLocInfo{}, err
	}

	logInfo := common.LogRecordLocInfo{
		Lsn:      record.LSN(),
		Location: loc,
	}

	return logInfo, nil
}

func loggerUndoRecord[T RevertableLogRecord](
	l *txnLogger,
	record T,
	parentLocation common.LogRecordLocInfo,
) (*CompensationLogRecord, common.LogRecordLocInfo, error) {
	var clr CompensationLogRecord
	location, err := l.pool.WithMarkDirtyLogPage(func() (common.LogRecordLocInfo, error) {
		l.seqMu.Lock()
		defer l.seqMu.Unlock()

		clr = record.Undo(
			l.newLSN(),
			parentLocation,
		)

		location, err := marshalRecordAndWriteAssumePoolLocked(l, &clr)
		if err != nil {
			return common.LogRecordLocInfo{}, err
		}
		return location, nil
	})

	return &clr, location, err
}

func (l *txnLogger) AppendBegin(
	TransactionID common.TxnID,
) (common.LogRecordLocInfo, error) {
	return l.pool.WithMarkDirtyLogPage(
		func() (common.LogRecordLocInfo, error) {
			l.seqMu.Lock()
			defer l.seqMu.Unlock()

			r := NewBeginLogRecord(l.newLSN(), TransactionID)
			return marshalRecordAndWriteAssumePoolLocked(l, &r)
		},
	)
}

func (l *txnLogger) AppendUpdate(
	txnID common.TxnID,
	prevLog common.LogRecordLocInfo,
	recordID common.RecordID,
	beforeValue []byte,
	afterValue []byte,
) (common.LogRecordLocInfo, error) {
	l.seqMu.Lock()
	defer l.seqMu.Unlock()

	r := NewUpdateLogRecord(
		l.newLSN(),
		txnID,
		prevLog,
		recordID,
		beforeValue,
		afterValue,
	)
	return marshalRecordAndWriteAssumePoolLocked(l, &r)
}

func (l *txnLogger) AppendInsert(
	txnID common.TxnID,
	prevLog common.LogRecordLocInfo,
	recordID common.RecordID,
	value []byte,
) (common.LogRecordLocInfo, error) {
	l.seqMu.Lock()
	defer l.seqMu.Unlock()

	r := NewInsertLogRecord(
		l.newLSN(),
		txnID,
		prevLog,
		recordID,
		value,
	)
	return marshalRecordAndWriteAssumePoolLocked(l, &r)
}

func (l *txnLogger) AppendDelete(
	txnID common.TxnID,
	prevLog common.LogRecordLocInfo,
	recordID common.RecordID,
) (common.LogRecordLocInfo, error) {
	l.seqMu.Lock()
	defer l.seqMu.Unlock()

	r := NewDeleteLogRecord(
		l.newLSN(),
		txnID,
		prevLog,
		recordID,
	)
	return marshalRecordAndWriteAssumePoolLocked(l, &r)
}

func (l *txnLogger) AppendCommit(
	txnID common.TxnID,
	prevLog common.LogRecordLocInfo,
) (common.LogRecordLocInfo, error) {
	loc, err := l.pool.WithMarkDirtyLogPage(
		func() (common.LogRecordLocInfo, error) {
			l.seqMu.Lock()

			r := NewCommitLogRecord(l.newLSN(), txnID, prevLog)
			logInfo, err := marshalRecordAndWriteAssumePoolLocked(l, &r)
			if err != nil {
				l.seqMu.Unlock()
				return common.NewNilLogRecordLocation(), err
			}

			l.seqMu.Unlock()
			return logInfo, l.pool.FlushLogs()
		},
	)
	return loc, err
}

func (l *txnLogger) AppendAbort(
	TransactionID common.TxnID,
	prevLog common.LogRecordLocInfo,
) (common.LogRecordLocInfo, error) {
	loc, err := l.pool.WithMarkDirtyLogPage(
		func() (common.LogRecordLocInfo, error) {
			l.seqMu.Lock()
			defer l.seqMu.Unlock()

			r := NewAbortLogRecord(l.newLSN(), TransactionID, prevLog)
			return marshalRecordAndWriteAssumePoolLocked(l, &r)
		},
	)
	return loc, err
}

func (l *txnLogger) AppendTxnEnd(
	TransactionID common.TxnID,
	prevLog common.LogRecordLocInfo,
) (common.LogRecordLocInfo, error) {
	loc, err := l.pool.WithMarkDirtyLogPage(
		func() (common.LogRecordLocInfo, error) {
			l.seqMu.Lock()
			defer l.seqMu.Unlock()

			r := NewTxnEndLogRecord(l.newLSN(), TransactionID, prevLog)
			return marshalRecordAndWriteAssumePoolLocked(l, &r)
		},
	)
	return loc, err
}

func (l *txnLogger) AppendCheckpointBegin() error {
	_, err := l.pool.WithMarkDirtyLogPage(
		func() (common.LogRecordLocInfo, error) {
			l.seqMu.Lock()
			defer l.seqMu.Unlock()

			r := NewCheckpointBegin(l.newLSN())
			return marshalRecordAndWriteAssumePoolLocked(l, &r)
		},
	)
	return err
}

func (l *txnLogger) AppendCheckpointEnd(
	activeTransacitons map[common.TxnID]common.LogRecordLocInfo,
	dirtyPageTable map[common.PageIdentity]common.LogRecordLocInfo,
) error {
	_, err := l.pool.WithMarkDirtyLogPage(
		func() (common.LogRecordLocInfo, error) {
			l.seqMu.Lock()
			defer l.seqMu.Unlock()

			r := NewCheckpointEnd(l.newLSN(), activeTransacitons, dirtyPageTable)
			return marshalRecordAndWriteAssumePoolLocked(l, &r)
		},
	)
	return err
}

func (l *txnLogger) activateCLR(record *CompensationLogRecord) error {
	pageID := record.modifiedRecordID.PageIdentity()
	pg, err := l.pool.GetPageNoCreate(pageID)
	if err != nil {
		return err
	}
	defer l.pool.Unpin(pageID)

	return l.pool.WithMarkDirty(
		common.NilTxnID,
		pageID,
		pg,
		func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
			switch record.clrType {
			case CLRtypeInsert:
				lockedPage.UndoInsert(record.modifiedRecordID.SlotNum)
			case CLRtypeUpdate:
				lockedPage.UnsafeUpdateNoLogs(record.modifiedRecordID.SlotNum, record.afterValue)
			case CLRtypeDelete:
				lockedPage.UndoDelete(record.modifiedRecordID.SlotNum)
			}
			return common.NewNilLogRecordLocation(), nil
		},
	)
}

func (l *txnLogger) Rollback(abortLogRecord common.LogRecordLocInfo) {
	assert.Assert(!abortLogRecord.IsNil(), "nil log record")

	_, abordRecord, err := l.readLogRecord(abortLogRecord.Location)
	assert.NoError(err)

	record := assert.Cast[AbortLogRecord](abordRecord)
	revertingRecordlocation := record.parentLogLocation
	lastInsertedRecordLocation := abortLogRecord

	clrsFound := 0
outer:
	for {
		tag, record, err := l.readLogRecord(revertingRecordlocation.Location)
		assert.NoError(err)
		switch tag {
		case TypeBegin:
			record := assert.Cast[BeginLogRecord](record)
			assert.Assert(clrsFound == 0, "CLRs aren't balanced out")
			_, err := l.AppendTxnEnd(record.txnID, abortLogRecord)
			assert.NoError(err)
			break outer
		case TypeInsert:
			record := assert.Cast[InsertLogRecord](record)

			revertingRecordlocation = record.parentLogLocation
			if clrsFound > 0 {
				clrsFound--
				continue
			}

			var clr *CompensationLogRecord
			clr, lastInsertedRecordLocation, err = loggerUndoRecord(l, &record, lastInsertedRecordLocation)
			assert.NoError(err)
			assert.NoError(l.activateCLR(clr))
		case TypeDelete:
			record := assert.Cast[DeleteLogRecord](record)

			revertingRecordlocation = record.parentLogLocation
			if clrsFound > 0 {
				clrsFound--
				continue
			}

			var clr *CompensationLogRecord
			clr, lastInsertedRecordLocation, err = loggerUndoRecord(l, &record, lastInsertedRecordLocation)
			assert.NoError(err)
			assert.NoError(l.activateCLR(clr))
		case TypeUpdate:
			record := assert.Cast[UpdateLogRecord](record)

			revertingRecordlocation = record.parentLogLocation
			if clrsFound > 0 {
				clrsFound--
				continue
			}

			var clr *CompensationLogRecord
			clr, lastInsertedRecordLocation, err = loggerUndoRecord(l, &record, lastInsertedRecordLocation)
			assert.NoError(err)
			assert.NoError(l.activateCLR(clr))
		case TypeCommit:
			_ = assert.Cast[CommitLogRecord](record)
			assert.Assert(clrsFound == 0, "found CLRs for a commited txn")
			assert.Assert(tag != TypeCommit, "cannot rollback a commited txn")
		case TypeAbort:
			_ = assert.Cast[AbortLogRecord](record)
			assert.Assert(
				tag != TypeAbort,
				"found multiple abort messages",
			)
		case TypeTxnEnd:
			_ = assert.Cast[TxnEndLogRecord](record)
			assert.Assert(tag != TypeTxnEnd, "cannot rollback a commited txn")
		case TypeCompensation:
			record := assert.Cast[CompensationLogRecord](record)
			assert.NoError(l.activateCLR(&record))
			clrsFound++
			revertingRecordlocation = record.parentLogLocation
		case TypeCheckpointBegin:
			_ = assert.Cast[CheckpointBeginLogRecord](record)
			assert.Assert(
				tag != TypeCheckpointBegin,
				"unreachable: ATT shouldn't have CheckpointBegin records",
			)
		case TypeCheckpointEnd:
			_ = assert.Cast[CheckpointEndLogRecord](record)
			assert.Assert(
				tag != TypeCheckpointEnd,
				"unexpected record type: CheckpointEndLogRecord",
			)
		default:
			assert.Assert(false, "unexpected record type: %d", tag)
			panic("unreachable")
		}
	}
}

func (l *txnLoggerWithContext) GetTxnID() common.TxnID {
	return l.txnID
}

func (l *txnLoggerWithContext) AppendBegin() error {
	loc, err := l.logger.AppendBegin(l.txnID)
	if err != nil {
		return err
	}
	l.lastLogRecordLocation = loc
	return nil
}

func (lockedLogger *txnLoggerWithContext) AppendInsert(
	recordID common.RecordID,
	value []byte,
) (common.LogRecordLocInfo, error) {
	loc, err := lockedLogger.logger.AppendInsert(
		lockedLogger.txnID,
		lockedLogger.lastLogRecordLocation,
		recordID,
		value,
	)
	if err != nil {
		return common.NewNilLogRecordLocation(), err
	}
	lockedLogger.lastLogRecordLocation = loc
	return lockedLogger.lastLogRecordLocation, nil
}

func (lockedLogger *txnLoggerWithContext) AppendUpdate(
	recordID common.RecordID,
	before []byte,
	after []byte,
) (common.LogRecordLocInfo, error) {
	loc, err := lockedLogger.logger.AppendUpdate(
		lockedLogger.txnID,
		lockedLogger.lastLogRecordLocation,
		recordID,
		before,
		after,
	)
	if err != nil {
		return common.NewNilLogRecordLocation(), err
	}
	lockedLogger.lastLogRecordLocation = loc
	return lockedLogger.lastLogRecordLocation, nil
}

func (lockedLogger *txnLoggerWithContext) AppendDelete(
	recordID common.RecordID,
) (common.LogRecordLocInfo, error) {
	loc, err := lockedLogger.logger.AppendDelete(
		lockedLogger.txnID,
		lockedLogger.lastLogRecordLocation,
		recordID,
	)
	if err != nil {
		return common.NewNilLogRecordLocation(), err
	}
	lockedLogger.lastLogRecordLocation = loc
	return lockedLogger.lastLogRecordLocation, nil
}

func (l *txnLoggerWithContext) AppendCommit() error {
	loc, err := l.logger.AppendCommit(
		l.txnID,
		l.lastLogRecordLocation,
	)
	if err != nil {
		return err
	}
	l.lastLogRecordLocation = loc
	return nil
}

func (l *txnLoggerWithContext) AppendAbort() error {
	loc, err := l.logger.AppendAbort(
		l.txnID,
		l.lastLogRecordLocation,
	)
	if err != nil {
		return err
	}
	l.lastLogRecordLocation = loc
	return nil
}

func (l *txnLoggerWithContext) AppendTxnEnd() error {
	loc, err := l.logger.AppendTxnEnd(
		l.txnID,
		l.lastLogRecordLocation,
	)
	if err != nil {
		return err
	}
	l.lastLogRecordLocation = loc
	return nil
}

func (l *txnLoggerWithContext) Rollback() {
	l.logger.Rollback(l.lastLogRecordLocation)
}
