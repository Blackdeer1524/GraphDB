package recovery

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
)

type loggerInfoPage page.SlottedPage

const (
	loggerMasterRecordSlot = iota
	loggerLastLocationSlot
)

const masterRecordPage = 0

func (p *loggerInfoPage) GetMasterRecord() common.LSN {
	o := (*page.SlottedPage)(p)
	return utils.FromBytes[common.LSN](o.Read(loggerMasterRecordSlot))
}

func (p *loggerInfoPage) GetLastLocation() common.LogRecordLocInfo {
	o := (*page.SlottedPage)(p)
	lsn := utils.FromBytes[common.LSN](o.Read(loggerMasterRecordSlot))
	var loc common.FileLocation
	assert.NoError(page.Get(o, loggerLastLocationSlot, &loc))
	return common.LogRecordLocInfo{
		Lsn:      lsn,
		Location: loc,
	}
}

func (p *loggerInfoPage) setInfo(newInfo common.LogRecordLocInfo) {
	o := (*page.SlottedPage)(p)
	o.Lock()
	defer o.Unlock()

	oldValue := utils.FromBytes[common.LSN]((o.Read(loggerMasterRecordSlot)))
	if oldValue >= newInfo.Lsn {
		return
	}
	o.Update(loggerMasterRecordSlot, utils.ToBytes[common.LSN](newInfo.Lsn))

	data, err := newInfo.Location.MarshalBinary()
	assert.NoError(err)
	o.Update(loggerLastLocationSlot, data)
}

func (p *loggerInfoPage) Setup() {
	o := (*page.SlottedPage)(p)
	o.Clear()

	slotOpt := o.Insert(utils.ToBytes[common.LSN](0))
	assert.Assert(slotOpt.IsSome())
	assert.Assert(slotOpt.Unwrap() == loggerMasterRecordSlot)

	dummyRecord := common.FileLocation{
		PageID:  1,
		SlotNum: 0,
	}
	slotOpt = page.InsertSerializable(o, &dummyRecord)
	assert.Assert(slotOpt.IsSome())
	assert.Assert(slotOpt.Unwrap() == loggerLastLocationSlot)
}

type txnLogger struct {
	pool       bufferpool.BufferPool
	logfileID  common.FileID
	masterPage *loggerInfoPage

	// ================
	// лок на запись логов. Нужно для четкой упорядоченности
	// номеров записей и записей на диск
	mu                 sync.Mutex
	logRecordsCount    uint64
	lastRecordLocation common.FileLocation
	lastFlushedPage    common.PageID
	// ================

	getActiveTransactions func() []common.TxnID // Прийдет из лок менеджера
}

func (l *txnLogger) SetActiveTransactionsGetter(getter func() []common.TxnID) {
	l.getActiveTransactions = getter
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
		pool:      pool,
		logfileID: logFileID,
		getActiveTransactions: func() []common.TxnID {
			panic("TODO")
		},
	}

	// this will load master log record's page into memory
	// note that we don't call `Unpin()`. We are going to need this
	// page during replacement.
	var err error
	pg, err := pool.GetPageNoCreate(common.PageIdentity{
		FileID: logFileID,
		PageID: masterRecordPage,
	})

	if errors.Is(err, bufferpool.ErrNoSuchPage) {
		pg, err = pool.GetPage(common.PageIdentity{
			FileID: logFileID,
			PageID: masterRecordPage,
		})
		assert.NoError(err)
		l.masterPage = (*loggerInfoPage)(pg)
		l.masterPage.Setup()
	} else {
		assert.NoError(err)
		l.masterPage = (*loggerInfoPage)(pg)
	}

	info := l.masterPage.GetLastLocation()
	l.logRecordsCount = uint64(info.Lsn)
	l.lastRecordLocation = info.Location
	l.lastFlushedPage = info.Location.PageID

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

type DummyLoggerWithContext struct{}

var dummyLogger DummyLoggerWithContext = DummyLoggerWithContext{}

func NoLogs() *DummyLoggerWithContext {
	return &dummyLogger
}

var (
	_ common.ITxnLogger            = &txnLogger{}
	_ common.ITxnLoggerWithContext = &txnLoggerWithContext{}
	_ common.ITxnLoggerWithContext = &DummyLoggerWithContext{}
)

func (l *txnLogger) WithContext(txnID common.TxnID) *txnLoggerWithContext {
	return newTxnLoggerWithContext(l, txnID)
}

func (l *txnLogger) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	curLocation := l.lastRecordLocation
	currentRecordsCount := l.logRecordsCount

	for i := l.lastFlushedPage; i <= curLocation.PageID; i++ {
		err := l.pool.FlushPage(common.PageIdentity{
			FileID: l.logfileID,
			PageID: i,
		})

		if err != nil && !errors.Is(err, bufferpool.ErrNoSuchPage) {
			return err
		}
	}

	l.masterPage.setInfo(common.LogRecordLocInfo{
		Lsn:      common.LSN(currentRecordsCount),
		Location: curLocation,
	})
	err := l.pool.FlushPage(common.PageIdentity{
		FileID: l.logfileID,
		PageID: masterRecordPage,
	})

	if err != nil {
		return err
	}

	l.lastFlushedPage = curLocation.PageID
	return nil
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

func (l *txnLogger) Recover(checkpointLocation common.FileLocation) {
	ATT, DPT := l.recoverAnalyze(checkpointLocation)
	earliestLog := l.recoverPrepareCLRs(ATT, DPT)
	l.recoverRedo(earliestLog.Location)
}

func (l *txnLogger) recoverAnalyze(
	checkpointLocation common.FileLocation,
) (ActiveTransactionsTable, map[common.PageIdentity]common.LogRecordLocInfo) {
	iter, err := l.iter(common.FileLocation{
		PageID:  checkpointLocation.PageID,
		SlotNum: checkpointLocation.SlotNum,
	})
	assert.Assert(err == nil, "couldn't recover. reason: %+v", err)

	ATT := NewActiveTransactionsTable()
	DPT := map[common.PageIdentity]common.LogRecordLocInfo{}

	for {
		tag, untypedRecord, err := iter.ReadRecord()
		assert.Assert(err == nil, "couldn't read a record. reason: %+v", err)

		switch tag {
		case TypeBegin:
			record := assert.Cast[BeginLogRecord](untypedRecord)
			assert.Assert(ATT.Insert(
				record.txnID,
				tag,
				NewATTEntry(
					TxnStatusUndo,
					common.LogRecordLocInfo{
						Lsn:      record.lsn,
						Location: iter.Location(),
					}),
			), "Found a `begin` record for the already running transaction. TransactionID: %d", record.txnID)
		case TypeInsert:
			record := assert.Cast[InsertLogRecord](untypedRecord)

			ATT.Insert(
				record.txnID,
				tag,
				NewATTEntry(
					TxnStatusUndo,
					common.LogRecordLocInfo{
						Lsn:      record.lsn,
						Location: iter.Location(),
					}),
			)

			pageID := record.modifiedRecordID.PageIdentity()
			_, alreadyExists := DPT[pageID]
			if !alreadyExists {
				DPT[pageID] = common.LogRecordLocInfo{
					Lsn:      record.lsn,
					Location: iter.Location(),
				}
			}
		case TypeUpdate:
			record := assert.Cast[UpdateLogRecord](untypedRecord)
			recordLocation := common.LogRecordLocInfo{
				Lsn:      record.lsn,
				Location: iter.Location(),
			}

			ATT.Insert(
				record.txnID,
				tag,
				NewATTEntry(
					TxnStatusUndo,
					recordLocation,
				),
			)

			pageID := record.modifiedRecordID.PageIdentity()
			_, alreadyExists := DPT[pageID]
			if !alreadyExists {
				DPT[pageID] = recordLocation
			}
		case TypeDelete:
			record := assert.Cast[DeleteLogRecord](untypedRecord)
			recordLocation := common.LogRecordLocInfo{
				Lsn:      record.lsn,
				Location: iter.Location(),
			}

			ATT.Insert(
				record.txnID,
				tag,
				NewATTEntry(
					TxnStatusUndo,
					recordLocation,
				),
			)

			pageID := record.modifiedRecordID.PageIdentity()
			_, alreadyExists := DPT[pageID]
			if !alreadyExists {
				DPT[pageID] = recordLocation
			}
		case TypeCommit:
			record := assert.Cast[CommitLogRecord](untypedRecord)
			ATT.Insert(
				record.txnID,
				tag,
				NewATTEntry(
					TxnStatusCommit,
					common.LogRecordLocInfo{
						Lsn:      record.lsn,
						Location: iter.Location(),
					}),
			)
		case TypeAbort:
			record := assert.Cast[AbortLogRecord](untypedRecord)

			ATT.Insert(
				record.txnID,
				tag,
				NewATTEntry(
					TxnStatusUndo,
					common.LogRecordLocInfo{
						Lsn:      record.lsn,
						Location: iter.Location(),
					}),
			)
		case TypeTxnEnd:
			record := assert.Cast[TxnEndLogRecord](untypedRecord)
			delete(ATT.table, record.txnID)
		case TypeCheckpointBegin:
			_ = assert.Cast[CheckpointBeginLogRecord](untypedRecord)
		case TypeCheckpointEnd:
			record := assert.Cast[CheckpointEndLogRecord](
				untypedRecord,
			)

			for _, TransactionID := range record.activeTransactions {
				ATT.Insert(TransactionID, TypeBegin, NewATTEntry(
					TxnStatusUndo,
					common.NewNilLogRecordLocation(),
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

			ATT.Insert(
				record.txnID,
				tag,
				NewATTEntry(
					TxnStatusUndo,
					common.LogRecordLocInfo{
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
		assert.NoError(err)

		if !success {
			break
		}
	}

	return ATT, DPT
}

func (l *txnLogger) recoverPrepareCLRs(
	ATT ActiveTransactionsTable,
	DPT map[common.PageIdentity]common.LogRecordLocInfo,
) common.LogRecordLocInfo {
	earliestLogLocation := common.LogRecordLocInfo{
		Lsn:      common.LSN(math.MaxUint64),
		Location: common.FileLocation{},
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

	return earliestLogLocation
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
				modifiedPage, err := l.pool.GetPageNoCreate(
					record.modifiedRecordID.PageIdentity(),
				)
				assert.NoError(err)
				defer func() { l.pool.Unpin(record.modifiedRecordID.PageIdentity()) }()

				modifiedPage.Lock()
				defer modifiedPage.Unlock()

				modifiedPage.UnsafeOverrideSlotStatus(
					record.modifiedRecordID.SlotNum,
					page.SlotStatusInserted,
				)
				slotData := modifiedPage.Read(
					record.modifiedRecordID.SlotNum,
				)

				assert.Assert(
					len(record.value) <= len(slotData),
					"new item len should be at most len of the old one",
				)

				clear(slotData)
				copy(slotData, record.value)
			}()
		case TypeUpdate:
			record := assert.Cast[UpdateLogRecord](record)
			func() {
				modifiedPage, err := l.pool.GetPageNoCreate(
					record.modifiedRecordID.PageIdentity(),
				)
				defer func() { l.pool.Unpin(record.modifiedRecordID.PageIdentity()) }()

				assert.NoError(err)
				modifiedPage.Lock()
				defer modifiedPage.Unlock()

				slotData := modifiedPage.Read(
					record.modifiedRecordID.SlotNum,
				)
				assert.Assert(
					len(record.afterValue) <= len(slotData),
					"new item len should be at most len of the old one",
				)

				clear(slotData)
				copy(slotData, record.afterValue)
			}()
		case TypeDelete:
			record := assert.Cast[DeleteLogRecord](record)
			func() {
				modifiedPage, err := l.pool.GetPageNoCreate(
					record.modifiedRecordID.PageIdentity(),
				)
				assert.NoError(err)
				defer func() { l.pool.Unpin(record.modifiedRecordID.PageIdentity()) }()

				modifiedPage.Lock()
				defer modifiedPage.Unlock()
				modifiedPage.UnsafeOverrideSlotStatus(
					record.modifiedRecordID.SlotNum,
					page.SlotStatusDeleted,
				)
			}()
		case TypeCompensation:
			record := assert.Cast[CompensationLogRecord](record)
			l.activateCLR(&record)
		}

		success, err := iter.MoveForward()
		assert.NoError(err)

		if !success {
			break
		}
	}
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

	page.RLock()
	record := page.Read(recordLocation.SlotNum)
	page.RUnlock()

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
//	common.LogRecordLocationInfo - The location information of the written log
//
// record.
//
//	error - An error if the operation fails, otherwise nil.
func (lockedLogger *txnLogger) writeLogRecord(
	serializedRecord []byte,
) (common.FileLocation, error) {
	pageInfo := common.PageIdentity{
		FileID: lockedLogger.logfileID,
		PageID: lockedLogger.lastRecordLocation.PageID,
	}

	p, err := lockedLogger.pool.GetPage(pageInfo)
	if err != nil {
		return common.FileLocation{}, err
	}
	p.Lock()
	slotNumberOpt := p.Insert(serializedRecord)
	p.Unlock()
	lockedLogger.pool.Unpin(pageInfo)
	if slotNumberOpt.IsSome() {
		slotNumber := slotNumberOpt.Unwrap()
		lockedLogger.lastRecordLocation.SlotNum = slotNumber
		return lockedLogger.lastRecordLocation, err
	}

	lockedLogger.lastRecordLocation.PageID++
	pageInfo.PageID++

	p, err = lockedLogger.pool.GetPage(pageInfo)
	if err != nil {
		return common.FileLocation{}, err
	}

	p.Lock()
	slotNumberOpt = p.Insert(serializedRecord)
	assert.Assert(
		slotNumberOpt.IsSome(),
		"impossible, because (1) the logger is locked [no concurrent writes are possible] "+
			"and (2) the newly allocated page should be empty",
	)
	p.Unlock()

	lockedLogger.pool.Unpin(pageInfo)
	lockedLogger.lastRecordLocation.SlotNum = slotNumberOpt.Unwrap()

	return lockedLogger.lastRecordLocation, err
}

func (lockedLogger *txnLogger) NewLSN() common.LSN {
	lockedLogger.logRecordsCount++
	lsn := common.LSN(lockedLogger.logRecordsCount)
	return lsn
}

func (lockedLogger *txnLogger) GetMasterRecord() common.LSN {
	masterRecordPageIdent := common.PageIdentity{
		FileID: lockedLogger.logfileID,
		PageID: masterRecordPage,
	}
	masterRecordPage, err := lockedLogger.pool.GetPageNoCreate(
		masterRecordPageIdent,
	)
	assert.Assert(
		err != nil,
		"the page should have been loaded into memory on buffer pool init. err: %v",
		err,
	)
	masterRecordPage.Lock()
	masterRecord := utils.FromBytes[common.LSN](
		masterRecordPage.Read(loggerMasterRecordSlot),
	)
	masterRecordPage.Unlock()
	lockedLogger.pool.Unpin(masterRecordPageIdent)
	return masterRecord
}

func marshalRecordAndWrite[T LogRecord](
	lockedLogger *txnLogger,
	record T,
) (common.LogRecordLocInfo, error) {
	bytes, err := record.MarshalBinary()
	if err != nil {
		return common.LogRecordLocInfo{}, err
	}

	loc, err := lockedLogger.writeLogRecord(bytes)
	if err != nil {
		return common.LogRecordLocInfo{}, err
	}

	logInfo := common.LogRecordLocInfo{
		Lsn:      record.LSN(),
		Location: loc,
	}

	return logInfo, nil
}

func (l *txnLogger) AppendBegin(
	TransactionID common.TxnID,
) (common.LogRecordLocInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	r := NewBeginLogRecord(l.NewLSN(), TransactionID)
	return marshalRecordAndWrite(l, &r)
}

func (l *txnLogger) AppendUpdate(
	TransactionID common.TxnID,
	prevLog common.LogRecordLocInfo,
	recordID common.RecordID,
	beforeValue []byte,
	afterValue []byte,
) (common.LogRecordLocInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	r := NewUpdateLogRecord(
		l.NewLSN(),
		TransactionID,
		prevLog,
		recordID,
		beforeValue,
		afterValue,
	)
	return marshalRecordAndWrite(l, &r)
}

func loggerUndoRecord[T RevertableLogRecord](
	l *txnLogger,
	record T,
	parentLocation common.LogRecordLocInfo,
) (*CompensationLogRecord, common.LogRecordLocInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	clr := record.Undo(
		l.NewLSN(),
		parentLocation,
	)
	location, err := marshalRecordAndWrite(l, &clr)
	if err != nil {
		return nil, common.LogRecordLocInfo{}, err
	}

	return &clr, location, nil
}

func (l *txnLogger) AppendInsert(
	txnID common.TxnID,
	prevLog common.LogRecordLocInfo,
	recordID common.RecordID,
	value []byte,
) (common.LogRecordLocInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	r := NewInsertLogRecord(
		l.NewLSN(),
		txnID,
		prevLog,
		recordID,
		value,
	)
	return marshalRecordAndWrite(l, &r)
}

func (l *txnLogger) AppendDelete(
	txnID common.TxnID,
	prevLog common.LogRecordLocInfo,
	recordID common.RecordID,
) (common.LogRecordLocInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	r := NewDeleteLogRecord(
		l.NewLSN(),
		txnID,
		prevLog,
		recordID,
	)
	return marshalRecordAndWrite(l, &r)
}

func (l *txnLogger) AppendCommit(
	txnID common.TxnID,
	prevLog common.LogRecordLocInfo,
) (common.LogRecordLocInfo, error) {
	defer l.Flush()

	l.mu.Lock()
	defer l.mu.Unlock()

	r := NewCommitLogRecord(l.NewLSN(), txnID, prevLog)
	logInfo, err := marshalRecordAndWrite(l, &r)
	if err != nil {
		return common.NewNilLogRecordLocation(), err
	}

	return logInfo, nil
}

func (l *txnLogger) AppendAbort(
	TransactionID common.TxnID,
	prevLog common.LogRecordLocInfo,
) (common.LogRecordLocInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	r := NewAbortLogRecord(l.NewLSN(), TransactionID, prevLog)
	return marshalRecordAndWrite(l, &r)
}

func (l *txnLogger) AppendTxnEnd(
	TransactionID common.TxnID,
	prevLog common.LogRecordLocInfo,
) (common.LogRecordLocInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	r := NewTxnEndLogRecord(l.NewLSN(), TransactionID, prevLog)
	return marshalRecordAndWrite(l, &r)
}

func (l *txnLogger) AppendCheckpointBegin() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	r := NewCheckpointBegin(l.NewLSN())
	_, err := marshalRecordAndWrite(l, &r)
	return err
}

func (l *txnLogger) AppendCheckpointEnd(
	activeTransacitons []common.TxnID,
	dirtyPageTable map[common.PageIdentity]common.LogRecordLocInfo,
) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	r := NewCheckpointEnd(
		l.NewLSN(),
		activeTransacitons,
		dirtyPageTable,
	)
	_, err := marshalRecordAndWrite(l, &r)
	return err
}

func (l *txnLogger) activateCLR(record *CompensationLogRecord) {
	pageID := record.modifiedRecordID.PageIdentity()
	page, err := l.pool.GetPageNoCreate(pageID)
	assert.NoError(err)
	defer func() { l.pool.Unpin(pageID) }()

	page.Lock()
	defer page.Unlock()

	switch record.clrType {
	case CLRtypeInsert:
		page.UndoInsert(record.modifiedRecordID.SlotNum)
	case CLRtypeUpdate:
		page.Update(record.modifiedRecordID.SlotNum, record.afterValue)
	case CLRtypeDelete:
		page.UndoDelete(record.modifiedRecordID.SlotNum)
	}
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
			l.activateCLR(clr)
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
			l.activateCLR(clr)
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
			l.activateCLR(clr)
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
			l.activateCLR(&record)
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

func (l *txnLogger) Checkpoint() {
	err := l.AppendCheckpointBegin()
	assert.NoError(err)
	activeTxns := l.getActiveTransactions()

	err = l.pool.FlushAllPages()
	assert.NoError(err)

	dpt := l.pool.GetDirtyPageTable()
	l.AppendCheckpointEnd(activeTxns, dpt)
}

func (l *DummyLoggerWithContext) AppendBegin() error {
	return nil
}

func (l *DummyLoggerWithContext) AppendDelete(
	recordID common.RecordID,
) (common.LogRecordLocInfo, error) {
	return common.NewNilLogRecordLocation(), nil
}

func (l *DummyLoggerWithContext) AppendInsert(
	recordID common.RecordID,
	value []byte,
) (common.LogRecordLocInfo, error) {
	return common.NewNilLogRecordLocation(), nil
}

func (l *DummyLoggerWithContext) AppendUpdate(
	recordID common.RecordID,
	before []byte,
	after []byte,
) (common.LogRecordLocInfo, error) {
	return common.NewNilLogRecordLocation(), nil
}

func (l *DummyLoggerWithContext) AppendCommit() error {
	return nil
}

func (l *DummyLoggerWithContext) AppendAbort() error {
	return nil
}

func (l *DummyLoggerWithContext) AppendTxnEnd() error {
	return nil
}

func (l *DummyLoggerWithContext) Rollback() {
}

func (l *txnLoggerWithContext) AppendBegin() error {
	loc, err := l.logger.AppendBegin(l.txnID)
	if err != nil {
		return err
	}
	l.lastLogRecordLocation = loc
	return nil
}

func (l *txnLoggerWithContext) AppendDelete(
	recordID common.RecordID,
) (common.LogRecordLocInfo, error) {
	loc, err := l.logger.AppendDelete(
		l.txnID,
		l.lastLogRecordLocation,
		recordID,
	)
	if err != nil {
		return common.NewNilLogRecordLocation(), err
	}
	l.lastLogRecordLocation = loc
	return l.lastLogRecordLocation, nil
}

func (l *txnLoggerWithContext) AppendInsert(
	recordID common.RecordID,
	value []byte,
) (common.LogRecordLocInfo, error) {
	loc, err := l.logger.AppendInsert(
		l.txnID,
		l.lastLogRecordLocation,
		recordID,
		value,
	)
	if err != nil {
		return common.NewNilLogRecordLocation(), err
	}
	l.lastLogRecordLocation = loc
	return l.lastLogRecordLocation, nil
}

func (l *txnLoggerWithContext) AppendUpdate(
	recordID common.RecordID,
	before []byte,
	after []byte,
) (common.LogRecordLocInfo, error) {
	loc, err := l.logger.AppendUpdate(
		l.txnID,
		l.lastLogRecordLocation,
		recordID,
		before,
		after,
	)
	if err != nil {
		return common.NewNilLogRecordLocation(), err
	}
	l.lastLogRecordLocation = loc
	return l.lastLogRecordLocation, nil
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
