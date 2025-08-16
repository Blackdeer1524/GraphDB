package recovery

import (
	"bytes"
	"maps"
	"math"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/optional"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

func TestValidRecovery(t *testing.T) {
	pool := bufferpool.NewBufferPoolMock()
	loggerStart := common.LogRecordLocInfo{
		Lsn:      0,
		Location: common.FileLocation{PageID: 0, SlotNum: 0},
	}
	logger := &TxnLogger{
		pool:            pool,
		logfileID:       1,
		lastLogLocation: loggerStart,
	}

	defer func() {
		if recover() != nil {
			b := &strings.Builder{}
			logger.Dump(loggerStart.Location, b)
			println(b.String())
		}
		assert.NoError(t, pool.EnsureAllPagesUnpinned())
	}()

	dataPageID := common.PageIdentity{FileID: 42, PageID: 123}
	slotNumOpt, err := insertValueNoLogs(t, pool, dataPageID, []byte("bef000"))
	require.True(t, slotNumOpt.IsSome())
	slotNum := slotNumOpt.Unwrap()
	require.NoError(t, err)

	TransactionID := common.TxnID(100)
	before := []byte("before")
	after := []byte("after")

	chain := NewTxnLogChain(logger, TransactionID)
	// Simulate a transaction: Begin -> Insert -> Update -> Commit -> TxnEnd
	chain.Begin().
		Insert(common.RecordID{
			FileID:  dataPageID.FileID,
			PageID:  dataPageID.PageID,
			SlotNum: uint16(slotNum),
		}, before).
		Update(common.RecordID{
			FileID:  dataPageID.FileID,
			PageID:  dataPageID.PageID,
			SlotNum: uint16(slotNum),
		}, before, after).
		Commit().
		TxnEnd()

	err = chain.Err()
	if err != nil {
		t.Fatalf("log record append failed: %v", err)
	}

	// Simulate a crash and recovery
	logger2 := &TxnLogger{
		pool:            pool,
		logfileID:       logger.logfileID,
		lastLogLocation: logger.lastLogLocation,
	}
	checkpoint := common.FileLocation{PageID: 0, SlotNum: 0}
	logger2.Recover(checkpoint)

	// Check that the page contains the "after" value
	p, err := pool.GetPage(dataPageID)
	defer func(pageID common.PageIdentity) { assert.NoError(t, pool.Unpin(pageID)) }(
		dataPageID,
	)

	p.RLock()
	defer p.RUnlock()

	if err != nil {
		t.Fatalf("GetPage failed: %v", err)
	}

	data := p.Read(uint16(slotNum))

	if !bytes.Equal(data[:len(after)], after) {
		t.Errorf(
			"Recovery failed: expected %q, got %q",
			after,
			data[:len(after)],
		)
	}
}

func TestFailedTxn(t *testing.T) {
	pool := bufferpool.NewBufferPoolMock()

	logStart := common.LogRecordLocInfo{
		Lsn:      0,
		Location: common.FileLocation{PageID: 0, SlotNum: 0},
	}
	logger := &TxnLogger{
		pool:            pool,
		logfileID:       1,
		lastLogLocation: logStart,
	}
	pageIdent := common.PageIdentity{FileID: 1, PageID: 42}

	TransactionID := common.TxnID(100)
	before := []byte("before")

	slotNumOpt, err := insertValueNoLogs(t, pool, pageIdent, before)
	require.NoError(t, err, "couldn't insert a record")
	require.True(t, slotNumOpt.IsSome())
	slotNum := slotNumOpt.Unwrap()

	// Simulate a transaction: **Begin -> Insert -> CRASH**
	chain := NewTxnLogChain(logger, TransactionID).
		Begin().
		Insert(common.RecordID{
			FileID:  pageIdent.FileID,
			PageID:  pageIdent.PageID,
			SlotNum: slotNum,
		}, before)
	require.Nil(t, chain.Err(), "log record append failed")

	// Simulate a crash and recovery
	checkpoint := logStart.Location
	logger.Recover(checkpoint)

	// BEGIN
	iter, err := logger.iter(logStart.Location)
	require.NoError(t, err, "couldn't create an iterator")
	{
		tag, untypedRecord, err := iter.ReadRecord()
		require.NoError(t, err)
		assertLogRecord(t, tag, untypedRecord, TypeBegin, TransactionID)
	}

	// INSERT
	ok, err := iter.MoveForward()
	require.NoError(t, err, "couldn't move the iterator")
	require.True(t, ok)
	{
		tag, untypedRecord, err := iter.ReadRecord()
		require.NoError(t, err)
		assertLogRecord(t, tag, untypedRecord, TypeInsert, TransactionID)
	}

	// CLR
	ok, err = iter.MoveForward()
	require.NoError(t, err, "couldn't move the iterator")
	require.True(t, ok)
	{
		tag, untypedRecord, err := iter.ReadRecord()
		require.NoError(t, err)
		assertLogRecord(t, tag, untypedRecord, TypeCompensation, TransactionID)
	}

	// TxnEnd
	ok, err = iter.MoveForward()
	require.NoError(t, err, "couldn't move the iterator")
	require.True(t, ok)
	{
		tag, untypedRecord, err := iter.ReadRecord()
		require.NoError(t, err)
		assertLogRecord(t, tag, untypedRecord, TypeTxnEnd, TransactionID)
	}

	// NOTHING
	ok, err = iter.MoveForward()
	require.NoError(t, err, "couldn't move the iterator")
	require.False(t, ok)
}

func insertValueNoLogs(
	t *testing.T,
	pool bufferpool.BufferPool[*page.SlottedPage],
	pageId common.PageIdentity,
	data []byte,
) (optional.Optional[uint16], error) {
	p, err := pool.GetPage(pageId)
	require.NoError(t, err)
	defer func(pgID common.PageIdentity) { require.NoError(t, pool.Unpin(pgID)) }(
		pageId,
	)

	p.Lock()
	defer p.Unlock()

	insertHandle := p.InsertPrepare(data)
	if insertHandle.IsNone() {
		return optional.None[uint16](), nil
	}

	p.InsertCommit(insertHandle.Unwrap())
	return insertHandle, nil
}

func TestMassiveRecovery(t *testing.T) {
	pool := bufferpool.NewBufferPoolMock()
	defer func() { assert.NoError(t, pool.EnsureAllPagesUnpinned()) }()

	logPageId := common.PageIdentity{
		FileID: 42,
		PageID: 321,
	}

	logger := &TxnLogger{
		pool:            pool,
		mu:              sync.Mutex{},
		logRecordsCount: 0,
		logfileID:       logPageId.FileID,
		lastLogLocation: common.LogRecordLocInfo{
			Lsn: 0,
			Location: common.FileLocation{
				PageID:  logPageId.PageID,
				SlotNum: 0,
			},
		},
		getActiveTransactions: func() []common.TxnID {
			panic("TODO")
		},
	}

	INIT := []byte("init")
	NEW := []byte("new1")
	NEW2 := []byte("1234")

	DataFileID := common.FileID(0)
	dataPageId := common.PageIdentity{
		FileID: DataFileID,
		PageID: 321,
	}
	slot := optional.Some(uint16(0))

	N := 2000
	i := 0

	index2pageID := map[int]common.FileLocation{}

	for i < N {
		succ := func() bool {
			var err error

			slot, err = insertValueNoLogs(t, pool, dataPageId, INIT)
			if slot.IsNone() {
				dataPageId.PageID++
				return false
			}

			require.NoError(t, err)
			return true
		}()

		if succ {
			index2pageID[i] = common.FileLocation{
				PageID:  dataPageId.PageID,
				SlotNum: slot.Unwrap(),
			}
			i++
		}
	}

	left := N - N/10
	inc := N * 6 / 10
	right := (left + inc) % N
	STEP := 5
	require.Equal(
		t,
		inc%STEP,
		0,
		"step must divide inc. otherwise, it would cause an infinite loop",
	)

	TransactionIDCounter := atomic.Uint64{}
	wg := sync.WaitGroup{}
	for i := left; i != right; i = (i + STEP) % N {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			TransactionID := common.TxnID(TransactionIDCounter.Add(1))
			chain := NewTxnLogChain(logger, TransactionID)

			chain.Begin()

			for j := range STEP {
				recordLoc, ok := index2pageID[(i+j)%N]
				require.True(t, ok, "%d", i+j)

				pageID := common.PageIdentity{
					FileID: DataFileID,
					PageID: recordLoc.PageID,
				}

				switch rand.Int() % 2 {
				case 0:
					chain.
						Update(common.RecordID{
							FileID:  pageID.FileID,
							PageID:  pageID.PageID,
							SlotNum: recordLoc.SlotNum,
						}, INIT, NEW).
						Update(common.RecordID{
							FileID:  pageID.FileID,
							PageID:  pageID.PageID,
							SlotNum: recordLoc.SlotNum,
						}, NEW, NEW2)
					func() {
						p, err := pool.GetPageNoCreate(pageID)
						require.NoError(t, err)

						defer func() { require.NoError(t, pool.Unpin(pageID)) }()

						p.Lock()
						defer p.Unlock()

						data := p.Read(recordLoc.SlotNum)

						clear(data)

						if (i+j)%2 != 0 {
							copy(data, NEW)
						} else {
							copy(data, NEW2)
						}
					}()
				case 1:
					chain.Delete(common.RecordID{
						FileID:  pageID.FileID,
						PageID:  pageID.PageID,
						SlotNum: recordLoc.SlotNum,
					})
				}
			}

			require.NoError(t, chain.Err())
		}(i)
	}

	wg.Wait()

	logger.Recover(common.FileLocation{
		PageID:  logPageId.PageID,
		SlotNum: 0,
	})

	for i := range N {
		func() {
			location := index2pageID[i]
			dataPageId := common.PageIdentity{
				FileID: DataFileID,
				PageID: location.PageID,
			}
			p, err := pool.GetPageNoCreate(dataPageId)
			require.NoError(t, err)
			defer func() { require.NoError(t, pool.Unpin(dataPageId)) }()

			p.RLock()
			defer p.RUnlock()

			data := p.Read(location.SlotNum)
			require.Equal(t, len(INIT), len(data))

			for i := range len(INIT) {
				require.Equal(t, INIT[i], data[i])
			}
		}()
	}
}

func assertLogRecord(
	t *testing.T,
	actualTag LogRecordTypeTag,
	untypedRecord any,
	expectedRecordType LogRecordTypeTag,
	expectedTransactionID common.TxnID,
) {
	require.Equal(t, actualTag, expectedRecordType)

	switch actualTag {
	case TypeBegin:
		r, ok := untypedRecord.(BeginLogRecord)
		require.True(t, ok)
		require.Equal(t, expectedTransactionID, r.txnID)
	case TypeUpdate:
		r, ok := untypedRecord.(UpdateLogRecord)
		require.True(t, ok)
		require.Equal(t, expectedTransactionID, r.txnID)
	case TypeInsert:
		r, ok := untypedRecord.(InsertLogRecord)
		require.True(t, ok)
		require.Equal(t, expectedTransactionID, r.txnID)
	case TypeDelete:
		r, ok := untypedRecord.(DeleteLogRecord)
		require.True(t, ok)
		require.Equal(t, expectedTransactionID, r.txnID)
	case TypeCommit:
		r, ok := untypedRecord.(CommitLogRecord)
		require.True(t, ok)
		require.Equal(t, expectedTransactionID, r.txnID)
	case TypeAbort:
		r, ok := untypedRecord.(AbortLogRecord)
		require.True(t, ok)
		require.Equal(t, expectedTransactionID, r.txnID)
	case TypeTxnEnd:
		r, ok := untypedRecord.(TxnEndLogRecord)
		require.True(t, ok)
		require.Equal(t, expectedTransactionID, r.txnID)
	case TypeCompensation:
		r, ok := untypedRecord.(CompensationLogRecord)
		require.True(t, ok)
		require.Equal(t, expectedTransactionID, r.txnID)
	default:
		require.Less(t, actualTag, TypeUnknown)
	}
}

func assertLogRecordWithRetrieval(
	t *testing.T,
	pool bufferpool.BufferPool[*page.SlottedPage],
	recordID common.RecordID,
	expectedRecordType LogRecordTypeTag,
	expectedTransactionID common.TxnID,
) {
	page, err := pool.GetPage(recordID.PageIdentity())
	require.NoError(t, err)
	page.RLock()

	data := page.Read(recordID.SlotNum)

	tag, untypedRecord, err := readLogRecord(data)
	require.NoError(t, err)

	assertLogRecord(
		t,
		tag,
		untypedRecord,
		expectedRecordType,
		expectedTransactionID,
	)

	page.RUnlock()
	require.NoError(t, pool.Unpin(recordID.PageIdentity()))
}

func TestLoggerValidConcurrentWrites(t *testing.T) {
	pool := bufferpool.NewBufferPoolMock()
	defer func() { assert.NoError(t, pool.EnsureAllPagesUnpinned()) }()

	logPageId := common.PageIdentity{
		FileID: 42,
		PageID: 321,
	}

	logger := &TxnLogger{
		pool:            pool,
		mu:              sync.Mutex{},
		logRecordsCount: 0,
		logfileID:       logPageId.FileID,
		lastLogLocation: common.LogRecordLocInfo{
			Lsn: 0,
			Location: common.FileLocation{
				PageID:  logPageId.PageID,
				SlotNum: 0,
			},
		},
		getActiveTransactions: func() []common.TxnID {
			panic("TODO")
		},
	}

	dataPageId := common.PageIdentity{
		FileID: 0,
		PageID: 0,
	}

	waitWg := sync.WaitGroup{}
	barierWg := sync.WaitGroup{}

	OUTER := 100
	INNER := 10

	for i := range OUTER {
		waitWg.Add(1)
		barierWg.Add(1)

		go func(TransactionID common.TxnID) {
			defer waitWg.Done()

			chain := NewTxnLogChain(logger, TransactionID)

			insertLocs := []common.LogRecordLocInfo{}
			updateLocs := []common.LogRecordLocInfo{}
			beginLoc := chain.Begin().Loc()

			for j := range INNER {
				switch rand.Int() % 2 {
				case 0:
					insertLocs = append(
						insertLocs,
						//nolint:gosec
						chain.Insert(
							common.RecordID{
								FileID:  dataPageId.FileID,
								PageID:  dataPageId.PageID,
								SlotNum: uint16(j),
							},
							utils.Uint32ToBytes(uint32(i*INNER+j))).
							Loc(),
					)
				case 1:
					updateLocs = append(
						updateLocs,
						//nolint:gosec
						chain.Update(
							common.RecordID{
								FileID:  dataPageId.FileID,
								PageID:  dataPageId.PageID,
								SlotNum: uint16(j),
							},
							utils.Uint32ToBytes(uint32(i)),
							utils.Uint32ToBytes(uint32(i*INNER+j)),
						).Loc(),
					)
				}
			}

			finishLoc := common.NewNilLogRecordLocation()
			isCommit := rand.Int()%2 != 0

			if isCommit {
				finishLoc = chain.Commit().Loc()
			} else {
				finishLoc = chain.Abort().Loc()
			}

			chain.TxnEnd()
			require.NoError(t, chain.Err())

			barierWg.Done()
			barierWg.Wait()

			assertLogRecordWithRetrieval(
				t,
				logger.pool,
				common.RecordID{
					FileID:  logger.logfileID,
					PageID:  beginLoc.Location.PageID,
					SlotNum: beginLoc.Location.SlotNum,
				},
				TypeBegin,
				TransactionID,
			)

			if isCommit {
				assertLogRecordWithRetrieval(
					t,
					logger.pool,
					common.RecordID{
						FileID:  logger.logfileID,
						PageID:  finishLoc.Location.PageID,
						SlotNum: finishLoc.Location.SlotNum,
					},
					TypeCommit,
					TransactionID,
				)
			} else {
				assertLogRecordWithRetrieval(t,
					logger.pool,
					common.RecordID{
						FileID:  logger.logfileID,
						PageID:  finishLoc.Location.PageID,
						SlotNum: finishLoc.Location.SlotNum,
					},
					TypeAbort,
					TransactionID,
				)
			}

			for _, insert := range insertLocs {
				assertLogRecordWithRetrieval(
					t,
					logger.pool,
					common.RecordID{
						FileID:  logger.logfileID,
						PageID:  insert.Location.PageID,
						SlotNum: insert.Location.SlotNum,
					},
					TypeInsert,
					TransactionID,
				)
			}

			for _, update := range updateLocs {
				assertLogRecordWithRetrieval(
					t,
					logger.pool,
					common.RecordID{
						FileID:  logger.logfileID,
						PageID:  update.Location.PageID,
						SlotNum: update.Location.SlotNum,
					},
					TypeUpdate,
					TransactionID,
				)
			}
		}(common.TxnID(i)) //nolint:gosec
	}

	waitWg.Wait()
}

type KVPair[K comparable, V any] struct {
	key   K
	value V
}

func mapBatch[K comparable, V any](
	m map[K]V,
	batchSize int,
) <-chan []KVPair[K, V] {
	c := make(chan []KVPair[K, V])
	go func() {
		innerC := make(chan KVPair[K, V])
		go func() {
			for k, v := range m {
				innerC <- KVPair[K, V]{key: k, value: v}
			}
			close(innerC)
		}()

		res := make([]KVPair[K, V], 0, batchSize)
	outer:
		for {
			for range batchSize {
				pair, ok := <-innerC
				if !ok {
					break outer
				}
				res = append(res, pair)
			}
			c <- res
			res = make([]KVPair[K, V], 0, batchSize)
		}
		if len(res) > 0 {
			c <- res
		}
		close(c)
	}()
	return c
}

func TestLoggerRollback(t *testing.T) {
	pool := bufferpool.NewBufferPoolMock()
	logPageId := common.PageIdentity{
		FileID: 42,
		PageID: 321,
	}
	logStartLocation := common.FileLocation{
		PageID:  logPageId.PageID,
		SlotNum: 0,
	}

	logger := &TxnLogger{
		pool:            pool,
		mu:              sync.Mutex{},
		logRecordsCount: 0,
		logfileID:       logPageId.FileID,
		lastLogLocation: common.LogRecordLocInfo{
			Lsn:      0,
			Location: logStartLocation,
		},
		getActiveTransactions: func() []common.TxnID {
			panic("TODO")
		},
	}

	defer func() {
		assert.NoError(t, pool.EnsureAllPagesUnpinned())
	}()

	files := []common.FileID{}
	for range 1 {
		for {
			fileID := common.FileID(rand.Uint64() % 1024)
			if fileID == logger.logfileID {
				continue
			}
			files = append(files, fileID)
			break
		}
	}

	recordValues := fillPages(t, logger, math.MaxUint64, 20000, files, 1024)
	require.NoError(t, pool.EnsureAllPagesUnpinned())

	updatedValues := make(map[common.RecordID]uint32, len(recordValues))
	maps.Copy(updatedValues, recordValues)

	locker := txns.NewLocker()

	txnID := atomic.Uint64{}
	wg := sync.WaitGroup{}
	for batch := range mapBatch(recordValues, 300) {
		wg.Add(1)
		go func(batch []KVPair[common.RecordID, uint32]) {
			defer func() {
				r := recover()
				if r != nil {
					b := &strings.Builder{}
					logger.Dump(logStartLocation, b)
					println(b.String())
					panic(r)
				}
			}()
			defer wg.Done()

			txnID := common.TxnID(txnID.Add(1))
			logger := logger.WithContext(txnID)
			require.NoError(t, logger.AppendBegin())

			cLockOpt := locker.LockCatalog(
				txnID,
				txns.GRANULAR_LOCK_INTENTION_EXCLUSIVE,
			)
			if cLockOpt.IsNone() {
				assert.NoError(t, logger.AppendAbort())
				logger.Rollback()
				return
			}
			cLock, cToken := cLockOpt.Unwrap().Destruct()
			<-cLock
			defer locker.Unlock(cToken)

			for j := range len(batch) * 3 / 2 {
				info := batch[j%len(batch)]

				tLockOpt := locker.LockFile(
					cToken,
					common.FileID(info.key.FileID),
					txns.GRANULAR_LOCK_INTENTION_EXCLUSIVE,
				)
				if tLockOpt.IsNone() {
					assert.NoError(t, logger.AppendAbort())
					logger.Rollback()
					return
				}
				tLock, tToken := tLockOpt.Unwrap().Destruct()
				<-tLock

				pLockOpt := locker.LockPage(
					tToken,
					common.PageID(info.key.PageID),
					txns.PAGE_LOCK_EXCLUSIVE,
				)
				if pLockOpt.IsNone() {
					assert.NoError(t, logger.AppendAbort())
					logger.Rollback()
					return
				}
				pLock, _ := pLockOpt.Unwrap().Destruct()
				<-pLock

				newValue := rand.Uint32()
				page, err := pool.GetPageNoCreate(info.key.PageIdentity())
				require.NoError(t, err)
				page.Lock()
				_, err = page.UpdateWithLogs(
					utils.Uint32ToBytes(newValue),
					info.key,
					logger,
				)
				require.NoError(t, err)
				page.Unlock()
				require.NoError(t, pool.Unpin(info.key.PageIdentity()))
			}

			for j := range len(batch) / 3 {
				info := batch[j]

				tLockOpt := locker.LockFile(
					cToken,
					common.FileID(info.key.FileID),
					txns.GRANULAR_LOCK_INTENTION_EXCLUSIVE,
				)
				if tLockOpt.IsNone() {
					assert.NoError(t, logger.AppendAbort())
					logger.Rollback()
					return
				}
				tLock, tToken := tLockOpt.Unwrap().Destruct()
				<-tLock

				pLockOpt := locker.LockPage(
					tToken,
					common.PageID(info.key.PageID),
					txns.PAGE_LOCK_EXCLUSIVE,
				)
				if pLockOpt.IsNone() {
					assert.NoError(t, logger.AppendAbort())
					logger.Rollback()
					return
				}
				pLock, _ := pLockOpt.Unwrap().Destruct()
				<-pLock

				page, err := pool.GetPageNoCreate(info.key.PageIdentity())
				require.NoError(t, err)
				page.Lock()
				_, err = page.DeleteWithLogs(info.key, logger)
				require.NoError(t, err)
				page.Unlock()
				require.NoError(t, pool.Unpin(info.key.PageIdentity()))
			}

			require.NoError(t, logger.AppendAbort())
			logger.Rollback()
		}(batch)
	}
	wg.Wait()

	for k, v := range recordValues {
		func() {
			pageID := common.PageIdentity{
				FileID: k.FileID,
				PageID: k.PageID,
			}
			page, err := pool.GetPageNoCreate(pageID)
			assert.NoError(t, err)
			defer func() { assert.NoError(t, pool.Unpin(pageID)) }()

			data := page.Read(k.SlotNum)
			assert.Equal(t, data, utils.Uint32ToBytes(v))
		}()
	}
}

// fillPages generates test data by filling pages with random values and logging
// the operations. It creates a transaction log chain and inserts random values
// into random pages within the given file IDs.
//
// Parameters:
//   - t: Testing context for assertions
//   - logger: Transaction logger to record operations
//   - txnID: Transaction ID for the operation
//   - length: Number of records to insert
//   - fileIDs: Slice of file IDs to distribute records across
//   - limit: Upper bound for generated random values (exclusive)
//
// Returns:
// - map[common.RecordID]uint32: Mapping of inserted record locations to their
// values
//
// The function ensures each insert operation is successful by retrying on full
// pages. All operations are performed within a single transaction that is
// committed at the end.
func fillPages(
	t *testing.T,
	logger *TxnLogger,
	txnID common.TxnID,
	length int,
	fileIDs []common.FileID,
	limit uint32,
) map[common.RecordID]uint32 {
	res := make(map[common.RecordID]uint32, length)

	chain := NewTxnLogChain(logger, txnID)
	chain.Begin()
	for range length {
		for {
			pageID := common.PageIdentity{
				FileID: common.FileID(fileIDs[rand.Int()%len(fileIDs)]),
				PageID: common.PageID(rand.Uint64() % 1024),
			}

			p, err := logger.pool.GetPage(pageID)
			require.NoError(t, err)
			success := func() bool {
				defer func() { require.NoError(t, logger.pool.Unpin(pageID)) }()
				p.Lock()
				defer p.Unlock()

				value := rand.Uint32() % limit
				insertedValue := utils.Uint32ToBytes(value)

				slotOpt := p.InsertPrepare(insertedValue)
				if slotOpt.IsNone() {
					return false
				}
				slot := slotOpt.Unwrap()
				chain.Insert(
					common.RecordID{
						FileID:  pageID.FileID,
						PageID:  pageID.PageID,
						SlotNum: slot,
					},
					insertedValue,
				)
				p.InsertCommit(slot)

				res[common.RecordID{
					FileID:  pageID.FileID,
					PageID:  pageID.PageID,
					SlotNum: slot,
				}] = value
				return true
			}()

			if success {
				break
			}
		}
	}
	chain.Commit()

	require.NoError(t, chain.Err())
	return res
}
