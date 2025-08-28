package recovery

import (
	"bytes"
	"errors"
	"maps"
	"math"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/optional"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
	"github.com/Blackdeer1524/GraphDB/src/storage/disk"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

func TestUndoInsertOnNonExistingPage(t *testing.T) {
	logPageId := common.PageIdentity{
		FileID: 42,
		PageID: 321,
	}

	diskManager := disk.NewInMemoryManager()
	masterRecordPageIdent := common.PageIdentity{
		FileID: logPageId.FileID,
		PageID: common.CheckpointInfoPageID,
	}
	pool := bufferpool.NewDebugBufferPool(
		bufferpool.New(10, bufferpool.NewLRUReplacer(), diskManager),
		map[common.PageIdentity]struct{}{
			masterRecordPageIdent: {},
		},
	)
	setupLoggerMasterPage(
		t,
		pool,
		masterRecordPageIdent.FileID,
		common.LogRecordLocInfo{
			Lsn:      common.NilLSN,
			Location: common.FileLocation{PageID: logPageId.PageID, SlotNum: 0},
		},
	)
	logger := NewTxnLogger(pool, logPageId.FileID)

	ctxLogger := logger.WithContext(common.TxnID(100))

	dataRecordID := common.RecordID{
		FileID:  32,
		PageID:  156,
		SlotNum: 0,
	}

	require.NoError(t, ctxLogger.AppendBegin())
	_, err := ctxLogger.AppendInsert(dataRecordID, []byte("bef000"))
	require.NoError(t, err)
	require.NoError(t, ctxLogger.AppendCommit())

	_, err = pool.GetPageNoCreate(dataRecordID.PageIdentity())
	require.ErrorIs(t, err, disk.ErrNoSuchPage)
	logger.Recover()
	page, err := pool.GetPageNoCreate(dataRecordID.PageIdentity())
	require.NoError(t, err)
	defer pool.Unpin(dataRecordID.PageIdentity())

	data := page.LockedRead(dataRecordID.SlotNum)
	require.Equal(t, []byte("bef000"), data)
}

func TestValidRecovery(t *testing.T) {
	logPageId := common.PageIdentity{
		FileID: 42,
		PageID: 321,
	}

	diskManager := disk.NewInMemoryManager()
	masterRecordPageIdent := common.PageIdentity{
		FileID: logPageId.FileID,
		PageID: common.CheckpointInfoPageID,
	}
	pool := bufferpool.NewDebugBufferPool(
		bufferpool.New(10, bufferpool.NewLRUReplacer(), diskManager),
		map[common.PageIdentity]struct{}{
			masterRecordPageIdent: {},
		},
	)
	loggerStart := common.LogRecordLocInfo{
		Lsn:      common.NilLSN,
		Location: common.FileLocation{PageID: logPageId.PageID, SlotNum: 0},
	}
	setupLoggerMasterPage(
		t,
		pool,
		masterRecordPageIdent.FileID,
		loggerStart,
	)
	logger := NewTxnLogger(pool, logPageId.FileID)

	defer func() {
		if recover() != nil {
			b := &strings.Builder{}
			logger.Dump(loggerStart.Location, b)
			println(b.String())
		}
		assert.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked())
	}()

	dataPageID := common.PageIdentity{FileID: 42, PageID: 123}
	slotNumOpt, err := insertValueNoLogs(t, pool, dataPageID, []byte("bef000"))
	require.True(t, slotNumOpt.IsSome())
	slotNum := slotNumOpt.Unwrap()
	require.NoError(t, err)

	txnID := common.TxnID(100)
	before := []byte("before")
	after := []byte("after!")

	chain := NewTxnLogChain(logger, txnID)
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
	logger2 := NewTxnLogger(pool, logger.logfileID)
	logger2.Recover()

	// Check that the page contains the "after" value
	p, err := pool.GetPage(dataPageID)
	defer func(pageID common.PageIdentity) { pool.Unpin(pageID) }(
		dataPageID,
	)

	if err != nil {
		t.Fatalf("GetPage failed: %v", err)
	}

	data := p.UnsafeRead(uint16(slotNum))

	if !bytes.Equal(data[:len(after)], after) {
		t.Errorf(
			"Recovery failed: expected %q, got %q",
			after,
			data[:len(after)],
		)
	}
}

func TestFailedTxn(t *testing.T) {
	logPageId := common.PageIdentity{
		FileID: 42,
		PageID: 1,
	}

	diskManager := disk.NewInMemoryManager()
	masterRecordPageIdent := common.PageIdentity{
		FileID: logPageId.FileID,
		PageID: common.CheckpointInfoPageID,
	}
	pool := bufferpool.NewDebugBufferPool(
		bufferpool.New(10, bufferpool.NewLRUReplacer(), diskManager),
		map[common.PageIdentity]struct{}{
			masterRecordPageIdent: {},
		},
	)

	checkpointLocation := common.LogRecordLocInfo{
		Lsn:      common.NilLSN,
		Location: common.FileLocation{PageID: logPageId.PageID, SlotNum: 0},
	}
	setupLoggerMasterPage(
		t,
		pool,
		masterRecordPageIdent.FileID,
		checkpointLocation,
	)
	logger := NewTxnLogger(pool, logPageId.FileID)

	pageIdent := common.PageIdentity{FileID: 13, PageID: 7}

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
	logger.Recover()

	// BEGIN
	iter, err := logger.iter(checkpointLocation.Location)
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
	pool bufferpool.BufferPool,
	pageId common.PageIdentity,
	data []byte,
) (optional.Optional[uint16], error) {
	p, err := pool.GetPage(pageId)
	require.NoError(t, err)
	defer pool.Unpin(pageId)

	p.Lock()
	defer p.Unlock()

	slotOpt := p.UnsafeInsertNoLogs(data)
	if slotOpt.IsNone() {
		return optional.None[uint16](), nil
	}

	return slotOpt, nil
}

func TestMassiveRecovery(t *testing.T) {
	logPageId := common.PageIdentity{
		FileID: 42,
		PageID: 321,
	}

	masterRecordPageIdent := common.PageIdentity{
		FileID: logPageId.FileID,
		PageID: common.CheckpointInfoPageID,
	}

	diskManager := disk.NewInMemoryManager()
	pool := bufferpool.NewDebugBufferPool(
		bufferpool.New(1000, bufferpool.NewLRUReplacer(), diskManager),
		map[common.PageIdentity]struct{}{
			masterRecordPageIdent: {},
		},
	)

	defer func() { assert.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	setupLoggerMasterPage(
		t,
		pool,
		masterRecordPageIdent.FileID,
		common.LogRecordLocInfo{
			Lsn:      common.NilLSN,
			Location: common.FileLocation{PageID: logPageId.PageID, SlotNum: 0},
		},
	)
	logger := NewTxnLogger(pool, logPageId.FileID)

	INIT := []byte("init")
	NEW := []byte("new1")
	NEW2 := []byte("1234")

	DataFileID := common.FileID(0)
	dataPageId := common.PageIdentity{
		FileID: DataFileID,
		PageID: 321,
	}
	slot := optional.Some(uint16(0))

	N := 30
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
	STEP := 1
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

						defer pool.Unpin(pageID)

						p.Lock()
						defer p.Unlock()

						data := p.UnsafeRead(recordLoc.SlotNum)

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

	logger.Recover()

	for i := range N {
		func() {
			location := index2pageID[i]
			dataPageId := common.PageIdentity{
				FileID: DataFileID,
				PageID: location.PageID,
			}
			p, err := pool.GetPageNoCreate(dataPageId)
			require.NoError(t, err)
			defer func() { pool.Unpin(dataPageId) }()

			p.RLock()
			defer p.RUnlock()

			data := p.UnsafeRead(location.SlotNum)
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
	expectedTxnID common.TxnID,
) {
	require.Equal(t, actualTag, expectedRecordType)

	switch actualTag {
	case TypeBegin:
		r, ok := untypedRecord.(BeginLogRecord)
		require.True(t, ok)
		require.Equal(t, expectedTxnID, r.txnID)
	case TypeUpdate:
		r, ok := untypedRecord.(UpdateLogRecord)
		require.True(t, ok)
		require.Equal(t, expectedTxnID, r.txnID)
	case TypeInsert:
		r, ok := untypedRecord.(InsertLogRecord)
		require.True(t, ok)
		require.Equal(t, expectedTxnID, r.txnID)
	case TypeDelete:
		r, ok := untypedRecord.(DeleteLogRecord)
		require.True(t, ok)
		require.Equal(t, expectedTxnID, r.txnID)
	case TypeCommit:
		r, ok := untypedRecord.(CommitLogRecord)
		require.True(t, ok)
		require.Equal(t, expectedTxnID, r.txnID)
	case TypeAbort:
		r, ok := untypedRecord.(AbortLogRecord)
		require.True(t, ok)
		require.Equal(t, expectedTxnID, r.txnID)
	case TypeTxnEnd:
		r, ok := untypedRecord.(TxnEndLogRecord)
		require.True(t, ok)
		require.Equal(t, expectedTxnID, r.txnID)
	case TypeCompensation:
		r, ok := untypedRecord.(CompensationLogRecord)
		require.True(t, ok)
		require.Equal(t, expectedTxnID, r.txnID)
	default:
		require.Less(t, actualTag, TypeUnknown)
	}
}

func assertLogRecordWithRetrieval(
	t *testing.T,
	pool bufferpool.BufferPool,
	recordID common.RecordID,
	expectedRecordType LogRecordTypeTag,
	expectedTxnID common.TxnID,
) {
	page, err := pool.GetPage(recordID.PageIdentity())
	require.NoError(t, err)
	page.RLock()

	data := page.UnsafeRead(recordID.SlotNum)

	tag, untypedRecord, err := parseLogRecord(data)
	require.NoError(t, err)

	assertLogRecord(
		t,
		tag,
		untypedRecord,
		expectedRecordType,
		expectedTxnID,
	)

	page.RUnlock()
	pool.Unpin(recordID.PageIdentity())
}

func TestLoggerValidConcurrentWrites(t *testing.T) {
	logFileID := common.FileID(42)

	diskManager := disk.NewInMemoryManager()
	masterRecordPageIdent := common.PageIdentity{
		FileID: logFileID,
		PageID: common.CheckpointInfoPageID,
	}
	pool := bufferpool.NewDebugBufferPool(
		bufferpool.New(1000, bufferpool.NewLRUReplacer(), diskManager),
		map[common.PageIdentity]struct{}{
			masterRecordPageIdent: {},
		},
	)
	defer func() { assert.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()
	setupLoggerMasterPage(
		t,
		pool,
		masterRecordPageIdent.FileID,
		common.LogRecordLocInfo{
			Lsn:      common.NilLSN,
			Location: common.FileLocation{PageID: 53, SlotNum: 0},
		},
	)
	logger := NewTxnLogger(pool, logFileID)

	dataPageId := common.PageIdentity{
		FileID: 33,
		PageID: 55,
	}

	waitWg := sync.WaitGroup{}
	barierWg := sync.WaitGroup{}

	OUTER := 100
	INNER := 4

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
							utils.ToBytes[uint32](uint32(i*INNER+j))).
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
							utils.ToBytes[uint32](uint32(i)),
							utils.ToBytes[uint32](uint32(i*INNER+j)),
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
	logFileID := common.FileID(42)

	diskManager := disk.NewInMemoryManager()
	masterRecordPageIdent := common.PageIdentity{
		FileID: logFileID,
		PageID: common.CheckpointInfoPageID,
	}
	pool := bufferpool.NewDebugBufferPool(
		bufferpool.New(100, bufferpool.NewLRUReplacer(), diskManager),
		map[common.PageIdentity]struct{}{
			masterRecordPageIdent: {},
		},
	)

	logStartLocation := common.FileLocation{
		PageID:  common.CheckpointInfoPageID + 1,
		SlotNum: 0,
	}

	setupLoggerMasterPage(
		t,
		pool,
		masterRecordPageIdent.FileID,
		common.LogRecordLocInfo{
			Lsn:      common.NilLSN,
			Location: logStartLocation,
		},
	)
	logger := NewTxnLogger(pool, logFileID)

	defer func() {
		assert.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked())
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

	recordValues := fillPages(t, logger, math.MaxUint64, 20_000, files, 1024, 40)
	require.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked())

	updatedValues := make(map[common.RecordID]uint32, len(recordValues))
	maps.Copy(updatedValues, recordValues)

	locker := txns.NewHierarchyLocker()
	defer func() {
		stillLockedTxns := locker.GetActiveTransactions()
		assert.Equal(
			t,
			0,
			len(stillLockedTxns),
			"There are still locked transactions: %+v",
			stillLockedTxns,
		)
	}()

	go func() {
		<-time.After(20 * time.Second)

		t.Logf("Have been waiting for too long. Preparing to dump dependency graph...")
		graph := locker.DumpDependencyGraph()
		t.Logf("\n%s", graph)
	}()

	txnID := atomic.Uint64{}
	wg := sync.WaitGroup{}
	t.Log("starting to process...")
	for batch := range mapBatch(recordValues, 200) {
		wg.Add(1)
		go func(batch []KVPair[common.RecordID, uint32]) {
			defer func() {
				r := recover()
				if r != nil {
					t.Logf("recovered from panic: %v", r)
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

			cToken := locker.LockCatalog(
				txnID,
				txns.GRANULAR_LOCK_INTENTION_EXCLUSIVE,
			)
			if cToken == nil {
				assert.NoError(t, logger.AppendAbort())
				logger.Rollback()
				return
			}
			defer locker.Unlock(cToken)

			for j := range len(batch) * 3 / 2 {
				info := batch[j%len(batch)]

				tToken := locker.LockFile(
					cToken,
					common.FileID(info.key.FileID),
					txns.GRANULAR_LOCK_INTENTION_EXCLUSIVE,
				)
				if tToken == nil {
					assert.NoError(t, logger.AppendAbort())
					logger.Rollback()
					return
				}

				ptoken := locker.LockPage(
					tToken,
					common.PageID(info.key.PageID),
					txns.PAGE_LOCK_EXCLUSIVE,
				)
				if ptoken == nil {
					assert.NoError(t, logger.AppendAbort())
					logger.Rollback()
					return
				}

				newValue := rand.Uint32()
				t.Logf("[%d] getting page %+v", txnID, info.key.PageIdentity())
				pg, err := pool.GetPageNoCreate(info.key.PageIdentity())
				require.NoError(t, err)
				t.Logf("[%d] updating page %+v", txnID, info.key.PageIdentity())

				err = pool.WithMarkDirty(
					txnID,
					info.key.PageIdentity(),
					pg,
					func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
						return lockedPage.UpdateWithLogs(
							utils.ToBytes[uint32](newValue),
							info.key,
							logger,
						)
					},
				)
				pool.Unpin(info.key.PageIdentity())
				t.Logf("[%d] done updating page %+v", txnID, info.key.PageIdentity())
				assert.NoError(t, err)
			}

			for j := range len(batch) / 3 {
				info := batch[j]

				tToken := locker.LockFile(
					cToken,
					common.FileID(info.key.FileID),
					txns.GRANULAR_LOCK_INTENTION_EXCLUSIVE,
				)
				if tToken == nil {
					assert.NoError(t, logger.AppendAbort())
					logger.Rollback()
					return
				}

				ptoken := locker.LockPage(
					tToken,
					common.PageID(info.key.PageID),
					txns.PAGE_LOCK_EXCLUSIVE,
				)
				if ptoken == nil {
					assert.NoError(t, logger.AppendAbort())
					logger.Rollback()
					return
				}

				t.Logf("[%d] getting page %+v", txnID, info.key.PageIdentity())
				pg, err := pool.GetPageNoCreate(info.key.PageIdentity())
				require.NoError(t, err)
				t.Logf("[%d] deleting page %+v", txnID, info.key.PageIdentity())
				err = pool.WithMarkDirty(
					txnID,
					info.key.PageIdentity(),
					pg,
					func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
						return lockedPage.DeleteWithLogs(info.key, logger)
					},
				)
				pool.Unpin(info.key.PageIdentity())
				t.Logf("[%d] done deleting page %+v", txnID, info.key.PageIdentity())
				require.NoError(t, err)
			}

			require.NoError(t, logger.AppendAbort())
			logger.Rollback()
		}(batch)
	}
	wg.Wait()

	t.Logf("checking rollback")
	for k, v := range recordValues {
		func() {
			pageID := common.PageIdentity{
				FileID: k.FileID,
				PageID: k.PageID,
			}
			page, err := pool.GetPageNoCreate(pageID)
			assert.NoError(t, err)
			defer func() { pool.Unpin(pageID) }()

			data := page.LockedRead(k.SlotNum)
			assert.Equal(t, data, utils.ToBytes[uint32](v))
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
//   - maxEntriesPerPage: Maximum number of entries allowed per page
//
// Returns:
//   - map[common.RecordID]uint32: Mapping of inserted record locations to their values
//
// The function ensures each insert operation is successful by retrying on full
// pages. All operations are performed within a single transaction that is
// committed at the end.
func fillPages(
	t *testing.T,
	logger *txnLogger,
	txnID common.TxnID,
	length int,
	fileIDs []common.FileID,
	limit uint32,
	maxEntriesPerPage int,
) map[common.RecordID]uint32 {
	res := make(map[common.RecordID]uint32, length)
	entriesPerPage := make(map[common.PageIdentity]int)

	chain := NewTxnLogChain(logger, txnID)

	chain.Begin()
	for range length {
		i := 0
		for i = range 256 {
			pageID := common.PageIdentity{
				FileID: common.FileID(fileIDs[rand.Int()%len(fileIDs)]),
				PageID: common.PageID(rand.Uint64() % 1024),
			}

			if entriesPerPage[pageID] >= maxEntriesPerPage {
				continue
			}

			p, err := logger.pool.GetPage(pageID)
			require.NoError(t, err)
			success := func() bool {
				defer func() { logger.pool.Unpin(pageID) }()

				value := rand.Uint32() % limit
				insertedValue := utils.ToBytes[uint32](value)

				slotNum := optional.None[uint16]()
				err := logger.pool.WithMarkDirty(
					txnID,
					pageID,
					p,
					func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
						slotNum = lockedPage.UnsafeInsertNoLogs(insertedValue)
						if slotNum.IsNone() {
							return common.NewNilLogRecordLocation(), page.ErrNoSpaceLeft
						}
						return common.NewNilLogRecordLocation(), nil
					},
				)
				if errors.Is(err, page.ErrNoSpaceLeft) {
					return false
				}
				slot := slotNum.Unwrap()
				chain.Insert(
					common.RecordID{
						FileID:  pageID.FileID,
						PageID:  pageID.PageID,
						SlotNum: slot,
					},
					insertedValue,
				)

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
		require.True(t, i < 256)
	}
	chain.Commit()

	require.NoError(t, chain.Err())
	return res
}

func TestTxnLoggerWithContext_Rollback(t *testing.T) {
	masterRecordPageIdent := common.PageIdentity{
		FileID: 42,
		PageID: common.CheckpointInfoPageID,
	}

	diskManager := disk.NewInMemoryManager()
	pool := bufferpool.NewDebugBufferPool(
		bufferpool.New(10, bufferpool.NewLRUReplacer(), diskManager),
		map[common.PageIdentity]struct{}{
			masterRecordPageIdent: {},
		},
	)

	defer func() {
		assert.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked())
	}()
	logger := NewTxnLogger(pool, 42)

	dataPageID := common.PageIdentity{
		FileID: 321,
		PageID: 64,
	}

	pg, err := pool.GetPage(dataPageID)
	require.NoError(t, err)
	defer pool.Unpin(dataPageID)

	txnID := common.TxnID(100)
	slot := uint16(123)
	require.NoError(t, pool.WithMarkDirty(
		txnID,
		dataPageID,
		pg,
		func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
			slotOpt := lockedPage.UnsafeInsertNoLogs([]byte("test"))
			require.True(t, slotOpt.IsSome())
			slot = slotOpt.Unwrap()
			return common.NewNilLogRecordLocation(), nil
		},
	))

	chain := NewTxnLogChain(logger, txnID).
		Begin().
		Insert(common.RecordID{
			FileID:  dataPageID.FileID,
			PageID:  dataPageID.PageID,
			SlotNum: slot,
		}, []byte("test")).
		Abort()

	require.NoError(t, chain.Err())
	logger.Rollback(chain.Loc())
}
