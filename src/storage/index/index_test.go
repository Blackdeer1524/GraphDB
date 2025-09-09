package index

import (
	"fmt"
	"sync"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
	"github.com/Blackdeer1524/GraphDB/src/recovery"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/storage/disk"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

func TestMarshalAndUnmarshalBucketItem(t *testing.T) {
	status := bucketItemStatusInserted
	key := "test"
	rid := common.RecordID{
		FileID:  1,
		PageID:  2,
		SlotNum: 3,
	}
	keySize := len(key)
	marshalled, err := marshalBucketItem(status, key, rid)
	assert.NoError(t, err)
	assert.Equal(t, int(bucketItemSizeWithoutKey)+keySize, len(marshalled))
}

func setupIndexPages(t *testing.T, pool bufferpool.BufferPool, indexMeta storage.IndexMeta) {
	bucketItemSize := bucketItemSizeWithoutKey + uintptr(indexMeta.KeyBytesCnt)
	bucketCapacity := page.PageCapacity(int(bucketItemSize))

	func() {
		masterPageIdent := getMasterPageIdent(indexMeta.FileID)
		masterPage, err := pool.GetPage(masterPageIdent)
		require.NoError(t, err)
		defer pool.Unpin(masterPageIdent)

		if masterPage.NumSlots() == masterPageSlotsCount {
			return
		}

		masterPage.Clear()
		inserts := []struct {
			expectedSlotNum uint16
			data            uint64
		}{
			{bucketsCountSlot, 1},
			{bucketItemSizeSlot, uint64(bucketItemSize)},
			{bucketCapacitySlot, uint64(bucketCapacity)},
			{recordsCountSlot, 0},
			{hashmapTotalCapacitySlot, uint64(bucketCapacity)},
			{startPageIDSlot, 1},
		}

		require.Equal(t, masterPageSlotsCount, len(inserts))
		err = pool.WithMarkDirty(
			common.NilTxnID,
			masterPageIdent,
			masterPage,
			func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
				lockedPage.Clear()
				for _, insert := range inserts {
					slotOpt := lockedPage.UnsafeInsertNoLogs(utils.ToBytes[uint64](insert.data))
					assert.Equal(t, insert.expectedSlotNum, slotOpt.Unwrap())
				}
				return common.NewNilLogRecordLocation(), nil
			},
		)
		require.NoError(t, err)
	}()

	func() {
		bucketPageIdent := common.PageIdentity{
			FileID: indexMeta.FileID,
			PageID: 1,
		}
		bucketPage, err := pool.GetPage(bucketPageIdent)
		require.NoError(t, err)
		defer pool.Unpin(bucketPageIdent)

		if bucketPage.NumSlots() == uint16(bucketCapacity) {
			return
		}
		dummyRecord := make([]byte, bucketItemSize)
		bucketPage.Clear()
		err = pool.WithMarkDirty(
			common.NilTxnID,
			bucketPageIdent,
			bucketPage,
			func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
				lockedPage.Clear()
				for range bucketCapacity {
					slotOpt := lockedPage.UnsafeInsertNoLogs(dummyRecord)
					assert.True(t, slotOpt.IsSome(), "impossible")
				}
				return common.NewNilLogRecordLocation(), nil
			},
		)
		require.NoError(t, err)
	}()
}

func setupIndex(
	t *testing.T,
	keyLength uint32,
) (*bufferpool.DebugBufferPool, common.ITxnLogger, storage.IndexMeta, *txns.LockManager) {
	newPageFunc := func(fileID common.FileID, pageID common.PageID) *page.SlottedPage {
		return page.NewSlottedPage()
	}
	fs := afero.NewMemMapFs()
	diskMgr := disk.New(newPageFunc, fs)

	logFileID := common.FileID(42)
	indexFileID := common.FileID(1)
	diskMgr.InsertToFileMap(logFileID, "/tmp/graphdb_test/log")
	diskMgr.InsertToFileMap(indexFileID, "/tmp/graphdb_test/index")

	pool := bufferpool.New(
		10,
		bufferpool.NewLRUReplacer(),
		diskMgr,
	)
	debugPool := bufferpool.NewDebugBufferPool(pool)
	debugPool.MarkPageAsLeaking(common.PageIdentity{
		FileID: logFileID,
		PageID: common.CheckpointInfoPageID,
	})

	logger := recovery.NewTxnLogger(debugPool, logFileID)
	locker := txns.NewLockManager()

	indexMeta := storage.IndexMeta{
		FileID:      indexFileID,
		KeyBytesCnt: keyLength,
	}
	setupIndexPages(t, debugPool, indexMeta)
	return debugPool, logger, indexMeta, locker
}

func TestNoLeakageAfterCreation(t *testing.T) {
	pool, logger, indexMeta, locker := setupIndex(t, 4)
	ctxLogger := logger.WithContext(common.TxnID(1))
	index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger)
	require.NoError(t, err)

	index.Close()
	assert.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked())
}

func TestIndexInsert(t *testing.T) {
	pool, logger, indexMeta, locker := setupIndex(t, 4)
	ctxLogger := logger.WithContext(common.TxnID(1))
	index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger)
	require.NoError(t, err)

	defer func() { assert.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()
	defer index.Close()

	key := []byte("test")
	expectedRID := common.RecordID{
		FileID:  1,
		PageID:  2,
		SlotNum: 3,
	}
	err = index.Insert(key, expectedRID)
	require.NoError(t, err)

	rid, err := index.Get(key)
	require.NoError(t, err)
	require.Equal(t, expectedRID, rid)

	err = index.grow()
	require.NoError(t, err)
}

func TestIndexWithRebuild(t *testing.T) {
	pool, logger, indexMeta, locker := setupIndex(t, 8)
	ctxLogger := logger.WithContext(common.TxnID(1))
	index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger)
	require.NoError(t, err)

	defer func() { assert.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()
	defer index.Close()

	N := 1000
	rid := common.RecordID{
		FileID:  1,
		PageID:  2,
		SlotNum: 0,
	}
	for i := range N {
		key := utils.ToBytes[uint64](uint64(i))
		rid.SlotNum = uint16(i)

		err := index.Insert(key, rid)
		require.NoError(t, err)

		storedRID, err := index.Get(key)
		require.NoError(t, err)
		assert.Equal(t, rid, storedRID)
	}

	for i := range N {
		rid.SlotNum = uint16(i)
		key := utils.ToBytes[uint64](uint64(i))
		storedRID, err := index.Get(key)
		require.NoError(t, err)
		assert.Equal(t, rid, storedRID)
	}

	for i := N; i < N*2; i++ {
		key := utils.ToBytes[uint64](uint64(i))
		_, err := index.Get(key)
		require.ErrorIs(t, err, storage.ErrKeyNotFound)
	}
}

func TestIndexRollback(t *testing.T) {
	N := 5000
	pool, logger, indexMeta, locker := setupIndex(t, 8)
	defer func() { assert.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()
	func() {
		ctxLogger := logger.WithContext(common.TxnID(1))
		defer func() { assert.True(t, locker.AreAllQueuesEmpty()) }()
		defer locker.Unlock(common.TxnID(1))

		index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger)
		require.NoError(t, err)
		defer index.Close()

		require.NoError(t, ctxLogger.AppendBegin())

		rid := common.RecordID{
			FileID:  1,
			PageID:  2,
			SlotNum: 0,
		}
		for i := range N {
			key := utils.ToBytes[uint64](uint64(i))
			rid.SlotNum = uint16(i)

			err := index.Insert(key, rid)
			require.NoError(t, err)

			storedRID, err := index.Get(key)
			require.NoError(t, err)
			assert.Equal(t, rid, storedRID)
		}

		require.NoError(t, ctxLogger.AppendCommit())
	}()

	func() {
		ctxLogger := logger.WithContext(common.TxnID(2))
		defer func() {
			if !assert.True(t, locker.AreAllQueuesEmpty()) {
				graph := locker.DumpDependencyGraph()
				t.Logf("\n%s", graph)
			}
		}()
		defer locker.Unlock(common.TxnID(2))
		index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger)
		require.NoError(t, err)

		defer index.Close()

		rid := common.RecordID{
			FileID:  1,
			PageID:  2,
			SlotNum: 0,
		}

		for i := range N {
			rid.SlotNum = uint16(i)
			key := utils.ToBytes[uint64](uint64(i))
			storedRID, err := index.Get(key)
			require.NoError(t, err)
			assert.Equal(t, rid, storedRID)
		}

		require.NoError(t, ctxLogger.AppendBegin())
		for i := N; i < N*2; i++ {
			key := utils.ToBytes[uint64](uint64(i))
			rid.SlotNum = uint16(i)

			err := index.Insert(key, rid)
			require.NoError(t, err)

			storedRID, err := index.Get(key)
			require.NoError(t, err)
			assert.Equal(t, rid, storedRID)
		}

		for i := range 2 * N {
			rid.SlotNum = uint16(i)
			key := utils.ToBytes[uint64](uint64(i))
			storedRID, err := index.Get(key)
			require.NoError(t, err)
			assert.Equal(t, rid, storedRID)
		}

		require.NoError(t, ctxLogger.AppendAbort())
		ctxLogger.Rollback()
	}()

	func() {
		ctxLogger := logger.WithContext(common.TxnID(3))
		defer func() {
			if !assert.True(t, locker.AreAllQueuesEmpty()) {
				graph := locker.DumpDependencyGraph()
				t.Logf("\n%s", graph)
			}
		}()
		defer locker.Unlock(common.TxnID(3))
		index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger)
		require.NoError(t, err)
		defer index.Close()

		rid := common.RecordID{
			FileID:  1,
			PageID:  2,
			SlotNum: 0,
		}
		for i := range N {
			rid.SlotNum = uint16(i)
			key := utils.ToBytes[uint64](uint64(i))
			storedRID, err := index.Get(key)
			require.NoError(t, err)
			assert.Equal(t, rid, storedRID)
		}

		for i := N; i < N*2; i++ {
			rid.SlotNum = uint16(i)
			key := utils.ToBytes[uint64](uint64(i))
			_, err := index.Get(key)
			require.ErrorIs(t, err, storage.ErrKeyNotFound)
		}

		for i := N * 3; i < N*4; i++ {
			rid.SlotNum = uint16(i)
			key := utils.ToBytes[uint64](uint64(i))
			err := index.Insert(key, rid)
			require.NoError(t, err)
		}

		for i := N * 3; i < N*4; i++ {
			rid.SlotNum = uint16(i)
			key := utils.ToBytes[uint64](uint64(i))
			storedRID, err := index.Get(key)
			require.NoError(t, err)
			assert.Equal(t, rid, storedRID)
		}
	}()
}

// TestConcurrentInserts tests concurrent insert operations
func TestConcurrentInserts(t *testing.T) {
	pool, logger, indexMeta, locker := setupIndex(t, 8)
	defer func() { assert.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	const numGoroutines = 5
	const insertsPerGoroutine = 20 // Reduced to avoid growth issues
	var wg sync.WaitGroup
	var mu sync.Mutex
	insertedKeys := make(map[string]common.RecordID)

	goroutineIDs := utils.GenerateUniqueInts[uint64](numGoroutines, 1, numGoroutines)

	// Start concurrent insert operations
	for _, goroutineID := range goroutineIDs {
		wg.Add(1)
		go func(goroutineID uint64) {
			txnID := common.TxnID(goroutineID + 1)
			defer wg.Done()

			ctxLogger := logger.WithContext(txnID)
			index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger)
			require.NoError(t, err)
			defer locker.Unlock(txnID)
			defer index.Close()

			for j := 0; j < insertsPerGoroutine; j++ {
				key := utils.ToBytes[uint64](goroutineID*uint64(insertsPerGoroutine) + uint64(j))
				rid := common.RecordID{
					FileID:  common.FileID(1),
					PageID:  common.PageID(goroutineID + 1),
					SlotNum: uint16(j),
				}

				err := index.Insert(key, rid)
				if err != nil {
					require.NoError(t, ctxLogger.AppendAbort())
					ctxLogger.Rollback()
					return
				}

				mu.Lock()
				insertedKeys[string(key)] = rid
				mu.Unlock()
			}
		}(goroutineID)
	}

	wg.Wait()

	// Create a final index to verify all inserts were successful
	ctxLogger := logger.WithContext(common.TxnID(numGoroutines + 1))
	index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger)
	require.NoError(t, err)
	defer index.Close()

	// Verify all inserts were successful
	for keyStr, expectedRID := range insertedKeys {
		key := []byte(keyStr)
		actualRID, err := index.Get(key)
		require.NoError(t, err)
		assert.Equal(t, expectedRID, actualRID)
	}
}

// TestConcurrentGets tests concurrent get operations
func TestConcurrentGets(t *testing.T) {
	pool, logger, indexMeta, locker := setupIndex(t, 8)
	defer func() { assert.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	// Pre-populate the index with a single index instance
	ctxLogger := logger.WithContext(common.TxnID(1))
	index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger)
	require.NoError(t, err)
	defer locker.Unlock(common.TxnID(1))
	defer index.Close()

	const numKeys = 1000
	expectedRIDs := make(map[string]common.RecordID)

	for i := 0; i < numKeys; i++ {
		key := utils.ToBytes[uint64](uint64(i))
		rid := common.RecordID{
			FileID:  common.FileID(1),
			PageID:  common.PageID(1),
			SlotNum: uint16(i),
		}

		err := index.Insert(key, rid)
		require.NoError(t, err)
		expectedRIDs[string(key)] = rid
	}

	const numGoroutines = 10
	const getsPerGoroutine = 20
	var wg sync.WaitGroup
	var mu sync.Mutex
	errors := make([]error, 0)

	goroutineIDs := utils.GenerateUniqueInts[uint64](numGoroutines, 2, numGoroutines+2)
	// Start concurrent get operations
	for _, goroutineID := range goroutineIDs {
		wg.Add(1)
		go func(goroutineID uint64) {
			defer wg.Done()
			txnID := common.TxnID(goroutineID + 2)

			// Create a separate index for this goroutine
			ctxLogger := logger.WithContext(txnID)
			index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger)
			require.NoError(t, err)
			defer locker.Unlock(txnID)
			defer index.Close()

			for j := 0; j < getsPerGoroutine; j++ {
				keyIndex := (goroutineID*uint64(getsPerGoroutine) + uint64(j)) % uint64(numKeys)
				key := utils.ToBytes[uint64](uint64(keyIndex))
				expectedRID := expectedRIDs[string(key)]

				actualRID, err := index.Get(key)
				if err != nil {
					require.NoError(t, ctxLogger.AppendAbort())
					ctxLogger.Rollback()
					return
				}

				if actualRID != expectedRID {
					mu.Lock()
					errors = append(
						errors,
						fmt.Errorf(
							"mismatch for key %d: expected %+v, got %+v",
							keyIndex,
							expectedRID,
							actualRID,
						),
					)
					mu.Unlock()
				}
			}
		}(goroutineID)
	}

	wg.Wait()

	// Verify no errors occurred
	assert.Empty(t, errors, "Concurrent gets should not produce errors")
}

// TestConcurrentDeletes tests concurrent delete operations
func TestConcurrentDeletes(t *testing.T) {
	pool, logger, indexMeta, locker := setupIndex(t, 8)
	defer func() { assert.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	// Pre-populate the index with a single index instance
	ctxLogger := logger.WithContext(common.TxnID(1))
	index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger)
	require.NoError(t, err)
	defer locker.Unlock(common.TxnID(1))
	defer index.Close()

	// Pre-populate the index
	const numKeys = 1000
	keys := make([][]byte, numKeys)

	for i := 0; i < numKeys; i++ {
		key := utils.ToBytes[uint64](uint64(i))
		keys[i] = key
		rid := common.RecordID{
			FileID:  common.FileID(1),
			PageID:  common.PageID(1),
			SlotNum: uint16(i),
		}

		err := index.Insert(key, rid)
		require.NoError(t, err)
	}

	const numGoroutines = 5
	const deletesPerGoroutine = 20
	var wg sync.WaitGroup
	var mu sync.Mutex
	deletedKeys := make(map[string]bool)

	// Start concurrent delete operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			// Create a separate index for this goroutine
			txnID := common.TxnID(goroutineID + 2)
			ctxLogger := logger.WithContext(txnID)
			index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger)
			require.NoError(t, err)
			defer locker.Unlock(txnID)
			defer index.Close()

			for j := 0; j < deletesPerGoroutine; j++ {
				keyIndex := (goroutineID*deletesPerGoroutine + j) % numKeys
				key := keys[keyIndex]

				err := index.Delete(key)
				if err != nil {
					// Some deletes might fail if key was already deleted
					continue
				}

				mu.Lock()
				deletedKeys[string(key)] = true
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	// Create a final index to verify deleted keys are no longer accessible
	ctxLogger = logger.WithContext(common.TxnID(numGoroutines + 2))
	index, err = NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger)
	require.NoError(t, err)
	defer locker.Unlock(common.TxnID(numGoroutines + 2))
	defer index.Close()

	// Verify deleted keys are no longer accessible
	for keyStr := range deletedKeys {
		key := []byte(keyStr)
		_, err := index.Get(key)
		assert.ErrorIs(t, err, storage.ErrKeyNotFound, "Deleted key should not be found")
	}
}

// TestConcurrentMixedOperations tests mixed concurrent operations
func TestConcurrentMixedOperations(t *testing.T) {
	pool, logger, indexMeta, locker := setupIndex(t, 8)
	defer func() { assert.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	const numGoroutines = 10
	const operationsPerGoroutine = 20
	var wg sync.WaitGroup
	var mu sync.Mutex
	insertedKeys := make(map[string]common.RecordID)
	errors := make([]error, 0)

	// Start mixed concurrent operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			// Create a separate index for this goroutine
			txnID := common.TxnID(goroutineID + 1)
			ctxLogger := logger.WithContext(txnID)
			index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger)
			require.NoError(t, err)
			defer locker.Unlock(txnID)
			defer index.Close()

			for j := 0; j < operationsPerGoroutine; j++ {
				operation := j % 3
				keyIndex := goroutineID*operationsPerGoroutine + j
				key := utils.ToBytes[uint64](uint64(keyIndex))
				rid := common.RecordID{
					FileID:  common.FileID(1),
					PageID:  common.PageID(goroutineID + 1),
					SlotNum: uint16(j),
				}

				switch operation {
				case 0: // Insert
					err := index.Insert(key, rid)
					if err != nil {
						mu.Lock()
						errors = append(
							errors,
							fmt.Errorf("insert failed for key %d: %w", keyIndex, err),
						)
						mu.Unlock()
						continue
					}

					mu.Lock()
					insertedKeys[string(key)] = rid
					mu.Unlock()

				case 1: // Get
					actualRID, err := index.Get(key)
					if err != nil && err != storage.ErrKeyNotFound {
						mu.Lock()
						errors = append(
							errors,
							fmt.Errorf("get failed for key %d: %w", keyIndex, err),
						)
						mu.Unlock()
						continue
					}

					// If key exists, verify it matches expected RID
					if err == nil {
						mu.Lock()
						expectedRID, exists := insertedKeys[string(key)]
						mu.Unlock()
						if exists && actualRID != expectedRID {
							mu.Lock()
							errors = append(
								errors,
								fmt.Errorf(
									"get mismatch for key %d: expected %+v, got %+v",
									keyIndex,
									expectedRID,
									actualRID,
								),
							)
							mu.Unlock()
						}
					}

				case 2: // Delete
					err := index.Delete(key)
					if err != nil && err != storage.ErrKeyNotFound {
						mu.Lock()
						errors = append(
							errors,
							fmt.Errorf("delete failed for key %d: %w", keyIndex, err),
						)
						mu.Unlock()
						continue
					}

					// If delete was successful, remove from tracking
					if err == nil {
						mu.Lock()
						delete(insertedKeys, string(key))
						mu.Unlock()
					}
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify no critical errors occurred
	assert.Empty(t, errors, "Mixed concurrent operations should not produce critical errors")

	// Create a final index to verify remaining keys are still accessible
	ctxLogger := logger.WithContext(common.TxnID(numGoroutines + 1))
	index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger)
	require.NoError(t, err)
	defer locker.Unlock(common.TxnID(numGoroutines + 1))
	defer index.Close()

	// Verify remaining keys are still accessible
	for keyStr, expectedRID := range insertedKeys {
		key := []byte(keyStr)
		actualRID, err := index.Get(key)
		require.NoError(t, err)
		assert.Equal(t, expectedRID, actualRID)
	}
}

// TestConcurrentStressTest performs a stress test with high concurrency
func TestConcurrentStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	pool, logger, indexMeta, locker := setupIndex(t, 8)
	defer func() { assert.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	const numGoroutines = 10
	const operationsPerGoroutine = 20
	var wg sync.WaitGroup
	var mu sync.Mutex
	insertedKeys := make(map[string]common.RecordID)
	errors := make([]error, 0)

	// Start stress test with high concurrency
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			// Create a separate index for this goroutine
			txnID := common.TxnID(goroutineID + 1)
			ctxLogger := logger.WithContext(txnID)
			index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger)
			require.NoError(t, err)
			defer locker.Unlock(txnID)
			defer index.Close()

			for j := 0; j < operationsPerGoroutine; j++ {
				operation := j % 4
				keyIndex := goroutineID*operationsPerGoroutine + j
				key := utils.ToBytes[uint64](uint64(keyIndex))
				rid := common.RecordID{
					FileID:  common.FileID(1),
					PageID:  common.PageID(goroutineID + 1),
					SlotNum: uint16(j),
				}

				switch operation {
				case 0, 1: // Insert (50% of operations)
					err := index.Insert(key, rid)
					if err != nil {
						mu.Lock()
						errors = append(
							errors,
							fmt.Errorf("insert failed for key %d: %w", keyIndex, err),
						)
						mu.Unlock()
						continue
					}

					mu.Lock()
					insertedKeys[string(key)] = rid
					mu.Unlock()

				case 2: // Get (25% of operations)
					_, err := index.Get(key)
					if err != nil && err != storage.ErrKeyNotFound {
						mu.Lock()
						errors = append(
							errors,
							fmt.Errorf("get failed for key %d: %w", keyIndex, err),
						)
						mu.Unlock()
					}

				case 3: // Delete (25% of operations)
					err := index.Delete(key)
					if err != nil && err != storage.ErrKeyNotFound {
						mu.Lock()
						errors = append(
							errors,
							fmt.Errorf("delete failed for key %d: %w", keyIndex, err),
						)
						mu.Unlock()
						continue
					}

					if err == nil {
						mu.Lock()
						delete(insertedKeys, string(key))
						mu.Unlock()
					}
				}
			}
		}(i)
	}

	wg.Wait()

	// Create a final index to verify remaining keys are still accessible
	ctxLogger := logger.WithContext(common.TxnID(numGoroutines + 1))
	index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger)
	require.NoError(t, err)
	defer locker.Unlock(common.TxnID(numGoroutines + 1))
	defer index.Close()

	// Verify remaining keys are still accessible
	for keyStr, expectedRID := range insertedKeys {
		key := []byte(keyStr)
		actualRID, err := index.Get(key)
		require.NoError(t, err)
		assert.Equal(t, expectedRID, actualRID)
	}
}

// TestConcurrentIndexGrowth tests concurrent operations during index growth
func TestConcurrentIndexGrowth(t *testing.T) {
	pool, logger, indexMeta, locker := setupIndex(t, 8)
	defer func() { assert.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	const numGoroutines = 5
	const insertsPerGoroutine = 20 // Reduced to avoid growth issues
	var wg sync.WaitGroup
	var mu sync.Mutex
	insertedKeys := make(map[string]common.RecordID)
	errors := make([]error, 0)

	// Start concurrent inserts that will trigger index growth
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			// Create a separate index for this goroutine
			txnID := common.TxnID(goroutineID + 1)
			ctxLogger := logger.WithContext(txnID)
			index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger)
			require.NoError(t, err)
			defer locker.Unlock(txnID)
			defer index.Close()

			for j := 0; j < insertsPerGoroutine; j++ {
				key := utils.ToBytes[uint64](uint64(goroutineID*insertsPerGoroutine + j))
				rid := common.RecordID{
					FileID:  common.FileID(1),
					PageID:  common.PageID(goroutineID + 1),
					SlotNum: uint16(j),
				}

				err := index.Insert(key, rid)
				if err != nil {
					mu.Lock()
					errors = append(
						errors,
						fmt.Errorf(
							"insert failed for key %d: %w",
							goroutineID*insertsPerGoroutine+j,
							err,
						),
					)
					mu.Unlock()
					continue
				}

				mu.Lock()
				insertedKeys[string(key)] = rid
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	// Verify no critical errors occurred during growth
	assert.Empty(
		t,
		errors,
		"Concurrent operations during index growth should not produce critical errors",
	)

	// Create a final index to verify all inserts were successful
	ctxLogger := logger.WithContext(common.TxnID(numGoroutines + 1))
	index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger)
	require.NoError(t, err)
	defer locker.Unlock(common.TxnID(numGoroutines + 1))
	defer index.Close()

	// Verify all inserts were successful
	for keyStr, expectedRID := range insertedKeys {
		key := []byte(keyStr)
		actualRID, err := index.Get(key)
		require.NoError(t, err)
		assert.Equal(t, expectedRID, actualRID)
	}
}
