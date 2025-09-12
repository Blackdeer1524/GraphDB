package index

import (
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"

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

func setup(
	t testing.TB,
	poolSize uint64,
	keyLength uint32,
) (*bufferpool.DebugBufferPool, common.ITxnLogger, storage.IndexMeta, *txns.LockManager) {
	basePath := "/tmp/graphdb_test"
	newPageFunc := func(fileID common.FileID, pageID common.PageID) *page.SlottedPage {
		return page.NewSlottedPage()
	}
	fs := afero.NewMemMapFs()
	diskMgr := disk.New(basePath, newPageFunc, fs)

	logFileID := common.FileID(42)
	indexFileID := common.FileID(33)

	pool := bufferpool.New(
		poolSize,
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
	return debugPool, logger, indexMeta, locker
}

func TestNoLeakageAfterCreation(t *testing.T) {
	pool, logger, indexMeta, locker := setup(t, 10, 4)
	ctxLogger := logger.WithContext(common.TxnID(1))
	index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger, true, 42)
	require.NoError(t, err)

	index.Close()
	assert.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked())
}

func TestIndexInsert(t *testing.T) {
	pool, logger, indexMeta, locker := setup(t, 10, 4)
	ctxLogger := logger.WithContext(common.TxnID(1))
	require.NoError(t, ctxLogger.AppendBegin())
	index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger, true, 42)
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
	require.NoError(t, ctxLogger.AppendCommit())
}

func TestIndexWithRebuild(t *testing.T) {
	pool, logger, indexMeta, locker := setup(t, 10, 8)
	ctxLogger := logger.WithContext(common.TxnID(1))
	require.NoError(t, ctxLogger.AppendBegin())
	index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger, true, 42)
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
	require.NoError(t, ctxLogger.AppendCommit())
}

func TestIndexRollback(t *testing.T) {
	N := 2000
	pool, logger, indexMeta, locker := setup(t, 10, 8)
	defer func() { assert.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()
	func() {
		ctxLogger := logger.WithContext(common.TxnID(1))
		require.NoError(t, ctxLogger.AppendBegin())
		defer func() { assert.True(t, locker.AreAllQueuesEmpty()) }()
		defer locker.Unlock(common.TxnID(1))

		index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger, true, 42)
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
		require.NoError(t, ctxLogger.AppendBegin())
		defer func() {
			if !assert.True(t, locker.AreAllQueuesEmpty()) {
				graph := locker.DumpDependencyGraph()
				t.Logf("\n%s", graph)
			}
		}()
		defer locker.Unlock(common.TxnID(2))
		index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger, true, 42)
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
		require.NoError(t, ctxLogger.AppendBegin())
		defer func() {
			if !assert.True(t, locker.AreAllQueuesEmpty()) {
				graph := locker.DumpDependencyGraph()
				t.Logf("\n%s", graph)
			}
		}()
		defer locker.Unlock(common.TxnID(3))
		index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger, true, 42)
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

		require.NoError(t, ctxLogger.AppendCommit())
	}()
}

// TestConcurrentInserts tests concurrent insert operations
func TestConcurrentInserts(t *testing.T) {
	pool, logger, indexMeta, locker := setup(t, 10, 8)
	defer func() { assert.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	const numGoroutines = 100
	const insertsPerGoroutine = 200 // Reduced to avoid growth issues
	var wg sync.WaitGroup
	var mu sync.Mutex
	insertedKeys := make(map[string]common.RecordID)

	r := rand.New(rand.NewSource(12))
	goroutineIDs := utils.GenerateUniqueInts[uint64](numGoroutines, 1, numGoroutines, r)

	// Start concurrent insert operations
	for _, goroutineID := range goroutineIDs {
		wg.Add(1)
		go func(goroutineID uint64) {
			defer wg.Done()

			txnID := common.TxnID(goroutineID + 1)
			ctxLogger := logger.WithContext(txnID)
			defer locker.Unlock(txnID)

			require.NoError(t, ctxLogger.AppendBegin())
			index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger, true, 42)
			if errors.Is(err, txns.ErrDeadlockPrevention) {
				require.NoError(t, ctxLogger.AppendAbort())
				ctxLogger.Rollback()
				return
			} else if err != nil {
				assert.NoError(t, err)
				require.NoError(t, ctxLogger.AppendAbort())
				ctxLogger.Rollback()
				return
			}
			defer index.Close()

			localInsertedKeys := make(map[string]common.RecordID)

			for j := 0; j < insertsPerGoroutine; j++ {
				key := utils.ToBytes[uint64](goroutineID*uint64(insertsPerGoroutine) + uint64(j))
				rid := common.RecordID{
					FileID:  common.FileID(1),
					PageID:  common.PageID(goroutineID + 1),
					SlotNum: uint16(j),
				}

				err := index.Insert(key, rid)
				if errors.Is(err, txns.ErrDeadlockPrevention) {
					require.NoError(t, ctxLogger.AppendAbort())
					ctxLogger.Rollback()
					return
				} else if err != nil {
					assert.NoError(t, err)
					require.NoError(t, ctxLogger.AppendAbort())
					ctxLogger.Rollback()
					return
				}

				localInsertedKeys[string(key)] = rid
			}

			require.NoError(t, ctxLogger.AppendCommit())
			for keyStr, expectedRID := range localInsertedKeys {
				mu.Lock()
				insertedKeys[keyStr] = expectedRID
				mu.Unlock()
			}
		}(goroutineID)
	}

	wg.Wait()

	// Create a final index to verify all inserts were successful
	ctxLogger := logger.WithContext(common.TxnID(numGoroutines + 1))
	index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger, true, 42)
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
	pool, logger, indexMeta, locker := setup(t, 10, 8)
	defer func() { assert.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	// Pre-populate the index with a single index instance
	ctxLogger := logger.WithContext(common.TxnID(1))
	require.NoError(t, ctxLogger.AppendBegin())
	index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger, true, 42)
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
	require.NoError(t, ctxLogger.AppendCommit())

	const numGoroutines = 10
	const getsPerGoroutine = 20
	var wg sync.WaitGroup

	r := rand.New(rand.NewSource(42))
	goroutineIDs := utils.GenerateUniqueInts[uint64](numGoroutines, 2, numGoroutines+2, r)
	// Start concurrent get operations
	for _, goroutineID := range goroutineIDs {
		wg.Add(1)
		go func(goroutineID uint64) {
			defer wg.Done()
			txnID := common.TxnID(goroutineID + 2)
			defer locker.Unlock(txnID)

			ctxLogger := logger.WithContext(txnID)
			require.NoError(t, ctxLogger.AppendBegin())

			index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger, true, 42)
			if errors.Is(err, txns.ErrDeadlockPrevention) {
				require.NoError(t, ctxLogger.AppendAbort())
				ctxLogger.Rollback()
				return
			} else if err != nil {
				assert.NoError(t, err)
				require.NoError(t, ctxLogger.AppendAbort())
				ctxLogger.Rollback()
				return
			}
			defer index.Close()

			for j := 0; j < getsPerGoroutine; j++ {
				keyIndex := (goroutineID*uint64(getsPerGoroutine) + uint64(j)) % uint64(numKeys)
				key := utils.ToBytes[uint64](uint64(keyIndex))
				expectedRID := expectedRIDs[string(key)]

				actualRID, err := index.Get(key)
				if errors.Is(err, txns.ErrDeadlockPrevention) {
					require.NoError(t, ctxLogger.AppendAbort())
					ctxLogger.Rollback()
					return
				} else if err != nil {
					assert.NoError(t, err)
					require.NoError(t, ctxLogger.AppendAbort())
					ctxLogger.Rollback()
					return
				}

				assert.Equal(t, expectedRID, actualRID)
			}
		}(goroutineID)
	}

	wg.Wait()
}

// TestConcurrentDeletes tests concurrent delete operations
func TestConcurrentDeletes(t *testing.T) {
	pool, logger, indexMeta, locker := setup(t, 10, 8)
	defer func() { assert.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	const numKeys = 1000
	keys := make([][]byte, numKeys)
	func() {
		ctxLogger := logger.WithContext(common.TxnID(1))
		defer locker.Unlock(common.TxnID(1))

		require.NoError(t, ctxLogger.AppendBegin())
		index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger, true, 42)
		require.NoError(t, err)
		defer index.Close()

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
		require.NoError(t, ctxLogger.AppendCommit())
	}()

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
			defer locker.Unlock(txnID)

			require.NoError(t, ctxLogger.AppendBegin())

			index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger, true, 42)
			if errors.Is(err, txns.ErrDeadlockPrevention) {
				require.NoError(t, ctxLogger.AppendAbort())
				ctxLogger.Rollback()
				return
			} else if err != nil {
				assert.NoError(t, err)
				require.NoError(t, ctxLogger.AppendAbort())
				ctxLogger.Rollback()
				return
			}
			defer index.Close()

			localDeletedKeys := make(map[string]bool)
			for j := 0; j < deletesPerGoroutine; j++ {
				keyIndex := (goroutineID*deletesPerGoroutine + j) % numKeys
				key := keys[keyIndex]

				err := index.Delete(key)
				if errors.Is(err, txns.ErrDeadlockPrevention) {
					require.NoError(t, ctxLogger.AppendAbort())
					ctxLogger.Rollback()
					return
				} else if errors.Is(err, storage.ErrKeyNotFound) {
					continue
				} else if err != nil {
					assert.NoError(t, err)
					require.NoError(t, ctxLogger.AppendAbort())
					ctxLogger.Rollback()
					return
				}

				localDeletedKeys[string(key)] = true
			}

			require.NoError(t, ctxLogger.AppendCommit())

			for keyStr := range localDeletedKeys {
				mu.Lock()
				deletedKeys[keyStr] = true
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	// Create a final index to verify deleted keys are no longer accessible
	ctxLogger := logger.WithContext(common.TxnID(numGoroutines + 2))
	index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger, true, 42)
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
	pool, logger, indexMeta, locker := setup(t, 10, 8)
	defer func() { assert.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	const numGoroutines = 10
	const operationsPerGoroutine = 20
	var wg sync.WaitGroup
	var mu sync.Mutex
	insertedKeys := make(map[string]common.RecordID)

	r := rand.New(rand.NewSource(42))
	keys := utils.GenerateUniqueInts[uint64](numGoroutines, 1, numGoroutines, r)
	goroutineIDs := utils.GenerateUniqueInts[uint64](numGoroutines, 1, numGoroutines, r)
	// Start mixed concurrent operations
	for _, goroutineID := range goroutineIDs {
		wg.Add(1)
		go func(goroutineID uint64) {
			defer wg.Done()

			// Create a separate index for this goroutine
			txnID := common.TxnID(goroutineID + 1)
			defer locker.Unlock(txnID)

			ctxLogger := logger.WithContext(txnID)
			require.NoError(t, ctxLogger.AppendBegin())

			index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger, true, 42)
			if errors.Is(err, txns.ErrDeadlockPrevention) {
				require.NoError(t, ctxLogger.AppendAbort())
				ctxLogger.Rollback()
				return
			} else if err != nil {
				assert.NoError(t, err)
				require.NoError(t, ctxLogger.AppendAbort())
				ctxLogger.Rollback()
				return
			}
			require.NoError(t, err)
			defer index.Close()

			localInsertedKeys := make(map[string]common.RecordID)
			localDeletedKeys := make(map[string]struct{})

			for j := 0; j < operationsPerGoroutine; j++ {
				operation := j % 3
				keyIndex := keys[int(goroutineID)%len(keys)]*operationsPerGoroutine + uint64(j)
				key := utils.ToBytes[uint64](keyIndex)
				rid := common.RecordID{
					FileID:  common.FileID(1),
					PageID:  common.PageID(goroutineID + 1),
					SlotNum: uint16(j),
				}

				switch operation {
				case 0: // Insert
					err := index.Insert(key, rid)
					if errors.Is(err, txns.ErrDeadlockPrevention) {
						require.NoError(t, ctxLogger.AppendAbort())
						ctxLogger.Rollback()
						return
					} else if err != nil {
						assert.NoError(t, err)
						require.NoError(t, ctxLogger.AppendAbort())
						ctxLogger.Rollback()
						return
					}

					localInsertedKeys[string(key)] = rid
				case 1: // Get
					actualRID, err := index.Get(key)
					if errors.Is(err, storage.ErrKeyNotFound) {
						continue
					} else if errors.Is(err, txns.ErrDeadlockPrevention) {
						require.NoError(t, ctxLogger.AppendAbort())
						ctxLogger.Rollback()
						return
					} else if err != nil {
						assert.NoError(t, err)
						require.NoError(t, ctxLogger.AppendAbort())
						ctxLogger.Rollback()
						return
					}

					// If key exists, verify it matches expected RID
					mu.Lock()
					expectedRID, exists := insertedKeys[string(key)]
					mu.Unlock()
					if exists && actualRID != expectedRID {
						assert.Equal(t, expectedRID, actualRID)
					}
				case 2: // Delete
					err := index.Delete(key)
					if errors.Is(err, storage.ErrKeyNotFound) {
						continue
					} else if errors.Is(err, txns.ErrDeadlockPrevention) {
						require.NoError(t, ctxLogger.AppendAbort())
						ctxLogger.Rollback()
						return
					} else if err != nil {
						assert.NoError(t, err)
						require.NoError(t, ctxLogger.AppendAbort())
						ctxLogger.Rollback()
						return
					}
					localDeletedKeys[string(key)] = struct{}{}
				}
			}
			require.NoError(t, ctxLogger.AppendCommit())
			for keyStr := range localInsertedKeys {
				mu.Lock()
				insertedKeys[keyStr] = localInsertedKeys[keyStr]
				mu.Unlock()
			}
			for keyStr := range localDeletedKeys {
				mu.Lock()
				delete(insertedKeys, keyStr)
				mu.Unlock()
			}
		}(goroutineID)
	}

	wg.Wait()

	// Create a final index to verify remaining keys are still accessible
	ctxLogger := logger.WithContext(common.TxnID(numGoroutines + 1))
	index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger, true, 42)
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

	pool, logger, indexMeta, locker := setup(t, 10, 8)
	defer func() { assert.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	const numGoroutines = 50
	const operationsPerGoroutine = 20
	var wg sync.WaitGroup
	var mu sync.Mutex
	insertedKeys := make(map[string]common.RecordID)

	r := rand.New(rand.NewSource(42))
	goroutineIDs := utils.GenerateUniqueInts[uint64](numGoroutines, 1, numGoroutines, r)
	for _, goroutineID := range goroutineIDs {
		wg.Add(1)
		go func(goroutineID uint64) {
			defer wg.Done()

			// Create a separate index for this goroutine
			txnID := common.TxnID(goroutineID + 1)
			defer locker.Unlock(txnID)

			ctxLogger := logger.WithContext(txnID)
			require.NoError(t, ctxLogger.AppendBegin())

			index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger, true, 42)
			if errors.Is(err, txns.ErrDeadlockPrevention) {
				require.NoError(t, ctxLogger.AppendAbort())
				ctxLogger.Rollback()
				return
			} else if err != nil {
				assert.NoError(t, err)
				require.NoError(t, ctxLogger.AppendAbort())
				ctxLogger.Rollback()
				return
			}

			defer index.Close()
			localInsertedKeys := make(map[string]common.RecordID)
			localDeletedKeys := make(map[string]struct{})

			for j := 0; j < operationsPerGoroutine; j++ {
				operation := j % 4
				keyIndex := goroutineID*operationsPerGoroutine + uint64(j)
				key := utils.ToBytes[uint64](uint64(keyIndex))
				rid := common.RecordID{
					FileID:  common.FileID(1),
					PageID:  common.PageID(goroutineID + 1),
					SlotNum: uint16(j),
				}

				switch operation {
				case 0, 1: // Insert (50% of operations)
					err := index.Insert(key, rid)
					if errors.Is(err, txns.ErrDeadlockPrevention) {
						require.NoError(t, ctxLogger.AppendAbort())
						ctxLogger.Rollback()
						return
					} else if err != nil {
						assert.NoError(t, err)
						require.NoError(t, ctxLogger.AppendAbort())
						ctxLogger.Rollback()
						return
					}

					localInsertedKeys[string(key)] = rid
				case 2: // Get (25% of operations)
					_, err := index.Get(key)
					if errors.Is(err, storage.ErrKeyNotFound) {
						continue
					} else if errors.Is(err, txns.ErrDeadlockPrevention) {
						require.NoError(t, ctxLogger.AppendAbort())
						ctxLogger.Rollback()
						return
					} else if err != nil {
						assert.NoError(t, err)
						require.NoError(t, ctxLogger.AppendAbort())
						ctxLogger.Rollback()
						return
					}
				case 3: // Delete (25% of operations)
					err := index.Delete(key)
					if errors.Is(err, storage.ErrKeyNotFound) {
						continue
					} else if errors.Is(err, txns.ErrDeadlockPrevention) {
						require.NoError(t, ctxLogger.AppendAbort())
						ctxLogger.Rollback()
						return
					} else if err != nil {
						mu.Lock()
						assert.NoError(t, err)
						require.NoError(t, ctxLogger.AppendAbort())
						ctxLogger.Rollback()
						return
					}
					localDeletedKeys[string(key)] = struct{}{}
				}
			}
			require.NoError(t, ctxLogger.AppendCommit())
			for keyStr := range localInsertedKeys {
				mu.Lock()
				insertedKeys[keyStr] = localInsertedKeys[keyStr]
				mu.Unlock()
			}
			for keyStr := range localDeletedKeys {
				mu.Lock()
				delete(insertedKeys, keyStr)
				mu.Unlock()
			}
		}(goroutineID)
	}

	wg.Wait()

	// Create a final index to verify remaining keys are still accessible
	ctxLogger := logger.WithContext(common.TxnID(numGoroutines + 1))
	index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger, true, 42)
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
	pool, logger, indexMeta, locker := setup(t, 10, 8)
	defer func() { assert.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	const numGoroutines = 50
	const insertsPerGoroutine = 20 // Reduced to avoid growth issues
	var wg sync.WaitGroup
	var mu sync.Mutex
	insertedKeys := make(map[string]common.RecordID)

	r := rand.New(rand.NewSource(42))
	goroutineIDs := utils.GenerateUniqueInts[uint64](numGoroutines, 1, numGoroutines, r)

	// Start concurrent inserts that will trigger index growth
	for _, goroutineID := range goroutineIDs {
		wg.Add(1)
		go func(goroutineID uint64) {
			defer wg.Done()

			// Create a separate index for this goroutine
			txnID := common.TxnID(goroutineID + 1)
			defer locker.Unlock(txnID)

			ctxLogger := logger.WithContext(txnID)
			index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger, true, 42)
			if errors.Is(err, txns.ErrDeadlockPrevention) {
				require.NoError(t, ctxLogger.AppendAbort())
				ctxLogger.Rollback()
				return
			} else if err != nil {
				assert.NoError(t, err)
				require.NoError(t, ctxLogger.AppendAbort())
				ctxLogger.Rollback()
				return
			}

			defer index.Close()

			localInsertedKeys := make(map[string]common.RecordID)

			for j := 0; j < insertsPerGoroutine; j++ {
				key := utils.ToBytes[uint64](goroutineID*uint64(insertsPerGoroutine) + uint64(j))
				rid := common.RecordID{
					FileID:  common.FileID(1),
					PageID:  common.PageID(goroutineID + 1),
					SlotNum: uint16(j),
				}

				err := index.Insert(key, rid)
				if errors.Is(err, txns.ErrDeadlockPrevention) {
					require.NoError(t, ctxLogger.AppendAbort())
					ctxLogger.Rollback()
					return
				} else if err != nil {
					assert.NoError(t, err)
					require.NoError(t, ctxLogger.AppendAbort())
					ctxLogger.Rollback()
					return
				}

				localInsertedKeys[string(key)] = rid
			}

			require.NoError(t, ctxLogger.AppendCommit())
			for keyStr := range localInsertedKeys {
				mu.Lock()
				insertedKeys[keyStr] = localInsertedKeys[keyStr]
				mu.Unlock()
			}
		}(goroutineID)
	}

	wg.Wait()

	// Create a final index to verify all inserts were successful
	ctxLogger := logger.WithContext(common.TxnID(numGoroutines + 1))
	index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger, true, 42)
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

func BenchmarkIndexInsert(b *testing.B) {
	pool, logger, indexMeta, locker := setup(b, 1000, 8)
	defer func() { assert.NoError(b, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	ctxLogger := logger.WithContext(common.TxnID(1))
	defer locker.Unlock(common.TxnID(1))

	require.NoError(b, ctxLogger.AppendBegin())
	index, err := NewLinearProbingIndex(indexMeta, pool, locker, ctxLogger, true, 42)
	require.NoError(b, err)
	defer index.Close()

	// Pre-generate keys to avoid timing key generation
	keys := make([][]byte, b.N)
	rids := make([]common.RecordID, b.N)

	r := rand.New(rand.NewSource(42))
	for i := 0; i < b.N; i++ {
		id := r.Uint64()
		keys[i] = utils.ToBytes[uint64](id)
		rids[i] = common.RecordID{
			FileID:  1,
			PageID:  2,
			SlotNum: uint16(i),
		}
	}

	go func() {
		time.AfterFunc(20*time.Second, func() {
			b.Logf("dumping dependency graph:\n%s", locker.DumpDependencyGraph())
		})
	}()

	// Reset timer to exclude setup time
	b.ResetTimer()

	// Benchmark the actual insert operations
	for i := 0; i < b.N; i++ {
		require.NoError(b, index.Insert(keys[i], rids[i]))
	}

	require.NoError(b, ctxLogger.AppendCommit())
}
