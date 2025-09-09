package index

import (
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
