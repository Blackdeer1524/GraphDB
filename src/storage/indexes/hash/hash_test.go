package hash

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/Blackdeer1524/GraphDB/src/storage/page"
	"github.com/Blackdeer1524/GraphDB/src/txns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func Test_getRootMetadata(t *testing.T) {
	t.Run("SimpleTest", func(t *testing.T) {
		m := rootMetaData{
			magic:         [4]byte{'h', 'i', 'd', 'x'},
			globalDepth:   2,
			maxBucketSize: 2,
			directory:     []uint64{1, 2, 3, 4},
			checksum:      5,
		}

		data := rootPageMetadata(m)

		require.Equal(t, m, getRootPageMetadata(data))
	})
}

func TestBucketPageMarshalUnmarshal(t *testing.T) {
	t.Run("NormalBucket", func(t *testing.T) {
		bucket := BucketPage[uint64]{
			localDepth: 2,
			entriesCnt: 3,
			entries: []KeyWithRID[uint64]{
				{key: 42, rid: RID{PageID: 1, SlotID: 2}},
				{key: 100, rid: RID{PageID: 3, SlotID: 4}},
				{key: 999, rid: RID{PageID: 5, SlotID: 6}},
			},
		}

		data, err := bucketPageToBytes(bucket)
		assert.NoError(t, err, "Serialization should succeed")
		assert.Len(t, data, page.PageSize, "Serialized data should match page size")

		result := getBucketPage[uint64](data)

		assert.Equal(t, bucket.localDepth, result.localDepth, "localDepth should match")
		assert.Equal(t, bucket.entriesCnt, result.entriesCnt, "entriesCnt should match")
		assert.Equal(t, len(bucket.entries), len(result.entries), "Number of entries should match")
		for i, entry := range bucket.entries {
			assert.Equal(t, entry.key, result.entries[i].key, "Key at index %d should match", i)
			assert.Equal(t, entry.rid, result.entries[i].rid, "RID at index %d should match", i)
		}
	})

	t.Run("EmptyBucket", func(t *testing.T) {
		bucket := BucketPage[uint64]{
			localDepth: 1,
			entriesCnt: 0,
			entries:    []KeyWithRID[uint64]{},
		}

		data, err := bucketPageToBytes(bucket)
		assert.NoError(t, err, "Serialization should succeed")
		assert.Len(t, data, page.PageSize, "Serialized data should match page size")

		result := getBucketPage[uint64](data)

		assert.Equal(t, bucket.localDepth, result.localDepth, "localDepth should match")
		assert.Equal(t, bucket.entriesCnt, result.entriesCnt, "entriesCnt should match")
		assert.Empty(t, result.entries, "Entries should be empty")
	})

	t.Run("ExceedsPageSize", func(t *testing.T) {
		bucket := BucketPage[uint64]{
			localDepth: 2,
			entriesCnt: 1000,
			entries:    make([]KeyWithRID[uint64], 1000),
		}
		for i := 0; i < 1000; i++ {
			bucket.entries[i] = KeyWithRID[uint64]{
				key: uint64(i),
				rid: RID{PageID: uint64(i + 1), SlotID: uint16(i + 1)},
			}
		}

		_, err := bucketPageToBytes(bucket)
		assert.Error(t, err, "Serialization should fail due to size")
		assert.Contains(t, err.Error(), "bucket data exceeds page size")
	})

	t.Run("SingleEntry", func(t *testing.T) {
		// Подготавливаем BucketPage с одной записью
		bucket := BucketPage[uint64]{
			localDepth: 3,
			entriesCnt: 1,
			entries: []KeyWithRID[uint64]{
				{key: 12345, rid: RID{PageID: 10, SlotID: 20}},
			},
		}

		data, err := bucketPageToBytes(bucket)
		assert.NoError(t, err, "Serialization should succeed")
		assert.Len(t, data, page.PageSize, "Serialized data should match page size")

		result := getBucketPage[uint64](data)

		assert.Equal(t, bucket.localDepth, result.localDepth, "localDepth should match")
		assert.Equal(t, bucket.entriesCnt, result.entriesCnt, "entriesCnt should match")
		assert.Equal(t, len(bucket.entries), len(result.entries), "Number of entries should match")
		for i, entry := range bucket.entries {
			assert.Equal(t, entry.key, result.entries[i].key, "Key at index %d should match", i)
			assert.Equal(t, entry.rid, result.entries[i].rid, "RID at index %d should match", i)
		}
	})
}

func resetMocks(mocks ...*mock.Mock) {
	for i := range mocks {
		mocks[i].ExpectedCalls = nil
		mocks[i].Calls = nil
	}
}

func TestIndexSearch(t *testing.T) {
	se := new(mockStorageEngine)
	locker := new(mockLocker)

	rootData := make([]byte, page.PageSize)
	rootData[0] = 'I'
	rootData[1] = 'N'
	rootData[2] = 'D'
	rootData[3] = 'X'
	binary.LittleEndian.PutUint32(rootData[4:], 2)                   // globalDepth = 2
	binary.LittleEndian.PutUint32(rootData[8:], 100)                 // maxBucketSize = 100
	binary.LittleEndian.PutUint64(rootData[12:], 1)                  // directory[0] = PageID(1)
	binary.LittleEndian.PutUint64(rootData[20:], 2)                  // directory[1] = PageID(2)
	binary.LittleEndian.PutUint64(rootData[28:], 3)                  // directory[2] = PageID(3)
	binary.LittleEndian.PutUint64(rootData[36:], 4)                  // directory[3] = PageID(4)
	binary.LittleEndian.PutUint64(rootData[page.PageSize-8:], 12345) // checksum
	rootPage := &mockPage{data: rootData}

	index := &Index[*mockPage, uint64]{
		se:      se,
		indexID: 1,
		lock:    locker,
	}

	t.Run("SuccessfulSearch", func(t *testing.T) {
		resetMocks(&locker.Mock, &se.Mock)

		bucket := BucketPage[uint64]{
			localDepth: 2,
			entriesCnt: 2,
			entries: []KeyWithRID[uint64]{
				{key: 42, rid: RID{PageID: 1, SlotID: 2}},
				{key: 100, rid: RID{PageID: 3, SlotID: 4}},
			},
		}
		bucketData, _ := bucketPageToBytes(bucket)
		bucketPage := &mockPage{data: bucketData}

		locker.On("GetPageLock", mock.Anything, uint64(idxKind)).Return(true)
		locker.On("GetPageUnlock", mock.Anything, uint64(idxKind)).Return(true)
		locker.On("GetPageLock", mock.Anything, uint64(idxKind)).Return(true)
		locker.On("GetPageUnlock", mock.Anything, uint64(idxKind)).Return(true)

		se.On("GetPage", uint64(idxKind), uint64(1), uint64(indexRootPageID)).Return(rootPage, nil)
		se.On("UnpinPage", uint64(idxKind), uint64(1), uint64(indexRootPageID)).Return(nil)
		se.On("GetPage", uint64(idxKind), uint64(1), uint64(4)).Return(bucketPage, nil)
		se.On("UnpinPage", uint64(idxKind), uint64(1), uint64(4)).Return(nil)

		rid, err := index.Search(1, 42)
		assert.NoError(t, err)
		assert.Equal(t, RID{PageID: 1, SlotID: 2}, rid)

		locker.AssertExpectations(t)
		se.AssertExpectations(t)
	})

	t.Run("KeyNotFound", func(t *testing.T) {
		resetMocks(&locker.Mock, &se.Mock)

		bucket := BucketPage[uint64]{
			localDepth: 2,
			entriesCnt: 1,
			entries: []KeyWithRID[uint64]{
				{key: 100, rid: RID{PageID: 3, SlotID: 4}},
			},
		}
		bucketData, _ := bucketPageToBytes(bucket)
		bucketPage := &mockPage{data: bucketData}

		locker.On("GetPageLock", mock.Anything, uint64(idxKind)).Return(true)
		locker.On("GetPageUnlock", mock.Anything, uint64(idxKind)).Return(true)
		locker.On("GetPageLock", mock.Anything, uint64(idxKind)).Return(true)
		locker.On("GetPageUnlock", mock.Anything, uint64(idxKind)).Return(true)

		se.On("GetPage", uint64(idxKind), uint64(1), uint64(indexRootPageID)).Return(rootPage, nil)
		se.On("UnpinPage", uint64(idxKind), uint64(1), uint64(indexRootPageID)).Return(nil)
		se.On("GetPage", uint64(idxKind), uint64(1), uint64(4)).Return(bucketPage, nil)
		se.On("UnpinPage", uint64(idxKind), uint64(1), uint64(4)).Return(nil)

		rid, err := index.Search(1, 42)
		assert.ErrorIs(t, err, ErrNotFound)
		assert.Equal(t, RID{}, rid)

		locker.AssertExpectations(t)
		se.AssertExpectations(t)
	})

	t.Run("RootLockFailure", func(t *testing.T) {
		resetMocks(&locker.Mock, &se.Mock)

		locker.On("GetPageLock", mock.Anything, uint64(idxKind)).Return(false)

		rid, err := index.Search(1, 42)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get page lock for index root")
		assert.Equal(t, RID{}, rid)

		locker.AssertExpectations(t)
		se.AssertExpectations(t)
	})

	t.Run("RootPageFailure", func(t *testing.T) {
		resetMocks(&locker.Mock, &se.Mock)

		locker.On("GetPageLock", mock.Anything, uint64(idxKind)).Return(true)
		locker.On("GetPageUnlock", mock.Anything, uint64(idxKind)).Return(true)

		se.On("GetPage", uint64(idxKind), uint64(1), uint64(indexRootPageID)).Return(
			&mockPage{}, errors.New("root page not found"))

		rid, err := index.Search(1, 42)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get root page")
		assert.Equal(t, RID{}, rid)

		locker.AssertExpectations(t)
		se.AssertExpectations(t)
	})

	t.Run("BucketLockFailure", func(t *testing.T) {
		resetMocks(&locker.Mock, &se.Mock)

		rootLockReq := txns.IndexLockRequest{
			TxnID:    1,
			LockMode: txns.IndexShared,
			PageID:   indexRootPageID,
		}

		locker.On("GetPageLock", rootLockReq, uint64(idxKind)).Return(true)
		locker.On("GetPageUnlock", rootLockReq, uint64(idxKind)).Return(true)
		locker.On("GetPageLock", mock.Anything, uint64(idxKind)).Return(false)

		se.On("GetPage", uint64(idxKind), uint64(1), uint64(indexRootPageID)).Return(rootPage, nil)
		se.On("UnpinPage", uint64(idxKind), uint64(1), uint64(indexRootPageID)).Return(nil)

		rid, err := index.Search(1, 42)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get page lock for bucket")
		assert.Equal(t, RID{}, rid)

		locker.AssertExpectations(t)
		se.AssertExpectations(t)
	})

	t.Run("GetPageFailure", func(t *testing.T) {
		resetMocks(&locker.Mock, &se.Mock)

		locker.On("GetPageLock", mock.Anything, mock.Anything).Return(true)
		locker.On("GetPageUnlock", mock.Anything, mock.Anything).Return(true)

		se.On("GetPage", uint64(idxKind), uint64(1), uint64(indexRootPageID)).Return(rootPage, nil)
		se.On("UnpinPage", uint64(idxKind), uint64(1), uint64(indexRootPageID)).Return(nil)
		se.On("GetPage", uint64(idxKind), uint64(1), uint64(4)).Return(&mockPage{}, errors.New("page not found"))

		rid, err := index.Search(1, 42)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get page: page not found")
		assert.Equal(t, RID{}, rid)

		locker.AssertExpectations(t)
		se.AssertExpectations(t)
	})

	t.Run("UnpinPageFailure", func(t *testing.T) {
		resetMocks(&locker.Mock, &se.Mock)

		bucket := BucketPage[uint64]{
			localDepth: 2,
			entriesCnt: 2,
			entries: []KeyWithRID[uint64]{
				{key: 42, rid: RID{PageID: 1, SlotID: 2}},
				{key: 100, rid: RID{PageID: 3, SlotID: 4}},
			},
		}
		bucketData, _ := bucketPageToBytes(bucket)
		bucketPage := &mockPage{data: bucketData}

		locker.On("GetPageLock", mock.Anything, mock.Anything).Return(true)
		locker.On("GetPageUnlock", mock.Anything, mock.Anything).Return(true)

		se.On("GetPage", uint64(idxKind), uint64(1), uint64(indexRootPageID)).Return(rootPage, nil)
		se.On("UnpinPage", uint64(idxKind), uint64(1), uint64(indexRootPageID)).Return(nil)
		se.On("GetPage", uint64(idxKind), uint64(1), uint64(4)).Return(bucketPage, nil)
		se.On("UnpinPage", uint64(idxKind), uint64(1), uint64(4)).Return(errors.New("unpin failed"))

		_, err := index.Search(1, 42)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unpin failed")

		locker.AssertExpectations(t)
		se.AssertExpectations(t)
	})
}

func TestDelete(t *testing.T) {
	se := new(mockStorageEngine)
	locker := new(mockLocker)

	rootData := make([]byte, page.PageSize)
	rootData[0] = 'I'
	rootData[1] = 'N'
	rootData[2] = 'D'
	rootData[3] = 'X'
	binary.LittleEndian.PutUint32(rootData[4:], 2)                   // globalDepth = 2
	binary.LittleEndian.PutUint32(rootData[8:], 100)                 // maxBucketSize = 100
	binary.LittleEndian.PutUint64(rootData[12:], 1)                  // directory[0] = PageID(1)
	binary.LittleEndian.PutUint64(rootData[20:], 2)                  // directory[1] = PageID(2)
	binary.LittleEndian.PutUint64(rootData[28:], 3)                  // directory[2] = PageID(3)
	binary.LittleEndian.PutUint64(rootData[36:], 4)                  // directory[3] = PageID(4)
	binary.LittleEndian.PutUint64(rootData[page.PageSize-8:], 12345) // checksum
	rootPage := &mockPage{data: rootData}

	index := &Index[*mockPage, uint64]{
		se:      se,
		indexID: 1,
		lock:    locker,
	}

	t.Run("SuccessfulSearch", func(t *testing.T) {
		resetMocks(&locker.Mock, &se.Mock)

		bucket := BucketPage[uint64]{
			localDepth: 2,
			entriesCnt: 2,
			entries: []KeyWithRID[uint64]{
				{key: 42, rid: RID{PageID: 1, SlotID: 2}},
				{key: 100, rid: RID{PageID: 3, SlotID: 4}},
			},
		}
		bucketData, _ := bucketPageToBytes(bucket)
		bucketPage := &mockPage{data: bucketData}

		nBucket := BucketPage[uint64]{
			localDepth: 2,
			entriesCnt: 1,
			entries: []KeyWithRID[uint64]{
				{key: 100, rid: RID{PageID: 3, SlotID: 4}},
			},
		}
		nBucketData, _ := bucketPageToBytes(nBucket)
		nBucketPage := &mockPage{data: nBucketData}

		nBucketPage.SetDirtiness(true)

		locker.On("GetPageLock", mock.Anything, uint64(idxKind)).Return(true)
		locker.On("GetPageUnlock", mock.Anything, uint64(idxKind)).Return(true)
		locker.On("GetPageLock", mock.Anything, uint64(idxKind)).Return(true)
		locker.On("GetPageUnlock", mock.Anything, uint64(idxKind)).Return(true)

		se.On("GetPage", uint64(idxKind), uint64(1), uint64(indexRootPageID)).Return(rootPage, nil)
		se.On("UnpinPage", uint64(idxKind), uint64(1), uint64(indexRootPageID)).Return(nil)
		se.On("GetPage", uint64(idxKind), uint64(1), uint64(4)).Return(bucketPage, nil)
		se.On("UnpinPage", uint64(idxKind), uint64(1), uint64(4)).Return(nil)

		se.On("WritePage", uint64(idxKind), uint64(1), uint64(4), nBucketPage).Return(nil)

		err := index.Delete(1, uint64(42))
		assert.NoError(t, err)

		locker.AssertExpectations(t)
		se.AssertExpectations(t)
	})

	t.Run("SameSecondDeleteIsError", func(t *testing.T) {
		resetMocks(&locker.Mock, &se.Mock)

		bucket := BucketPage[uint64]{
			localDepth: 2,
			entriesCnt: 2,
			entries: []KeyWithRID[uint64]{
				{key: 42, rid: RID{PageID: 1, SlotID: 2}},
				{key: 100, rid: RID{PageID: 3, SlotID: 4}},
			},
		}
		bucketData, _ := bucketPageToBytes(bucket)
		bucketPage := &mockPage{data: bucketData}

		nBucket := BucketPage[uint64]{
			localDepth: 2,
			entriesCnt: 1,
			entries: []KeyWithRID[uint64]{
				{key: 100, rid: RID{PageID: 3, SlotID: 4}},
			},
		}
		nBucketData, _ := bucketPageToBytes(nBucket)
		nBucketPage := &mockPage{data: nBucketData}

		nBucketPage.SetDirtiness(true)

		locker.On("GetPageLock", mock.Anything, uint64(idxKind)).Return(true)
		locker.On("GetPageUnlock", mock.Anything, uint64(idxKind)).Return(true)
		locker.On("GetPageLock", mock.Anything, uint64(idxKind)).Return(true)
		locker.On("GetPageUnlock", mock.Anything, uint64(idxKind)).Return(true)

		se.On("GetPage", uint64(idxKind), uint64(1), uint64(indexRootPageID)).Return(rootPage, nil)
		se.On("UnpinPage", uint64(idxKind), uint64(1), uint64(indexRootPageID)).Return(nil)
		se.On("GetPage", uint64(idxKind), uint64(1), uint64(4)).Return(bucketPage, nil)
		se.On("UnpinPage", uint64(idxKind), uint64(1), uint64(4)).Return(nil)

		se.On("WritePage", uint64(idxKind), uint64(1), uint64(4), nBucketPage).Return(nil)

		err := index.Delete(1, uint64(42))
		assert.NoError(t, err)

		locker.AssertExpectations(t)
		se.AssertExpectations(t)

		err = index.Delete(1, uint64(42))
		assert.ErrorIs(t, err, ErrNotFound)
	})
}
