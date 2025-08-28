package hash

import (
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
	"github.com/Blackdeer1524/GraphDB/src/storage/index/hash/mocks"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
)

func makeSlot(key []byte, rid *common.RecordID, exist uint16, keySize int) []byte {
	b := make([]byte, 0, keySize+20)
	b = append(b, key...)
	b = append(b, utils.ToBytes(uint64(rid.FileID))...)
	b = append(b, utils.ToBytes(uint64(rid.PageID))...)
	b = append(b, utils.ToBytes(rid.SlotNum)...)
	b = append(b, utils.ToBytes(exist)...)

	return b
}

// Find tests

func TestBucketPage_Find(t *testing.T) {
	const keySize = 4
	key1 := []byte{1, 2, 3, 4}
	key2 := []byte{9, 9, 9, 9}

	rid1 := &common.RecordID{
		FileID:  10,
		PageID:  20,
		SlotNum: 30,
	}

	p := page.NewSlottedPage()

	p.Insert(utils.ToBytes(uint16(1)))

	p.Insert(makeSlot(key1, rid1, 1, keySize))

	p.Insert([]byte{42})

	p.Insert(makeSlot(key2, rid1, 0, keySize))

	bp := &bucketPage{p: p}

	t.Run("found", func(t *testing.T) {
		got, err := bp.find(key1)
		require.NoError(t, err)
		require.Equal(t, rid1, got)
	})

	t.Run("not found", func(t *testing.T) {
		_, err := bp.find([]byte{7, 7, 7, 7})
		require.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("deleted record ignored", func(t *testing.T) {
		_, err := bp.find(key2)
		require.ErrorIs(t, err, ErrKeyNotFound)
	})
}

func TestIndex_Get_KeySizeMismatch(t *testing.T) {
	se := &mocks.MockStorageEngine{}
	idx, _ := New(1, se, 4)

	_, err := idx.Get([]byte{1, 2}) // неверная длина
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key size is not equal")
}

func TestIndex_Get_Success(t *testing.T) {
	const keySize = 4
	key := []byte{1, 2, 3, 4}
	rid := &common.RecordID{FileID: 5, PageID: 6, SlotNum: 7}

	root := page.NewSlottedPage()
	root.Insert(utils.ToBytes(overflowPageNotExist))
	root.Insert(utils.ToBytes(uint16(1)))
	root.Insert(utils.ToBytes(uint64(10)))

	dir := page.NewSlottedPage()
	dir.Insert(utils.ToBytes(uint64(18)))
	dir.Insert(utils.ToBytes(uint64(20)))

	// bucket page
	bucket := page.NewSlottedPage()
	bucket.Insert(utils.ToBytes(uint16(1)))
	bucket.Insert(makeSlot(key, rid, 1, keySize))

	se := &mocks.MockStorageEngine{
		Pages: map[uint64]*page.SlottedPage{
			0:  root,
			10: dir,
			20: bucket,
		},
	}

	idx, err := New(1, se, keySize)
	require.NoError(t, err)

	got, err := idx.Get(key)
	require.NoError(t, err)
	require.Equal(t, rid, got)
}

func TestIndex_Get_KeyNotFound(t *testing.T) {
	const keySize = 4
	key := []byte{1, 2, 3, 4}

	root := page.NewSlottedPage()
	root.Insert(utils.ToBytes(overflowPageNotExist))
	root.Insert(utils.ToBytes(uint16(1)))
	root.Insert(utils.ToBytes(uint64(10)))

	dir := page.NewSlottedPage()
	dir.Insert(utils.ToBytes(uint64(18)))
	dir.Insert(utils.ToBytes(uint64(20)))

	bucket := page.NewSlottedPage()
	bucket.Insert(utils.ToBytes(uint16(1)))

	se := &mocks.MockStorageEngine{
		Pages: map[uint64]*page.SlottedPage{
			0:  root,
			10: dir,
			20: bucket,
		},
	}

	idx, err := New(1, se, keySize)
	require.NoError(t, err)

	_, err = idx.Get(key)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to find key")
}

// Delete tests

func TestBucketPage_Delete(t *testing.T) {
	const keySize = 4
	key1 := []byte{1, 2, 3, 4}
	key2 := []byte{9, 9, 9, 9}

	rid1 := common.RecordID{
		FileID:  10,
		PageID:  20,
		SlotNum: 30,
	}

	p := page.NewSlottedPage()

	p.Insert(utils.ToBytes(uint16(1)))

	p.Insert(makeSlot(key1, &rid1, 1, keySize))

	p.Insert([]byte{42})

	p.Insert(makeSlot(key2, &rid1, 0, keySize))

	bp := &bucketPage{p: p}

	t.Run("deleted", func(t *testing.T) {
		got, err := bp.delete(key1)
		require.NoError(t, err)
		require.Equal(t, rid1, got)

		// check exist flag
		slot := p.Read(1)
		base := keySize
		ext := utils.FromBytes[uint16](slot[base+18 : base+20])

		require.Equal(t, uint16(0), ext)
	})

	t.Run("not found", func(t *testing.T) {
		_, err := bp.delete([]byte{7, 7, 7, 7})
		require.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("already deleted record ignored", func(t *testing.T) {
		_, err := bp.delete(key2)
		require.ErrorIs(t, err, ErrKeyNotFound)
	})
}

func TestIndex_Delete_KeySizeMismatch(t *testing.T) {
	se := &mocks.MockStorageEngine{}
	idx, _ := New(1, se, 4)

	_, err := idx.Delete([]byte{1, 2})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key size is not equal")
}

func TestIndex_Delete_Success(t *testing.T) {
	const keySize = 4
	key := []byte{1, 2, 3, 4}
	rid := common.RecordID{FileID: 5, PageID: 6, SlotNum: 7}

	root := page.NewSlottedPage()
	root.Insert(utils.ToBytes(overflowPageNotExist))
	root.Insert(utils.ToBytes(uint16(1)))
	root.Insert(utils.ToBytes(uint64(10)))

	dir := page.NewSlottedPage()
	dir.Insert(utils.ToBytes(uint64(18)))
	dir.Insert(utils.ToBytes(uint64(20)))

	// bucket page
	bucket := page.NewSlottedPage()
	bucket.Insert(utils.ToBytes(uint16(1)))
	bucket.Insert(makeSlot(key, &rid, 1, keySize))

	se := &mocks.MockStorageEngine{
		Pages: map[uint64]*page.SlottedPage{
			0:  root,
			10: dir,
			20: bucket,
		},
	}

	idx, err := New(1, se, keySize)
	require.NoError(t, err)

	got, err := idx.Delete(key)
	require.NoError(t, err)
	require.Equal(t, rid, got)

	// check exist flag in bucket
	slot := bucket.Read(1)
	base := keySize
	ext := utils.FromBytes[uint16](slot[base+18 : base+20])
	require.Equal(t, uint16(0), ext)
}

func TestIndex_Delete_KeyNotFound(t *testing.T) {
	const keySize = 4
	key := []byte{1, 2, 3, 4}

	root := page.NewSlottedPage()
	root.Insert(utils.ToBytes(overflowPageNotExist))
	root.Insert(utils.ToBytes(uint16(1)))
	root.Insert(utils.ToBytes(uint64(10)))

	dir := page.NewSlottedPage()
	dir.Insert(utils.ToBytes(uint64(18)))
	dir.Insert(utils.ToBytes(uint64(20)))

	bucket := page.NewSlottedPage()
	bucket.Insert(utils.ToBytes(uint16(1)))

	se := &mocks.MockStorageEngine{
		Pages: map[uint64]*page.SlottedPage{
			0:  root,
			10: dir,
			20: bucket,
		},
	}

	idx, err := New(1, se, keySize)
	require.NoError(t, err)

	_, err = idx.Delete(key)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to delete key")
}

// Insert tests

func TestBucketPage_Insert(t *testing.T) {
	const keySize = 4
	key1 := []byte{1, 2, 3, 4}
	key2 := []byte{9, 9, 9, 9}

	rid1 := common.RecordID{
		FileID:  10,
		PageID:  20,
		SlotNum: 30,
	}
	rid2 := common.RecordID{
		FileID:  11,
		PageID:  21,
		SlotNum: 31,
	}

	t.Run("insert success", func(t *testing.T) {
		p := page.NewSlottedPage()
		p.Insert(utils.ToBytes(uint16(1))) // local depth

		bp := &bucketPage{p: p}
		ok, err := bp.insert(key1, rid1, 10)
		require.NoError(t, err)
		require.True(t, ok)

		d, err := bp.find(key1)
		require.NoError(t, err)
		require.Equal(t, rid1, *d)

		slot := p.Read(1)
		require.Equal(t, key1, slot[:keySize])

		gotRid := common.RecordID{
			FileID:  common.FileID(utils.FromBytes[uint64](slot[keySize : keySize+8])),
			PageID:  common.PageID(utils.FromBytes[uint64](slot[keySize+8 : keySize+16])),
			SlotNum: utils.FromBytes[uint16](slot[keySize+16 : keySize+18]),
		}
		require.Equal(t, rid1, gotRid)

		exist := utils.FromBytes[uint16](slot[keySize+18 : keySize+20])
		require.Equal(t, uint16(1), exist)
	})

	t.Run("respect deleted slot (reuse)", func(t *testing.T) {
		p := page.NewSlottedPage()
		p.Insert(utils.ToBytes(uint16(1)))

		bp := &bucketPage{p: p}
		_, err := bp.insert(key1, rid1, 10)
		require.NoError(t, err)

		d, err := bp.find(key1)
		require.NoError(t, err)
		require.Equal(t, rid1, *d)

		_, err = bp.delete(key1)
		require.NoError(t, err)

		ok, err := bp.insert(key1, rid1, 10)
		require.NoError(t, err)
		require.True(t, ok)

		d, err = bp.find(key1)
		require.NoError(t, err)
		require.Equal(t, rid1, *d)
	})

	t.Run("bucket overflow", func(t *testing.T) {
		p := page.NewSlottedPage()
		p.Insert(utils.ToBytes(uint16(1)))

		bp := &bucketPage{p: p}

		_, err := bp.insert(key1, rid1, 1)
		require.NoError(t, err)

		d, err := bp.find(key1)
		require.NoError(t, err)
		require.Equal(t, rid1, *d)

		ok, err := bp.insert(key2, rid2, 1)
		require.Error(t, err)
		require.False(t, ok)
		require.Contains(t, err.Error(), "bucket page is full")

		d, err = bp.find(key1)
		require.NoError(t, err)
		require.Equal(t, rid1, *d)
	})
}
