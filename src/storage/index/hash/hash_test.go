package hash

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
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

type mockStorageEngine struct {
	pages map[uint64]*page.SlottedPage
	err   error
}

func (m *mockStorageEngine) GetPage(pageID uint64, fileID uint64) (*page.SlottedPage, error) {
	if m.err != nil {
		return nil, m.err
	}
	p, ok := m.pages[pageID]
	if !ok {
		return nil, errors.New("page not found")
	}
	return p, nil
}
func (m *mockStorageEngine) UnpinPage(pageID uint64, fileID uint64) error { return nil }

func (m *mockStorageEngine) WritePage(pageID uint64, fileID uint64, p *page.SlottedPage) error {
	m.pages[pageID] = p

	return nil
}

func (m *mockStorageEngine) Unpin(pageID common.PageIdentity) {
	return
}

func TestIndex_Get_KeySizeMismatch(t *testing.T) {
	se := &mockStorageEngine{}
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

	se := &mockStorageEngine{
		pages: map[uint64]*page.SlottedPage{
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

	se := &mockStorageEngine{
		pages: map[uint64]*page.SlottedPage{
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
