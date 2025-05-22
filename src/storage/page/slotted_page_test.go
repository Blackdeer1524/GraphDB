package page

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInsertAndGet(t *testing.T) {
	page := NewSlottedPage(0, 0)

	records := [][]byte{
		[]byte("alpha"),
		[]byte("beta"),
		[]byte("gamma"),
	}

	var slotIDs []uint32

	for _, rec := range records {
		id, err := page.Insert(rec)
		require.NoError(t, err, "Insert failed")
		slotIDs = append(slotIDs, id)
	}

	for i, id := range slotIDs {
		got, err := page.Get(id)
		require.NoError(t, err, "Get failed")
		assert.Equal(t, string(records[i]), string(got), "Retrieved record doesn't match")
	}
}

func TestFreeSpaceReduction(t *testing.T) {
	page := NewSlottedPage(0, 0)
	initialFree := page.freeEnd() - page.freeStart()

	_, err := page.Insert([]byte("1234567890"))
	require.NoError(t, err, "Insert failed")

	used := page.freeEnd() - page.freeStart()
	assert.Less(t, used, initialFree, "Free space did not reduce correctly")
}

func TestInsertTooLarge(t *testing.T) {
	page := NewSlottedPage(0, 0)

	tooBig := make([]byte, Size)
	_, err := page.Insert(tooBig)
	assert.Error(t, err, "Expected error when inserting too large record")
}

func TestInvalidSlotID(t *testing.T) {
	page := NewSlottedPage(0, 0)

	_, err := page.Get(999)
	assert.Error(t, err, "Expected error for invalid slot ID")
}
