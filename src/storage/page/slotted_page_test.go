package page

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	. "github.com/Blackdeer1524/GraphDB/src/pkg/optional"
)

func TestInsertAndGet(t *testing.T) {
	page := NewSlottedPage()

	records := [][]byte{
		[]byte("alpha"),
		[]byte("beta"),
		[]byte("gamma"),
	}

	var slotIDs []uint16

	for _, rec := range records {
		slot := page.InsertPrepare(rec)
		require.NotEqual(t, slot, None[uint16]())
		page.InsertCommit(slot.Unwrap())
		slotIDs = append(slotIDs, slot.Unwrap())
	}

	for i, id := range slotIDs {
		got := page.Read(id)
		assert.Equal(
			t,
			string(records[i]),
			string(got),
			"Retrieved record doesn't match",
		)
	}
}

func TestInsertAndGetLarger(t *testing.T) {
	page := NewSlottedPage()
	i := 0
	for {
		handle := page.InsertPrepare([]byte(strconv.Itoa(i)))
		if handle.IsNone() {
			break
		}
		page.InsertCommit(handle.Unwrap())
		i++
	}

	for j := range i {
		data := page.Read(uint16(j))
		expected := []byte(strconv.Itoa(j))
		assert.Equal(t, expected, data)
	}
}

func TestFreeSpaceReduction(t *testing.T) {
	page := NewSlottedPage()
	initialFree := page.getHeader().freeEnd - page.getHeader().freeStart

	slot := page.InsertPrepare([]byte("1234567890"))
	page.InsertCommit(slot.Unwrap())

	used := page.getHeader().freeEnd - page.getHeader().freeStart
	assert.Less(t, used, initialFree, "Free space did not reduce correctly")
}

func TestInsertTooLarge(t *testing.T) {
	page := NewSlottedPage()

	tooBig := make([]byte, PageSize)
	handle := page.InsertPrepare(tooBig)
	assert.Equal(t, None[uint16](), handle)
}

func TestInvalidSlotID(t *testing.T) {
	page := NewSlottedPage()
	assert.Panicsf(t,
		func() {
			_ = page.Read(uint16(999))
		},
		"123",
	)
}
func TestUpdateSuccess(t *testing.T) {
	page := NewSlottedPage()
	orig := []byte("original")
	slot := page.InsertPrepare(orig)
	require.NotEqual(t, slot, None[uint16]())
	page.InsertCommit(slot.Unwrap())

	newData := []byte("changed0")
	page.Update(slot.Unwrap(), newData)

	got := page.Read(slot.Unwrap())
	assert.Equal(t, newData, got)
}

func TestUpdateTooLarge(t *testing.T) {
	page := NewSlottedPage()
	orig := []byte("short")
	slot := page.InsertPrepare(orig)
	require.NotEqual(t, slot, None[uint16]())
	page.InsertCommit(slot.Unwrap())

	newData := []byte("this is too long")
	require.Panics(t, func() {
		page.Update(slot.Unwrap(), newData)
	}, "Update should fail when newData is too large")

	got := page.Read(slot.Unwrap())
	assert.NotEqual(t, newData, got)
	assert.Equal(t, len(orig), len(got))
}

func TestDeleteRemovesData(t *testing.T) {
	page := NewSlottedPage()
	orig := []byte("todelete")
	slot := page.InsertPrepare(orig)
	require.NotEqual(t, slot, None[uint16]())
	page.InsertCommit(slot.Unwrap())

	page.Delete(slot.Unwrap())

	// After delete, the slot status should be Deleted
	header := page.getHeader()
	ptr := header.getSlots()[slot.Unwrap()]
	assert.Equal(
		t,
		SlotStatusDeleted,
		ptr.slotInfo(),
		"Slot status should be Deleted",
	)

	data := page.getBytesBySlotPtr(ptr)
	expected := make([]byte, len(orig))
	assert.NotEqual(
		t,
		expected,
		data,
		"Data should NOT be cleared after delete",
	)

	// Trying to GetBytes should panic
	assert.Panics(t, func() {
		_ = page.Read(slot.Unwrap())
	})
}

func TestDeleteInvalidSlotPanics(t *testing.T) {
	page := NewSlottedPage()
	assert.Panics(t, func() {
		page.Delete(1234)
	})
}

func TestDeleteTwicePanics(t *testing.T) {
	page := NewSlottedPage()
	orig := []byte("doubledelete")
	slot := page.InsertPrepare(orig)
	require.NotEqual(t, slot, None[uint16]())
	page.InsertCommit(slot.Unwrap())

	page.Delete(slot.Unwrap())
	assert.Panics(t, func() {
		page.Delete(slot.Unwrap())
	})
}

func TestGetBytes_ValidSlot(t *testing.T) {
	page := NewSlottedPage()
	data := []byte("testdata")
	slot := page.InsertPrepare(data)
	require.NotEqual(t, slot, None[uint16]())
	page.InsertCommit(slot.Unwrap())

	got := page.Read(slot.Unwrap())
	assert.Equal(t, data, got, "GetBytes should return the correct data")
}

func TestGetBytes_InvalidSlotID_Panics(t *testing.T) {
	page := NewSlottedPage()
	assert.Panics(t, func() {
		_ = page.Read(9999)
	}, "GetBytes should panic for invalid slotID")
}

func TestGetBytes_NotCommitted_Panics(t *testing.T) {
	page := NewSlottedPage()
	data := []byte("pending")
	slot := page.InsertPrepare(data)
	// Not calling InsertCommit
	assert.Panics(t, func() {
		_ = page.Read(slot.Unwrap())
	}, "GetBytes should panic if slot is not committed")
}

func TestGetBytes_DeletedSlot_Panics(t *testing.T) {
	page := NewSlottedPage()
	data := []byte("todelete")
	slot := page.InsertPrepare(data)
	require.NotEqual(t, slot, None[uint16]())
	page.InsertCommit(slot.Unwrap())
	page.Delete(slot.Unwrap())

	assert.Panics(t, func() {
		_ = page.Read(slot.Unwrap())
	}, "GetBytes should panic if slot is deleted")
}

func TestUndoDelete_RestoresDataAndStatus(t *testing.T) {
	page := NewSlottedPage()
	orig := []byte("restoreme")
	slot := page.InsertPrepare(orig)
	require.NotEqual(t, slot, None[uint16]())
	page.InsertCommit(slot.Unwrap())

	// Delete the slot
	page.Delete(slot.Unwrap())
	page.UndoDelete(slot.Unwrap())

	// Slot status should be Inserted again
	header := page.getHeader()
	ptr := header.getSlots()[slot.Unwrap()]
	assert.Equal(
		t,
		SlotStatusInserted,
		ptr.slotInfo(),
		"Slot status should be Inserted after UndoDelete",
	)

	// Data should be restored
	got := page.Read(slot.Unwrap())
	assert.Equal(
		t,
		orig,
		got,
		"Data should be restored after UndoDelete",
	)
}

func TestUndoDelete_PanicsIfSlotNotDeleted(t *testing.T) {
	page := NewSlottedPage()
	orig := []byte("notdeleted")
	slot := page.InsertPrepare(orig)
	require.NotEqual(t, slot, None[uint16]())
	page.InsertCommit(slot.Unwrap())

	// Try UndoDelete on a slot that is not deleted
	assert.Panics(t, func() {
		page.UndoDelete(slot.Unwrap())
	}, "UndoDelete should panic if slot is not deleted")
}

func TestUndoDelete_PanicsIfSlotIDTooLarge(t *testing.T) {
	page := NewSlottedPage()
	assert.Panics(t, func() {
		page.UndoDelete(9999)
	}, "UndoDelete should panic if slotID is too large")
}
