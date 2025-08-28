package recovery

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
)

func TestBeginLogRecord_MarshalUnmarshal(t *testing.T) {
	original := NewBeginLogRecord(123, common.TxnID(456))

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	var recovered BeginLogRecord
	if err := recovered.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}

	if original.lsn != recovered.lsn {
		t.Errorf("lsn mismatch: got %v, want %v", recovered.lsn, original.lsn)
	}

	if original.txnID != recovered.txnID {
		t.Errorf(
			"TransactionID mismatch: got %v, want %v",
			recovered.txnID,
			original.txnID,
		)
	}
}

func TestUpdateLogRecord_MarshalUnmarshal(t *testing.T) {
	original := NewUpdateLogRecord(
		123,
		common.TxnID(456),
		common.LogRecordLocInfo{
			Lsn:      789,
			Location: common.FileLocation{PageID: 101112, SlotNum: 13141},
		},
		common.RecordID{
			PageID:  161718,
			FileID:  192021,
			SlotNum: 2224,
		},
		[]byte("before value"),
		[]byte("after value"),
	)

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	var recovered UpdateLogRecord
	if err := recovered.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}

	// Compare all fields
	if original.lsn != recovered.lsn {
		t.Errorf("lsn mismatch: got %v, want %v", recovered.lsn, original.lsn)
	}

	if original.txnID != recovered.txnID {
		t.Errorf(
			"TransactionID mismatch: got %v, want %v",
			recovered.txnID,
			original.txnID,
		)
	}

	if original.parentLogLocation != recovered.parentLogLocation {
		t.Errorf(
			"prevLog mismatch: got %v, want %v",
			recovered.parentLogLocation,
			original.parentLogLocation,
		)
	}

	if original.modifiedRecordID != recovered.modifiedRecordID {
		t.Errorf(
			"pageInfo mismatch: got %v, want %v",
			recovered.modifiedRecordID,
			original.modifiedRecordID,
		)
	}

	if !bytes.Equal(original.beforeValue, recovered.beforeValue) {
		t.Errorf(
			"beforeValue mismatch: got %v, want %v",
			recovered.beforeValue,
			original.beforeValue,
		)
	}

	if !bytes.Equal(original.afterValue, recovered.afterValue) {
		t.Errorf(
			"afterValue mismatch: got %v, want %v",
			recovered.afterValue,
			original.afterValue,
		)
	}
}

func TestInsertLogRecord_MarshalUnmarshal(t *testing.T) {
	original := NewInsertLogRecord(
		123,
		common.TxnID(456),
		common.LogRecordLocInfo{
			Lsn:      789,
			Location: common.FileLocation{PageID: 101112, SlotNum: 13115},
		},
		common.RecordID{
			PageID:  161718,
			FileID:  192021,
			SlotNum: 2224,
		},
		[]byte("test value"),
	)

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	var recovered InsertLogRecord
	if err := recovered.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}

	// Compare all fields
	if original.lsn != recovered.lsn {
		t.Errorf("lsn mismatch: got %v, want %v", recovered.lsn, original.lsn)
	}

	if original.txnID != recovered.txnID {
		t.Errorf(
			"TransactionID mismatch: got %v, want %v",
			recovered.txnID,
			original.txnID,
		)
	}

	if original.parentLogLocation != recovered.parentLogLocation {
		t.Errorf(
			"prevLog mismatch: got %v, want %v",
			recovered.parentLogLocation,
			original.parentLogLocation,
		)
	}

	if original.modifiedRecordID != recovered.modifiedRecordID {
		t.Errorf(
			"pageInfo mismatch: got %v, want %v",
			recovered.modifiedRecordID,
			original.modifiedRecordID,
		)
	}

	if !bytes.Equal(original.value, recovered.value) {
		t.Errorf(
			"value mismatch: got %v, want %v",
			recovered.value,
			original.value,
		)
	}
}

func TestCommitLogRecord_MarshalUnmarshal(t *testing.T) {
	original := NewCommitLogRecord(
		123,
		common.TxnID(456),
		common.LogRecordLocInfo{
			Lsn:      789,
			Location: common.FileLocation{PageID: 101112, SlotNum: 13115},
		},
	)

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	var recovered CommitLogRecord
	if err := recovered.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}

	// Compare all fields
	if original.lsn != recovered.lsn {
		t.Errorf("lsn mismatch: got %v, want %v", recovered.lsn, original.lsn)
	}

	if original.txnID != recovered.txnID {
		t.Errorf(
			"TransactionID mismatch: got %v, want %v",
			recovered.txnID,
			original.txnID,
		)
	}

	if original.parentLogLocation != recovered.parentLogLocation {
		t.Errorf(
			"prevLog mismatch: got %v, want %v",
			recovered.parentLogLocation,
			original.parentLogLocation,
		)
	}
}

func TestAbortLogRecord_MarshalUnmarshal(t *testing.T) {
	original := NewAbortLogRecord(
		123,
		common.TxnID(456),
		common.LogRecordLocInfo{
			Lsn:      789,
			Location: common.FileLocation{PageID: 101112, SlotNum: 13145},
		},
	)

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	var recovered AbortLogRecord
	if err := recovered.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}

	// Compare all fields
	if original.lsn != recovered.lsn {
		t.Errorf("lsn mismatch: got %v, want %v", recovered.lsn, original.lsn)
	}

	if original.txnID != recovered.txnID {
		t.Errorf(
			"TransactionID mismatch: got %v, want %v",
			recovered.txnID,
			original.txnID,
		)
	}

	if original.parentLogLocation != recovered.parentLogLocation {
		t.Errorf(
			"prevLog mismatch: got %v, want %v",
			recovered.parentLogLocation,
			original.parentLogLocation,
		)
	}
}

func TestTxnEndLogRecord_MarshalUnmarshal(t *testing.T) {
	original := NewTxnEndLogRecord(
		123,
		common.TxnID(456),
		common.LogRecordLocInfo{
			Lsn:      789,
			Location: common.FileLocation{PageID: 101112, SlotNum: 13415},
		},
	)

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	var recovered TxnEndLogRecord
	if err := recovered.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}

	// Compare all fields
	if original.lsn != recovered.lsn {
		t.Errorf("lsn mismatch: got %v, want %v", recovered.lsn, original.lsn)
	}

	if original.txnID != recovered.txnID {
		t.Errorf(
			"TransactionID mismatch: got %v, want %v",
			recovered.txnID,
			original.txnID,
		)
	}

	if original.parentLogLocation != recovered.parentLogLocation {
		t.Errorf(
			"prevLog mismatch: got %v, want %v",
			recovered.parentLogLocation,
			original.parentLogLocation,
		)
	}
}

func TestCompensationLogRecord_MarshalUnmarshal(t *testing.T) {
	original := NewCompensationLogRecord(
		123,
		common.TxnID(456),
		common.LogRecordLocInfo{
			Lsn:      789,
			Location: common.FileLocation{PageID: 101112, SlotNum: 13145},
		},
		common.RecordID{
			PageID:  161718,
			FileID:  192021,
			SlotNum: 22224,
		},
		CLRtypeUpdate,
		252627,
		[]byte("before value"),
		[]byte("after value123"),
	)

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	var recovered CompensationLogRecord
	if err := recovered.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}

	assert.Equal(t, original, recovered)
}

func TestInvalidTypeTag(t *testing.T) {
	// Create a valid record first
	original := NewBeginLogRecord(123, common.TxnID(456))

	data, err := original.MarshalBinary()
	assert.NoError(t, err)

	// Corrupt the type tag
	data[0] = 0xFF

	var recovered BeginLogRecord
	err = recovered.UnmarshalBinary(data)
	assert.Error(t, err, "expected error for invalid type tag")
}

func TestEmptyData(t *testing.T) {
	var recovered BeginLogRecord
	err := recovered.UnmarshalBinary([]byte{})
	assert.Error(t, err, "Expected error for empty data")
}

func TestPartialData(t *testing.T) {
	// Create a valid record first
	original := NewBeginLogRecord(123, common.TxnID(456))

	data, err := original.MarshalBinary()
	assert.NoError(t, err)

	// Truncate the data
	data = data[:len(data)-2]

	var recovered BeginLogRecord
	err = recovered.UnmarshalBinary(data)
	assert.Error(t, err, "Expected error for truncated data")
}

func TestCheckpointBegin_MarshalUnmarshal(t *testing.T) {
	original := NewCheckpointBegin(123456789)

	data, err := original.MarshalBinary()
	assert.NoError(t, err)

	var recovered CheckpointBeginLogRecord
	err = recovered.UnmarshalBinary(data)
	assert.NoError(t, err)

	assert.Equal(t, original, recovered)
}

func TestCheckpointBegin_EmptyData(t *testing.T) {
	var recovered CheckpointBeginLogRecord
	err := recovered.UnmarshalBinary([]byte{})
	assert.Error(t, err, "Expected error for empty data")
}

func TestCheckpointBegin_InvalidTypeTag(t *testing.T) {
	// Create a valid record first
	original := NewCheckpointBegin(123456789)

	data, err := original.MarshalBinary()
	assert.NoError(t, err)

	// Corrupt the type tag
	data[0] = 0xFF

	var recovered CheckpointBeginLogRecord
	err = recovered.UnmarshalBinary(data)
	assert.Error(t, err, "Expected error for invalid type tag")
}

func TestCheckpointBegin_TruncatedData(t *testing.T) {
	// Create a valid record first
	original := NewCheckpointBegin(123456789)

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	// Truncate the data (remove last byte)
	data = data[:len(data)-1]

	var recovered CheckpointBeginLogRecord
	if err := recovered.UnmarshalBinary(data); err == nil {
		t.Fatal("Expected error for truncated data, got nil")
	}
}

func TestCheckpointEnd_MarshalUnmarshal(t *testing.T) {
	activeTxns := map[common.TxnID]common.LogRecordLocInfo{
		123: {Lsn: 100, Location: common.FileLocation{PageID: 1, SlotNum: 1}},
		456: {Lsn: 200, Location: common.FileLocation{PageID: 2, SlotNum: 2}},
		789: {Lsn: 300, Location: common.FileLocation{PageID: 3, SlotNum: 3}},
	}
	dirtyPages := map[common.PageIdentity]common.LogRecordLocInfo{
		{PageID: 1, FileID: 1}: {
			Lsn:      100,
			Location: common.FileLocation{},
		},
		{PageID: 2, FileID: 1}: {
			Lsn:      200,
			Location: common.FileLocation{},
		},
	}

	original := NewCheckpointEnd(999, activeTxns, dirtyPages)

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	var recovered CheckpointEndLogRecord
	if err := recovered.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}

	// Compare all fields
	if original.lsn != recovered.lsn {
		t.Errorf("lsn mismatch: got %v, want %v", recovered.lsn, original.lsn)
	}

	if len(original.activeTransactions) != len(recovered.activeTransactions) {
		t.Fatalf("activeTransactions length mismatch: got %d, want %d",
			len(recovered.activeTransactions), len(original.activeTransactions))
	}

	for txnID, expectedLoc := range original.activeTransactions {
		recoveredLoc, exists := recovered.activeTransactions[txnID]
		if !exists {
			t.Errorf("missing txnID in recovered activeTransactions: %v", txnID)
			continue
		}
		if expectedLoc != recoveredLoc {
			t.Errorf(
				"activeTransactions[%v] mismatch: got %v, want %v",
				txnID,
				recoveredLoc,
				expectedLoc,
			)
		}
	}

	if len(original.dirtyPageTable) != len(recovered.dirtyPageTable) {
		t.Fatalf("dirtyPageTable length mismatch: got %d, want %d",
			len(recovered.dirtyPageTable), len(original.dirtyPageTable))
	}

	for pageID, lsn := range original.dirtyPageTable {
		recoveredLSN, exists := recovered.dirtyPageTable[pageID]
		if !exists {
			t.Errorf("missing pageID in recovered dirtyPageTable: %v", pageID)
			continue
		}

		if lsn != recoveredLSN {
			t.Errorf("LSN mismatch for pageID %v: got %v, want %v",
				pageID, recoveredLSN, lsn)
		}
	}
}

func TestCheckpointEnd_EmptyData(t *testing.T) {
	var recovered CheckpointEndLogRecord
	if err := recovered.UnmarshalBinary([]byte{}); err == nil {
		t.Fatal("Expected error for empty data, got nil")
	}
}

func TestCheckpointEnd_InvalidTypeTag(t *testing.T) {
	// Create a valid record first
	original := NewCheckpointEnd(
		999,
		map[common.TxnID]common.LogRecordLocInfo{123: {Lsn: 100, Location: common.FileLocation{}}},
		make(map[common.PageIdentity]common.LogRecordLocInfo),
	)

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	// Corrupt the type tag
	data[0] = 0xFF

	var recovered CheckpointEndLogRecord
	if err := recovered.UnmarshalBinary(data); err == nil {
		t.Fatal("Expected error for invalid type tag, got nil")
	}
}

func TestCheckpointEnd_EmptyMaps(t *testing.T) {
	// Test with empty maps
	original := NewCheckpointEnd(
		999,
		make(map[common.TxnID]common.LogRecordLocInfo),
		make(map[common.PageIdentity]common.LogRecordLocInfo),
	)

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	var recovered CheckpointEndLogRecord
	if err := recovered.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}

	// Verify fields
	if original.lsn != recovered.lsn {
		t.Errorf("lsn mismatch: got %v, want %v", recovered.lsn, original.lsn)
	}

	if len(recovered.activeTransactions) != 0 {
		t.Errorf("expected empty activeTransactions, got %d", len(recovered.activeTransactions))
	}

	if len(recovered.dirtyPageTable) != 0 {
		t.Errorf("expected empty dirtyPageTable, got %d", len(recovered.dirtyPageTable))
	}
}

func TestCheckpointEnd_TruncatedData(t *testing.T) {
	// Create a valid record first
	original := NewCheckpointEnd(
		999,
		map[common.TxnID]common.LogRecordLocInfo{123: {Lsn: 100, Location: common.FileLocation{}}},
		map[common.PageIdentity]common.LogRecordLocInfo{
			{PageID: 1, FileID: 1}: {Lsn: 200, Location: common.FileLocation{}},
		},
	)

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	// Truncate the data
	data = data[:len(data)-2]

	var recovered CheckpointEndLogRecord
	if err := recovered.UnmarshalBinary(data); err == nil {
		t.Fatal("Expected error for truncated data, got nil")
	}
}

func TestDeleteLogRecord_MarshalUnmarshal(t *testing.T) {
	original := NewDeleteLogRecord(
		123,
		common.TxnID(456),
		common.LogRecordLocInfo{
			Lsn:      789,
			Location: common.FileLocation{PageID: 101112, SlotNum: 13141},
		},
		common.RecordID{
			PageID:  161718,
			FileID:  192021,
			SlotNum: 2224,
		},
	)

	data, err := original.MarshalBinary()
	assert.NoError(t, err)

	var recovered DeleteLogRecord
	err = recovered.UnmarshalBinary(data)
	assert.NoError(t, err)

	assert.Equal(t, original.lsn, recovered.lsn)
	assert.Equal(t, original.txnID, recovered.txnID)
	assert.Equal(t, original.parentLogLocation, recovered.parentLogLocation)
	assert.Equal(t, original.modifiedRecordID, recovered.modifiedRecordID)
}

func TestDeleteLogRecord_MarshalBinary_EmptyBeforeValue(t *testing.T) {
	original := NewDeleteLogRecord(
		1,
		common.TxnID(2),
		common.LogRecordLocInfo{
			Lsn:      3,
			Location: common.FileLocation{PageID: 4, SlotNum: 5},
		},
		common.RecordID{
			PageID:  6,
			FileID:  7,
			SlotNum: 8,
		},
	)

	data, err := original.MarshalBinary()
	assert.NoError(t, err)

	var recovered DeleteLogRecord
	err = recovered.UnmarshalBinary(data)
	assert.NoError(t, err)
}

func TestDeleteLogRecord_UnmarshalBinary_InvalidTypeTag(t *testing.T) {
	original := NewDeleteLogRecord(
		1,
		common.TxnID(2),
		common.LogRecordLocInfo{
			Lsn:      3,
			Location: common.FileLocation{PageID: 4, SlotNum: 5},
		},
		common.RecordID{
			PageID:  6,
			FileID:  7,
			SlotNum: 8,
		},
	)

	data, err := original.MarshalBinary()
	assert.NoError(t, err)

	data[0] = 0xFF // Corrupt the type tag

	var recovered DeleteLogRecord
	err = recovered.UnmarshalBinary(data)
	assert.Error(t, err)
}

func TestDeleteLogRecord_UnmarshalBinary_EmptyData(t *testing.T) {
	var recovered DeleteLogRecord
	err := recovered.UnmarshalBinary([]byte{})
	assert.Error(t, err)
}

func TestDeleteLogRecord_UnmarshalBinary_TruncatedData(t *testing.T) {
	original := NewDeleteLogRecord(
		1,
		common.TxnID(2),
		common.LogRecordLocInfo{
			Lsn:      3,
			Location: common.FileLocation{PageID: 4, SlotNum: 5},
		},
		common.RecordID{
			PageID:  6,
			FileID:  7,
			SlotNum: 8,
		},
	)

	data, err := original.MarshalBinary()
	assert.NoError(t, err)

	// Truncate the data
	data = data[:len(data)-2]

	var recovered DeleteLogRecord
	err = recovered.UnmarshalBinary(data)
	assert.Error(t, err)
}
