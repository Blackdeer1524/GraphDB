package recovery

import (
	"bytes"
	"testing"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

func TestBeginLogRecord_MarshalUnmarshal(t *testing.T) {
	original := NewBeginLogRecord(123, txns.TxnID(456))

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

	if original.TransactionID != recovered.TransactionID {
		t.Errorf("TransactionID mismatch: got %v, want %v", recovered.TransactionID, original.TransactionID)
	}
}

func TestUpdateLogRecord_MarshalUnmarshal(t *testing.T) {
	original := NewUpdateLogRecord(
		123,
		txns.TxnID(456),
		LogRecordLocationInfo{789, FileLocation{101112, 13141}},
		bufferpool.PageIdentity{PageID: 161718, FileID: 192021},
		2224,
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

	if original.TransactionID != recovered.TransactionID {
		t.Errorf("TransactionID mismatch: got %v, want %v", recovered.TransactionID, original.TransactionID)
	}

	if original.parentLogLocation != recovered.parentLogLocation {
		t.Errorf("prevLog mismatch: got %v, want %v", recovered.parentLogLocation, original.parentLogLocation)
	}

	if original.modifiedPageIdentity != recovered.modifiedPageIdentity {
		t.Errorf("pageInfo mismatch: got %v, want %v", recovered.modifiedPageIdentity, original.modifiedPageIdentity)
	}

	if original.modifiedSlotNumber != recovered.modifiedSlotNumber {
		t.Errorf("slotNumber mismatch: got %v, want %v", recovered.modifiedSlotNumber, original.modifiedSlotNumber)
	}

	if !bytes.Equal(original.beforeValue, recovered.beforeValue) {
		t.Errorf("beforeValue mismatch: got %v, want %v", recovered.beforeValue, original.beforeValue)
	}

	if !bytes.Equal(original.afterValue, recovered.afterValue) {
		t.Errorf("afterValue mismatch: got %v, want %v", recovered.afterValue, original.afterValue)
	}
}

func TestInsertLogRecord_MarshalUnmarshal(t *testing.T) {
	original := NewInsertLogRecord(
		123,
		txns.TxnID(456),
		LogRecordLocationInfo{789, FileLocation{101112, 13115}},
		bufferpool.PageIdentity{PageID: 161718, FileID: 192021},
		2224,
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

	if original.TransactionID != recovered.TransactionID {
		t.Errorf("TransactionID mismatch: got %v, want %v", recovered.TransactionID, original.TransactionID)
	}

	if original.parentLogLocation != recovered.parentLogLocation {
		t.Errorf("prevLog mismatch: got %v, want %v", recovered.parentLogLocation, original.parentLogLocation)
	}

	if original.modifiedPageIdentity != recovered.modifiedPageIdentity {
		t.Errorf("pageInfo mismatch: got %v, want %v", recovered.modifiedPageIdentity, original.modifiedPageIdentity)
	}

	if original.modifiedSlotNumber != recovered.modifiedSlotNumber {
		t.Errorf("slotNumber mismatch: got %v, want %v", recovered.modifiedSlotNumber, original.modifiedSlotNumber)
	}

	if !bytes.Equal(original.value, recovered.value) {
		t.Errorf("value mismatch: got %v, want %v", recovered.value, original.value)
	}
}

func TestCommitLogRecord_MarshalUnmarshal(t *testing.T) {
	original := NewCommitLogRecord(
		123,
		txns.TxnID(456),
		LogRecordLocationInfo{789, FileLocation{101112, 13115}},
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

	if original.TransactionID != recovered.TransactionID {
		t.Errorf("TransactionID mismatch: got %v, want %v", recovered.TransactionID, original.TransactionID)
	}

	if original.parentLogLocation != recovered.parentLogLocation {
		t.Errorf("prevLog mismatch: got %v, want %v", recovered.parentLogLocation, original.parentLogLocation)
	}
}

func TestAbortLogRecord_MarshalUnmarshal(t *testing.T) {
	original := NewAbortLogRecord(
		123,
		txns.TxnID(456),
		LogRecordLocationInfo{789, FileLocation{101112, 13145}},
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

	if original.TransactionID != recovered.TransactionID {
		t.Errorf("TransactionID mismatch: got %v, want %v", recovered.TransactionID, original.TransactionID)
	}

	if original.parentLogLocation != recovered.parentLogLocation {
		t.Errorf("prevLog mismatch: got %v, want %v", recovered.parentLogLocation, original.parentLogLocation)
	}
}

func TestTxnEndLogRecord_MarshalUnmarshal(t *testing.T) {
	original := NewTxnEndLogRecord(
		123,
		txns.TxnID(456),
		LogRecordLocationInfo{789, FileLocation{101112, 13415}},
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

	if original.TransactionID != recovered.TransactionID {
		t.Errorf("TransactionID mismatch: got %v, want %v", recovered.TransactionID, original.TransactionID)
	}

	if original.parentLogLocation != recovered.parentLogLocation {
		t.Errorf("prevLog mismatch: got %v, want %v", recovered.parentLogLocation, original.parentLogLocation)
	}
}

func TestCompensationLogRecord_MarshalUnmarshal(t *testing.T) {
	original := NewCompensationLogRecord(
		123,
		txns.TxnID(456),
		LogRecordLocationInfo{789, FileLocation{101112, 13145}},
		bufferpool.PageIdentity{PageID: 161718, FileID: 192021},
		22224,
		true,
		252627,
		[]byte("before value"),
		[]byte("after value"),
	)

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	var recovered CompensationLogRecord
	if err := recovered.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}

	// Compare all fields
	if original.lsn != recovered.lsn {
		t.Errorf("lsn mismatch: got %v, want %v", recovered.lsn, original.lsn)
	}

	if original.TransactionID != recovered.TransactionID {
		t.Errorf("TransactionID mismatch: got %v, want %v", recovered.TransactionID, original.TransactionID)
	}

	if original.parentLogLocation != recovered.parentLogLocation {
		t.Errorf("prevLog mismatch: got %v, want %v", recovered.parentLogLocation, original.parentLogLocation)
	}

	if original.modifiedPageIdentity != recovered.modifiedPageIdentity {
		t.Errorf("pageInfo mismatch: got %v, want %v", recovered.modifiedPageIdentity, original.modifiedPageIdentity)
	}

	if original.modifiedSlotNumber != recovered.modifiedSlotNumber {
		t.Errorf("slotNumber mismatch: got %v, want %v", recovered.modifiedSlotNumber, original.modifiedSlotNumber)
	}

	if original.nextUndoLSN != recovered.nextUndoLSN {
		t.Errorf("nextUndoLSN mismatch: got %v, want %v", recovered.nextUndoLSN, original.nextUndoLSN)
	}

	if !bytes.Equal(original.beforeValue, recovered.beforeValue) {
		t.Errorf("beforeValue mismatch: got %v, want %v", recovered.beforeValue, original.beforeValue)
	}

	if !bytes.Equal(original.afterValue, recovered.afterValue) {
		t.Errorf("afterValue mismatch: got %v, want %v", recovered.afterValue, original.afterValue)
	}
}

func TestInvalidTypeTag(t *testing.T) {
	// Create a valid record first
	original := NewBeginLogRecord(123, txns.TxnID(456))

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	// Corrupt the type tag
	data[0] = 0xFF

	var recovered BeginLogRecord
	if err := recovered.UnmarshalBinary(data); err == nil {
		t.Fatal("Expected error for invalid type tag, got nil")
	}
}

func TestEmptyData(t *testing.T) {
	var recovered BeginLogRecord
	if err := recovered.UnmarshalBinary([]byte{}); err == nil {
		t.Fatal("Expected error for empty data, got nil")
	}
}

func TestPartialData(t *testing.T) {
	// Create a valid record first
	original := NewBeginLogRecord(123, txns.TxnID(456))

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	// Truncate the data
	data = data[:len(data)-2]

	var recovered BeginLogRecord
	if err := recovered.UnmarshalBinary(data); err == nil {
		t.Fatal("Expected error for truncated data, got nil")
	}
}

func TestCheckpointBegin_MarshalUnmarshal(t *testing.T) {
	original := NewCheckpointBegin(123456789)

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	var recovered CheckpointBeginLogRecord
	if err := recovered.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}

	if original.lsn != recovered.lsn {
		t.Errorf("lsn mismatch: got %v, want %v", recovered.lsn, original.lsn)
	}
}

func TestCheckpointBegin_EmptyData(t *testing.T) {
	var recovered CheckpointBeginLogRecord
	if err := recovered.UnmarshalBinary([]byte{}); err == nil {
		t.Fatal("Expected error for empty data, got nil")
	}
}

func TestCheckpointBegin_InvalidTypeTag(t *testing.T) {
	// Create a valid record first
	original := NewCheckpointBegin(123456789)

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	// Corrupt the type tag
	data[0] = 0xFF

	var recovered CheckpointBeginLogRecord
	if err := recovered.UnmarshalBinary(data); err == nil {
		t.Fatal("Expected error for invalid type tag, got nil")
	}
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
	activeTxns := []txns.TxnID{123, 456, 789}
	dirtyPages := map[bufferpool.PageIdentity]LogRecordLocationInfo{
		{PageID: 1, FileID: 1}: {
			Lsn:      100,
			Location: FileLocation{},
		},
		{PageID: 2, FileID: 1}: {
			Lsn:      200,
			Location: FileLocation{},
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

	for i := range original.activeTransactions {
		if original.activeTransactions[i] != recovered.activeTransactions[i] {
			t.Errorf("activeTransactions[%d] mismatch: got %v, want %v",
				i, recovered.activeTransactions[i], original.activeTransactions[i])
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
	original := NewCheckpointEnd(999, []txns.TxnID{123}, make(map[bufferpool.PageIdentity]LogRecordLocationInfo))

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
