package recovery

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
)

func TestChainSanity(t *testing.T) {
	pool := bufferpool.NewBufferPoolMock()
	defer func() { assert.NoError(t, pool.EnsureAllPagesUnpinned()) }()

	logPageId := common.PageIdentity{
		FileID: 42,
		PageID: 321,
	}

	logger := &TxnLogger{
		pool:            pool,
		mu:              sync.Mutex{},
		logRecordsCount: 0,
		logfileID:       logPageId.FileID,
		lastLogLocation: common.LogRecordLocInfo{
			Lsn: 123,
			Location: common.FileLocation{
				PageID:  logPageId.PageID,
				SlotNum: 0,
			},
		},
		getActiveTransactions: func() []common.TxnID {
			panic("TODO")
		},
	}

	txnID := common.TxnID(89)
	chain := NewTxnLogChain(logger, txnID)

	insertSlotNumber := uint16(6)
	insertPageID := common.PageIdentity{
		FileID: 1,
		PageID: 2,
	}
	insert := []byte("insert")

	updateSlotNumber := uint16(7)
	updatePageID := common.PageIdentity{
		FileID: 2,
		PageID: 1,
	}
	updateFrom := []byte("updateOld")
	updateTo := []byte("updateNew")

	deleteSlotNumber := uint16(8)
	deletePageID := common.PageIdentity{
		FileID: 3,
		PageID: 1,
	}

	checkpointATT := []common.TxnID{1, 2, 3}
	checkpointDPT := map[common.PageIdentity]common.LogRecordLocInfo{
		{
			FileID: 42,
			PageID: 123,
		}: {
			Lsn: 5,
			Location: common.FileLocation{
				PageID:  6,
				SlotNum: 7,
			},
		},
	}

	chain.Begin().
		Insert(common.RecordID{FileID: insertPageID.FileID, PageID: insertPageID.PageID, SlotNum: insertSlotNumber}, insert).
		Update(common.RecordID{FileID: updatePageID.FileID, PageID: updatePageID.PageID, SlotNum: updateSlotNumber}, updateFrom, updateTo).
		Delete(common.RecordID{FileID: deletePageID.FileID, PageID: deletePageID.PageID, SlotNum: deleteSlotNumber}).
		Abort().
		Commit().
		CheckpointBegin().
		CheckpointEnd(checkpointATT, checkpointDPT).
		TxnEnd()

	require.NoError(t, chain.Err())
	require.NoError(t, pool.EnsureAllPagesUnpinned())

	page, err := pool.GetPage(logPageId)
	require.NoError(t, err)

	defer func() { assert.NoError(t, pool.Unpin(logPageId)) }()

	page.RLock()
	defer page.RUnlock()

	// Begin
	{
		data := page.Read(0)

		tag, untypedRecord, err := readLogRecord(data)
		require.NoError(t, err)
		require.Equal(t, TypeBegin, tag)

		_, ok := untypedRecord.(BeginLogRecord)
		require.True(t, ok)
	}

	// Insert
	{
		data := page.Read(1)
		require.NoError(t, err)
		tag, untypedRecord, err := readLogRecord(data)

		require.NoError(t, err)
		require.Equal(t, TypeInsert, tag)

		r, ok := untypedRecord.(InsertLogRecord)
		require.True(t, ok)

		require.Equal(t, txnID, r.txnID)
		require.Equal(t, insertPageID, r.modifiedRecordID.PageIdentity())
		require.Equal(t, insert, r.value)
		require.Equal(t, insertSlotNumber, r.modifiedRecordID.SlotNum)
	}

	// Update
	{
		data := page.Read(2)
		require.NoError(t, err)
		tag, untypedRecord, err := readLogRecord(data)

		require.NoError(t, err)
		require.Equal(t, TypeUpdate, tag)

		r, ok := untypedRecord.(UpdateLogRecord)
		require.True(t, ok)

		require.Equal(t, txnID, r.txnID)
		require.Equal(t, updatePageID, r.modifiedRecordID.PageIdentity())
		require.Equal(t, updateSlotNumber, r.modifiedRecordID.SlotNum)
		require.Equal(t, updateFrom, r.beforeValue)
		require.Equal(t, updateTo, r.afterValue)
	}

	// Delete
	{
		data := page.Read(3)
		require.NoError(t, err)
		tag, untypedRecord, err := readLogRecord(data)

		require.NoError(t, err)
		require.Equal(t, TypeDelete, tag)

		r, ok := untypedRecord.(DeleteLogRecord)
		require.True(t, ok)

		require.Equal(t, txnID, r.txnID)
		require.Equal(t, deletePageID, r.modifiedRecordID.PageIdentity())
		require.Equal(t, deleteSlotNumber, r.modifiedRecordID.SlotNum)
	}

	// Abort
	{
		data := page.Read(4)
		require.NoError(t, err)
		tag, untypedRecord, err := readLogRecord(data)

		require.NoError(t, err)
		require.Equal(t, TypeAbort, tag)

		r, ok := untypedRecord.(AbortLogRecord)
		require.True(t, ok)

		require.Equal(t, txnID, r.txnID)
	}

	// Commit
	{
		data := page.Read(5)
		require.NoError(t, err)
		tag, untypedRecord, err := readLogRecord(data)

		require.NoError(t, err)
		require.Equal(t, TypeCommit, tag)

		r, ok := untypedRecord.(CommitLogRecord)
		require.True(t, ok)

		require.Equal(t, txnID, r.txnID)
	}

	// CheckpointBegin
	{
		data := page.Read(6)
		require.NoError(t, err)
		tag, untypedRecord, err := readLogRecord(data)

		require.NoError(t, err)
		require.Equal(t, TypeCheckpointBegin, tag)

		_, ok := untypedRecord.(CheckpointBeginLogRecord)
		require.True(t, ok)
	}

	// CheckpointEnd
	{
		data := page.Read(7)
		require.NoError(t, err)
		tag, untypedRecord, err := readLogRecord(data)

		require.NoError(t, err)
		require.Equal(t, TypeCheckpointEnd, tag)

		r, ok := untypedRecord.(CheckpointEndLogRecord)
		require.True(t, ok)

		require.Equal(t, checkpointATT, r.activeTransactions)
		require.Equal(t, checkpointDPT, r.dirtyPageTable)
	}

	// TxnEnd
	{
		data := page.Read(8)
		require.NoError(t, err)
		tag, untypedRecord, err := readLogRecord(data)

		require.NoError(t, err)
		require.Equal(t, TypeTxnEnd, tag)

		_, ok := untypedRecord.(TxnEndLogRecord)
		require.True(t, ok)
	}
}

func TestChain(t *testing.T) {
	pool := bufferpool.NewBufferPoolMock()

	logPageId := common.PageIdentity{
		FileID: 42,
		PageID: 23,
	}

	logger := &TxnLogger{
		pool:            pool,
		mu:              sync.Mutex{},
		logRecordsCount: 0,
		logfileID:       logPageId.FileID,
		lastLogLocation: common.LogRecordLocInfo{
			Lsn: 0,
			Location: common.FileLocation{
				PageID:  logPageId.PageID,
				SlotNum: 0,
			},
		},
		getActiveTransactions: func() []common.TxnID {
			panic("TODO")
		},
	}

	dataPageId := common.PageIdentity{
		FileID: 1,
		PageID: 0,
	}

	TransactionID_1 := common.TxnID(1)
	TransactionID_2 := common.TxnID(2)

	chain := NewTxnLogChain(logger, TransactionID_1)

	// interleaving
	chain.Begin().
		Insert(common.RecordID{FileID: dataPageId.FileID, PageID: dataPageId.PageID, SlotNum: 0}, []byte("first")).
		SwitchTransactionID(TransactionID_2).
		Begin().
		Insert(common.RecordID{FileID: dataPageId.FileID, PageID: dataPageId.PageID, SlotNum: 1}, []byte("second")).
		Update(common.RecordID{FileID: dataPageId.FileID, PageID: dataPageId.PageID, SlotNum: 1}, []byte("second"), []byte("sec0nd")).
		SwitchTransactionID(TransactionID_1).
		Update(common.RecordID{FileID: dataPageId.FileID, PageID: dataPageId.PageID, SlotNum: 0}, []byte("first"), []byte("updat"))

	require.NoError(t, chain.Err())
	require.NoError(t, pool.EnsureAllPagesUnpinned())

	page, err := pool.GetPage(logPageId)
	require.NoError(t, err)

	defer func() { assert.NoError(t, pool.Unpin(logPageId)) }()

	page.RLock()
	defer page.RUnlock()

	{
		data := page.Read(0)
		require.NoError(t, err)
		tag, untypedRecord, err := readLogRecord(data)

		require.NoError(t, err)
		require.Equal(t, TypeBegin, tag)

		r, ok := untypedRecord.(BeginLogRecord)
		require.True(t, ok)
		require.Equal(t, TransactionID_1, r.txnID)
	}
	{
		data := page.Read(1)
		require.NoError(t, err)
		tag, untypedRecord, err := readLogRecord(data)

		require.NoError(t, err)
		require.Equal(t, TypeInsert, tag)

		r, ok := untypedRecord.(InsertLogRecord)
		require.True(t, ok)
		require.Equal(t, TransactionID_1, r.txnID)
	}

	{
		data := page.Read(2)
		require.NoError(t, err)
		tag, untypedRecord, err := readLogRecord(data)

		require.NoError(t, err)
		require.Equal(t, TypeBegin, tag)

		r, ok := untypedRecord.(BeginLogRecord)
		require.True(t, ok)
		require.Equal(t, TransactionID_2, r.txnID)
	}
	{
		data := page.Read(3)
		require.NoError(t, err)
		tag, untypedRecord, err := readLogRecord(data)

		require.NoError(t, err)
		require.Equal(t, TypeInsert, tag)

		r, ok := untypedRecord.(InsertLogRecord)
		require.True(t, ok)
		require.Equal(t, TransactionID_2, r.txnID)
	}
	{
		data := page.Read(4)
		require.NoError(t, err)
		tag, untypedRecord, err := readLogRecord(data)

		require.NoError(t, err)
		require.Equal(t, TypeUpdate, tag)

		r, ok := untypedRecord.(UpdateLogRecord)
		require.True(t, ok)
		require.Equal(t, TransactionID_2, r.txnID)
	}

	{
		data := page.Read(5)
		require.NoError(t, err)
		tag, untypedRecord, err := readLogRecord(data)

		require.NoError(t, err)
		require.Equal(t, TypeUpdate, tag)

		r, ok := untypedRecord.(UpdateLogRecord)
		require.True(t, ok)
		require.Equal(t, TransactionID_1, r.txnID)
	}
}
