package recovery

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

func TestChainSanity(t *testing.T) {
	pool := bufferpool.NewBufferPoolMock()
	defer func() { assert.NoError(t, pool.EnsureAllPagesUnpinned()) }()

	logPageId := bufferpool.PageIdentity{
		FileID: 42,
		PageID: 321,
	}

	logger := &TxnLogger{
		pool:            pool,
		mu:              sync.Mutex{},
		logRecordsCount: 0,
		logfileID:       logPageId.FileID,
		lastLogLocation: LogRecordLocationInfo{
			Lsn: 123,
			Location: FileLocation{
				PageID:  logPageId.PageID,
				SlotNum: 0,
			},
		},
		getActiveTransactions: func() []txns.TxnID {
			panic("TODO")
		},
	}

	chain := NewTxnLogChain(logger, txns.TxnID(1))

	dataPageId := bufferpool.PageIdentity{
		FileID: 1,
		PageID: 0,
	}
	// Begin -> Insert -> Update -> Abort -> TxnEnd
	chain.Begin().
		Insert(dataPageId, 0, []byte("insert")).
		Update(dataPageId, 0, []byte("insert"), []byte("update")).
		Abort().
		TxnEnd()

	require.NoError(t, chain.Err())

	page, err := pool.GetPage(logPageId)
	require.NoError(t, err)

	defer func() { assert.NoError(t, pool.Unpin(logPageId)) }()

	page.RLock()
	defer page.RUnlock()

	// Begin
	{
		data, err := page.Get(0)
		require.NoError(t, err)
		tag, untypedRecord, err := readLogRecord(data)

		require.NoError(t, err)
		require.Equal(t, TypeBegin, tag)

		_, ok := untypedRecord.(BeginLogRecord)
		require.True(t, ok)
	}

	// Insert
	{
		data, err := page.Get(1)
		require.NoError(t, err)
		tag, untypedRecord, err := readLogRecord(data)

		require.NoError(t, err)
		require.Equal(t, TypeInsert, tag)

		_, ok := untypedRecord.(InsertLogRecord)
		require.True(t, ok)
	}

	// Update
	{
		data, err := page.Get(2)
		require.NoError(t, err)
		tag, untypedRecord, err := readLogRecord(data)

		require.NoError(t, err)
		require.Equal(t, TypeUpdate, tag)

		_, ok := untypedRecord.(UpdateLogRecord)
		require.True(t, ok)
	}

	// Abort
	{
		data, err := page.Get(3)
		require.NoError(t, err)
		tag, untypedRecord, err := readLogRecord(data)

		require.NoError(t, err)
		require.Equal(t, TypeAbort, tag)

		_, ok := untypedRecord.(AbortLogRecord)
		require.True(t, ok)
	}

	// TxnEnd
	{
		data, err := page.Get(4)
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

	logPageId := bufferpool.PageIdentity{
		FileID: 42,
		PageID: 23,
	}

	logger := &TxnLogger{
		pool:            pool,
		mu:              sync.Mutex{},
		logRecordsCount: 0,
		logfileID:       logPageId.FileID,
		lastLogLocation: LogRecordLocationInfo{
			Lsn: 0,
			Location: FileLocation{
				PageID:  logPageId.PageID,
				SlotNum: 0,
			},
		},
		getActiveTransactions: func() []txns.TxnID {
			panic("TODO")
		},
	}

	dataPageId := bufferpool.PageIdentity{
		FileID: 1,
		PageID: 0,
	}

	TransactionID_1 := txns.TxnID(1)
	TransactionID_2 := txns.TxnID(2)

	chain := NewTxnLogChain(logger, TransactionID_1)

	// interleaving
	chain.Begin().
		Insert(dataPageId, 0, []byte("first")).
		SwitchTransactionID(TransactionID_2).
		Begin().
		Insert(dataPageId, 1, []byte("second")).
		Update(dataPageId, 1, []byte("second"), []byte("sec0nd")).
		SwitchTransactionID(TransactionID_1).
		Update(dataPageId, 0, []byte("first"), []byte("update"))

	require.NoError(t, chain.Err())

	page, err := pool.GetPage(logPageId)
	require.NoError(t, err)

	defer func() { assert.NoError(t, pool.Unpin(logPageId)) }()

	page.RLock()
	defer page.RUnlock()

	{
		data, err := page.Get(0)
		require.NoError(t, err)
		tag, untypedRecord, err := readLogRecord(data)

		require.NoError(t, err)
		require.Equal(t, TypeBegin, tag)

		r, ok := untypedRecord.(BeginLogRecord)
		require.True(t, ok)
		require.Equal(t, TransactionID_1, r.TransactionID)
	}
	{
		data, err := page.Get(1)
		require.NoError(t, err)
		tag, untypedRecord, err := readLogRecord(data)

		require.NoError(t, err)
		require.Equal(t, TypeInsert, tag)

		r, ok := untypedRecord.(InsertLogRecord)
		require.True(t, ok)
		require.Equal(t, TransactionID_1, r.TransactionID)
	}

	{
		data, err := page.Get(2)
		require.NoError(t, err)
		tag, untypedRecord, err := readLogRecord(data)

		require.NoError(t, err)
		require.Equal(t, TypeBegin, tag)

		r, ok := untypedRecord.(BeginLogRecord)
		require.True(t, ok)
		require.Equal(t, TransactionID_2, r.TransactionID)
	}
	{
		data, err := page.Get(3)
		require.NoError(t, err)
		tag, untypedRecord, err := readLogRecord(data)

		require.NoError(t, err)
		require.Equal(t, TypeInsert, tag)

		r, ok := untypedRecord.(InsertLogRecord)
		require.True(t, ok)
		require.Equal(t, TransactionID_2, r.TransactionID)
	}
	{
		data, err := page.Get(4)
		require.NoError(t, err)
		tag, untypedRecord, err := readLogRecord(data)

		require.NoError(t, err)
		require.Equal(t, TypeUpdate, tag)

		r, ok := untypedRecord.(UpdateLogRecord)
		require.True(t, ok)
		require.Equal(t, TransactionID_2, r.TransactionID)
	}

	{
		data, err := page.Get(5)
		require.NoError(t, err)
		tag, untypedRecord, err := readLogRecord(data)

		require.NoError(t, err)
		require.Equal(t, TypeUpdate, tag)

		r, ok := untypedRecord.(UpdateLogRecord)
		require.True(t, ok)
		require.Equal(t, TransactionID_1, r.TransactionID)
	}
}
