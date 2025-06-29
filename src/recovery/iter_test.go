package recovery

import (
	"math/rand"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

func generateSequence(
	chain *TxnLogChain,
	dataPageId bufferpool.PageIdentity,
	length int,
) []LogRecordTypeTag {
	chain.Begin()

	res := make([]LogRecordTypeTag, length+2)
	res[0] = TypeBegin

	for i := 1; i <= length; i++ {
		switch rand.Int() % 2 {
		case 0:
			res[i] = TypeInsert

			//nolint:gosec
			chain.Insert(dataPageId, uint16(i), []byte(strconv.Itoa(i))).Loc()
		case 1:
			res[i] = TypeUpdate

			//nolint:gosec
			chain.Update(dataPageId, uint16(i), []byte(strconv.Itoa(i)), []byte(strconv.Itoa(i))).
				Loc()
		}
	}

	if rand.Int()%2 == 0 {
		chain.Abort()

		res[len(res)-1] = TypeAbort
	} else {
		chain.Commit()

		res[len(res)-1] = TypeCommit
	}

	return res
}

func TestIterSanity(t *testing.T) {
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
		FileID: 123,
		PageID: 23,
	}

	TransactionID := txns.TxnID(1)
	chain := NewTxnLogChain(logger, TransactionID)

	types := generateSequence(chain, dataPageId, 100)
	iter, err := logger.Iter(FileLocation{
		PageID:  logPageId.PageID,
		SlotNum: 0,
	})
	require.NoError(t, err)

	for i := range len(types) {
		expectedType := types[i]
		tag, untypedRecord, err := iter.ReadRecord()
		assertLogRecord(t, tag, untypedRecord, expectedType, TransactionID)
		require.NoError(t, err)

		ok, err := iter.MoveForward()
		require.NoError(t, err)

		if i != len(types)-1 {
			require.True(t, ok)
		} else {
			require.False(t, ok)
		}
	}
}
