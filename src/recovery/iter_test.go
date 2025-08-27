package recovery

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
	"github.com/Blackdeer1524/GraphDB/src/storage/disk"
)

// generateSequence creates a random sequence of log record operations for
// testing purposes. It generates a series of Insert, Update and Delete
// operations followed by either Commit or Abort.
//
// Parameters:
//   - t: Testing context for assertions
//   - chain: Transaction log chain to record operations
//   - dataPageId: Page ID where operations will be performed
//
// - length: Number of operations to generate (excluding Begin and Commit/Abort)
//
// Returns:
// - []LogRecordTypeTag: Slice containing the sequence of operation types
// generated,
//
//	including Begin at start and Commit/Abort at end
//
// The function generates a random sequence by:
// 1. Starting with Begin operation
// 2. Randomly choosing between Insert/Update/Delete operations 'length' times
// 3. Ending with either Commit or Abort randomly
// Each operation uses the iteration number as both the slot ID and data value.
func generateSequence(
	t *testing.T,
	chain *TxnLogChain,
	dataPageId common.PageIdentity,
	length int,
) []LogRecordTypeTag {
	chain.Begin()

	res := make([]LogRecordTypeTag, length+2)
	res[0] = TypeBegin

	for i := 1; i <= length; i++ {
		switch rand.Int() % 3 {
		case 0:
			res[i] = TypeInsert

			//nolint:gosec
			chain.Insert(
				common.RecordID{
					FileID:  dataPageId.FileID,
					PageID:  dataPageId.PageID,
					SlotNum: uint16(i),
				},
				utils.ToBytes[uint32](uint32(i)),
			)
		case 1:
			res[i] = TypeUpdate

			//nolint:gosec
			chain.Update(
				common.RecordID{
					FileID:  dataPageId.FileID,
					PageID:  dataPageId.PageID,
					SlotNum: uint16(i),
				},
				utils.ToBytes[uint32](uint32(i)),
				utils.ToBytes[uint32](uint32(i+1)),
			)
		case 2:
			res[i] = TypeDelete

			//nolint:gosec
			chain.Delete(
				common.RecordID{
					FileID:  dataPageId.FileID,
					PageID:  dataPageId.PageID,
					SlotNum: uint16(i),
				},
			)
		}
	}

	if rand.Int()%2 == 0 {
		chain.Abort()
		res[len(res)-1] = TypeAbort
	} else {
		chain.Commit()
		res[len(res)-1] = TypeCommit
	}

	assert.NoError(t, chain.Err())
	return res
}

func TestIterSanity(t *testing.T) {
	logPageId := common.PageIdentity{
		FileID: 42,
		PageID: 321,
	}

	masterRecordPageIdent := common.PageIdentity{
		FileID: logPageId.FileID,
		PageID: common.CheckpointInfoPageID,
	}

	diskManager := disk.NewInMemoryManager()
	pool := bufferpool.NewDebugBufferPool(
		bufferpool.New(10, bufferpool.NewLRUReplacer(), diskManager),
		map[common.PageIdentity]struct{}{
			masterRecordPageIdent: {},
		},
	)
	defer func() { assert.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	setupLoggerMasterPage(
		t,
		pool,
		masterRecordPageIdent.FileID,
		common.LogRecordLocInfo{
			Lsn:      common.NilLSN,
			Location: common.FileLocation{PageID: logPageId.PageID, SlotNum: 0},
		},
	)
	logger := NewTxnLogger(pool, logPageId.FileID)

	dataPageId := common.PageIdentity{
		FileID: 123,
		PageID: 23,
	}

	txnID := common.TxnID(1)
	chain := NewTxnLogChain(logger, txnID)

	types := generateSequence(t, chain, dataPageId, 1)
	assert.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked())

	iter, err := logger.iter(common.FileLocation{
		PageID:  logPageId.PageID,
		SlotNum: 0,
	})
	require.NoError(t, err)

	for i := range len(types) {
		expectedType := types[i]
		tag, untypedRecord, err := iter.ReadRecord()
		assertLogRecord(t, tag, untypedRecord, expectedType, txnID)
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
