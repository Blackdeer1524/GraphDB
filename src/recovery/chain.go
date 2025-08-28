package recovery

import (
	"fmt"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
)

type TxnLogChain struct {
	logger *txnLogger
	txnID  common.TxnID

	lastLocations map[common.TxnID]common.LogRecordLocInfo
	err           error
}

func NewTxnLogChain(
	logger *txnLogger,
	TransactionID common.TxnID,
) *TxnLogChain {
	return &TxnLogChain{
		logger: logger,
		txnID:  TransactionID,

		lastLocations: map[common.TxnID]common.LogRecordLocInfo{},
	}
}

func (c *TxnLogChain) SwitchTransactionID(
	TransactionID common.TxnID,
) *TxnLogChain {
	if c.err != nil {
		return c
	}

	c.txnID = TransactionID
	return c
}

func (c *TxnLogChain) Begin() *TxnLogChain {
	if c.err != nil {
		return c
	}

	c.lastLocations[c.txnID], c.err = c.logger.AppendBegin(c.txnID)

	return c
}

func (c *TxnLogChain) Insert(
	recordID common.RecordID,
	value []byte,
) *TxnLogChain {
	if c.err != nil {
		return c
	}

	if _, ok := c.lastLocations[c.txnID]; !ok {
		c.err = fmt.Errorf("no last location found for %d", c.txnID)
		return c
	}

	c.lastLocations[c.txnID], c.err = c.logger.pool.WithMarkDirtyLogPage(
		func() (common.LogRecordLocInfo, error) {
			return c.logger.AppendInsert(
				c.txnID,
				c.lastLocations[c.txnID],
				recordID,
				value,
			)
		},
	)

	return c
}

func (c *TxnLogChain) Update(
	recordID common.RecordID,
	beforeValue, afterValue []byte,
) *TxnLogChain {
	if c.err != nil {
		return c
	}

	if _, ok := c.lastLocations[c.txnID]; !ok {
		c.err = fmt.Errorf("no last location found for %d", c.txnID)
		return c
	}

	c.lastLocations[c.txnID], c.err = c.logger.pool.WithMarkDirtyLogPage(
		func() (common.LogRecordLocInfo, error) {
			return c.logger.AppendUpdate(
				c.txnID,
				c.lastLocations[c.txnID],
				recordID,
				beforeValue,
				afterValue,
			)
		},
	)

	return c
}

func (c *TxnLogChain) Delete(
	recordID common.RecordID,
) *TxnLogChain {
	if c.err != nil {
		return c
	}

	if _, ok := c.lastLocations[c.txnID]; !ok {
		c.err = fmt.Errorf("no last location found for %d", c.txnID)
		return c
	}

	c.lastLocations[c.txnID], c.err = c.logger.pool.WithMarkDirtyLogPage(
		func() (common.LogRecordLocInfo, error) {
			return c.logger.AppendDelete(
				c.txnID,
				c.lastLocations[c.txnID],
				recordID,
			)
		},
	)

	return c
}

func (c *TxnLogChain) Commit() *TxnLogChain {
	if c.err != nil {
		return c
	}

	if _, ok := c.lastLocations[c.txnID]; !ok {
		c.err = fmt.Errorf("no last location found for %d", c.txnID)
		return c
	}

	c.lastLocations[c.txnID], c.err = c.logger.AppendCommit(c.txnID, c.lastLocations[c.txnID])
	return c
}

func (c *TxnLogChain) Abort() *TxnLogChain {
	if c.err != nil {
		return c
	}

	if _, ok := c.lastLocations[c.txnID]; !ok {
		c.err = fmt.Errorf("no last location found for %d", c.txnID)
		return c
	}

	c.lastLocations[c.txnID], c.err = c.logger.AppendAbort(c.txnID, c.lastLocations[c.txnID])

	return c
}

func (c *TxnLogChain) TxnEnd() *TxnLogChain {
	if c.err != nil {
		return c
	}

	if _, ok := c.lastLocations[c.txnID]; !ok {
		c.err = fmt.Errorf("no last location found for %d", c.txnID)
		return c
	}

	c.lastLocations[c.txnID], c.err = c.logger.AppendTxnEnd(c.txnID, c.lastLocations[c.txnID])

	return c
}

func (c *TxnLogChain) CheckpointBegin() *TxnLogChain {
	if c.err != nil {
		return c
	}

	c.err = c.logger.AppendCheckpointBegin()

	return c
}

func (c *TxnLogChain) CheckpointEnd(
	ATT map[common.TxnID]common.LogRecordLocInfo,
	DPT map[common.PageIdentity]common.LogRecordLocInfo,
) *TxnLogChain {
	if c.err != nil {
		return c
	}

	c.err = c.logger.AppendCheckpointEnd(ATT, DPT)
	return c
}

func (c *TxnLogChain) Loc() common.LogRecordLocInfo {
	return c.lastLocations[c.txnID]
}

func (c *TxnLogChain) Err() error {
	return c.err
}
