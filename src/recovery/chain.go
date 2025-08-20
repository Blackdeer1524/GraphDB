package recovery

import (
	"fmt"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
)

type TxnLogChain struct {
	logger        *txnLogger
	TransactionID common.TxnID

	lastLocations map[common.TxnID]common.LogRecordLocInfo
	err           error
}

func NewTxnLogChain(
	logger *txnLogger,
	TransactionID common.TxnID,
) *TxnLogChain {
	return &TxnLogChain{
		logger:        logger,
		TransactionID: TransactionID,

		lastLocations: map[common.TxnID]common.LogRecordLocInfo{},
	}
}

func (c *TxnLogChain) SwitchTransactionID(
	TransactionID common.TxnID,
) *TxnLogChain {
	if c.err != nil {
		return c
	}

	c.TransactionID = TransactionID
	return c
}

func (c *TxnLogChain) Begin() *TxnLogChain {
	if c.err != nil {
		return c
	}

	c.lastLocations[c.TransactionID], c.err = c.logger.AppendBegin(
		c.TransactionID,
	)

	return c
}

func (c *TxnLogChain) Insert(
	recordID common.RecordID,
	value []byte,
) *TxnLogChain {
	if c.err != nil {
		return c
	}

	if _, ok := c.lastLocations[c.TransactionID]; !ok {
		c.err = fmt.Errorf("no last location found for %d", c.TransactionID)
		return c
	}

	c.lastLocations[c.TransactionID], c.err = c.logger.AppendInsert(
		c.TransactionID,
		c.lastLocations[c.TransactionID],
		recordID,
		value,
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

	if _, ok := c.lastLocations[c.TransactionID]; !ok {
		c.err = fmt.Errorf("no last location found for %d", c.TransactionID)
		return c
	}

	c.lastLocations[c.TransactionID], c.err = c.logger.AppendUpdate(
		c.TransactionID,
		c.lastLocations[c.TransactionID],
		recordID,
		beforeValue,
		afterValue,
	)

	return c
}

func (c *TxnLogChain) Delete(
	recordID common.RecordID,
) *TxnLogChain {
	if c.err != nil {
		return c
	}

	if _, ok := c.lastLocations[c.TransactionID]; !ok {
		c.err = fmt.Errorf("no last location found for %d", c.TransactionID)
		return c
	}

	c.lastLocations[c.TransactionID], c.err = c.logger.AppendDelete(
		c.TransactionID,
		c.lastLocations[c.TransactionID],
		recordID,
	)

	return c
}

func (c *TxnLogChain) Commit() *TxnLogChain {
	if c.err != nil {
		return c
	}

	if _, ok := c.lastLocations[c.TransactionID]; !ok {
		c.err = fmt.Errorf("no last location found for %d", c.TransactionID)
		return c
	}

	c.lastLocations[c.TransactionID], c.err = c.logger.AppendCommit(
		c.TransactionID,
		c.lastLocations[c.TransactionID],
	)

	return c
}

func (c *TxnLogChain) Abort() *TxnLogChain {
	if c.err != nil {
		return c
	}

	if _, ok := c.lastLocations[c.TransactionID]; !ok {
		c.err = fmt.Errorf("no last location found for %d", c.TransactionID)
		return c
	}

	c.lastLocations[c.TransactionID], c.err = c.logger.AppendAbort(
		c.TransactionID,
		c.lastLocations[c.TransactionID],
	)

	return c
}

func (c *TxnLogChain) TxnEnd() *TxnLogChain {
	if c.err != nil {
		return c
	}

	if _, ok := c.lastLocations[c.TransactionID]; !ok {
		c.err = fmt.Errorf("no last location found for %d", c.TransactionID)
		return c
	}

	c.lastLocations[c.TransactionID], c.err = c.logger.AppendTxnEnd(
		c.TransactionID,
		c.lastLocations[c.TransactionID],
	)

	return c
}

func (c *TxnLogChain) CheckpointBegin() *TxnLogChain {
	if c.err != nil {
		return c
	}

	_, c.err = c.logger.AppendCheckpointBegin()

	return c
}

func (c *TxnLogChain) CheckpointEnd(
	ATT []common.TxnID,
	DPT map[common.PageIdentity]common.LogRecordLocInfo,
) *TxnLogChain {
	if c.err != nil {
		return c
	}

	_, c.err = c.logger.AppendCheckpointEnd(ATT, DPT)

	return c
}

func (c *TxnLogChain) Loc() common.LogRecordLocInfo {
	return c.lastLocations[c.TransactionID]
}

func (c *TxnLogChain) Err() error {
	return c.err
}
