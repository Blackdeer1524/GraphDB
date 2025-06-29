package recovery

import (
	"fmt"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

type TxnLogChain struct {
	logger        *TxnLogger
	TransactionID txns.TxnID

	lastLocations map[txns.TxnID]LogRecordLocationInfo
	err           error
}

func NewTxnLogChain(logger *TxnLogger, TransactionID txns.TxnID) *TxnLogChain {
	return &TxnLogChain{
		logger:        logger,
		TransactionID: TransactionID,

		lastLocations: map[txns.TxnID]LogRecordLocationInfo{},
	}
}

func (c *TxnLogChain) SwitchTransactionID(
	TransactionID txns.TxnID,
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
	pageInfo bufferpool.PageIdentity,
	slotNumber uint16,
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
		pageInfo,
		slotNumber,
		value,
	)

	return c
}

func (c *TxnLogChain) Update(
	pageInfo bufferpool.PageIdentity,
	slotNumber uint16,
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
		pageInfo,
		slotNumber,
		beforeValue,
		afterValue,
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
	ATT []txns.TxnID,
	DPT map[bufferpool.PageIdentity]LogRecordLocationInfo,
) *TxnLogChain {
	if c.err != nil {
		return c
	}

	_, c.err = c.logger.AppendCheckpointEnd(ATT, DPT)

	return c
}

func (c *TxnLogChain) Loc() LogRecordLocationInfo {
	return c.lastLocations[c.TransactionID]
}

func (c *TxnLogChain) Err() error {
	return c.err
}
