package query

import (
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

type Task func(txnID common.TxnID, e *Executor, logger common.ITxnLoggerWithContext) (err error)

type Executor struct {
	se     storage.Engine
	locker txns.ILockManager
}

func New(
	se storage.Engine,
	locker txns.ILockManager,
) *Executor {
	return &Executor{
		se:     se,
		locker: locker,
	}
}
