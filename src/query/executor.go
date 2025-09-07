package query

import (
	"sync/atomic"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

type Executor struct {
	se        storage.Engine
	txnTicker atomic.Uint64
	locker    txns.ILockManager
	logger    common.ITxnLogger
}

func New(
	se storage.Engine,
	locker txns.ILockManager,
	logger common.ITxnLogger,
) *Executor {
	return &Executor{
		se:     se,
		locker: locker,
		logger: logger,
	}
}
