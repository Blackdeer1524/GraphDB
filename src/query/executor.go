package query

import (
	"sync/atomic"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

type Executor struct {
	se        storage.StorageEngine
	txnTicker atomic.Uint64
	locker    txns.ILockManager
	logger    common.ITxnLogger
}

func New(
	se storage.StorageEngine,
	locker txns.ILockManager,
	logger common.ITxnLogger,
) *Executor {
	return &Executor{
		se:     se,
		locker: locker,
		logger: logger,
	}
}
