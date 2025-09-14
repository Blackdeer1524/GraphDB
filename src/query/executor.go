package query

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/recovery"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/storage/disk"
	"github.com/Blackdeer1524/GraphDB/src/storage/engine"
	"github.com/Blackdeer1524/GraphDB/src/storage/index"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
	"github.com/Blackdeer1524/GraphDB/src/storage/systemcatalog"
	"github.com/Blackdeer1524/GraphDB/src/txns"
	"github.com/spf13/afero"
)

var ErrRollback = errors.New("rollback")

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

func SetupExecutor(
	fs afero.Fs,
	catalogBasePath string,
	poolPageCount uint64,
	debugMode bool,
) (*Executor, *bufferpool.DebugBufferPool, *txns.LockManager, *recovery.TxnLogger, error) {
	err := systemcatalog.InitSystemCatalog(catalogBasePath, fs)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	err = systemcatalog.CreateLogFileIfDoesntExist(catalogBasePath, fs)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	diskMgr := disk.New(
		catalogBasePath,
		func(fileID common.FileID, pageID common.PageID) page.SlottedPage {
			return page.NewSlottedPage()
		},
		fs,
	)

	pool := bufferpool.New(poolPageCount, bufferpool.NewLRUReplacer(), diskMgr)
	debugPool := bufferpool.NewDebugBufferPool(pool)

	debugPool.MarkPageAsLeaking(systemcatalog.CatalogVersionPageIdent())
	debugPool.MarkPageAsLeaking(recovery.GetMasterPageIdent(systemcatalog.LogFileID))

	logger := recovery.NewTxnLogger(pool, systemcatalog.LogFileID)
	sysCat, err := systemcatalog.New(catalogBasePath, fs, debugPool)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	locker := txns.NewLockManager()
	indexLoader := func(
		indexMeta storage.IndexMeta,
		pool bufferpool.BufferPool,
		locker *txns.LockManager,
		logger common.ITxnLoggerWithContext,
	) (storage.Index, error) {
		return index.NewLinearProbingIndex(
			indexMeta,
			pool,
			locker,
			logger,
			debugMode,
			42,
		)
	}

	se := engine.New(
		sysCat,
		debugPool,
		diskMgr.GetLastFilePage,
		diskMgr.GetEmptyPage,
		locker,
		fs,
		indexLoader,
		debugMode,
	)
	executor := New(se, locker)
	return executor, debugPool, locker, logger, nil
}

func Execute(
	ticker *atomic.Uint64,
	executor *Executor,
	logger common.ITxnLogger,
	fn Task,
	isReadOnly bool, // не будет писать логов -> немного получше скорость
) (err error) {
	txnID := common.TxnID(ticker.Add(1))
	defer executor.locker.Unlock(txnID)

	ctxLogger := logger.WithContext(txnID)
	if !isReadOnly {
		if err := ctxLogger.AppendBegin(); err != nil {
			return fmt.Errorf("failed to append begin: %w", err)
		}
	}

	defer func() {
		if isReadOnly {
			return
		}

		if err != nil {
			ctxLogger.Rollback()
			if err == ErrRollback || errors.Is(err, txns.ErrDeadlockPrevention) {
				err = nil
				return
			}
			return
		}
		if err = ctxLogger.AppendCommit(); err != nil {
			err = fmt.Errorf("failed to append commit: %w", err)
		} else if err = ctxLogger.AppendTxnEnd(); err != nil {
			err = fmt.Errorf("failed to append txn end: %w", err)
		}
	}()

	return fn(txnID, executor, ctxLogger)
}
