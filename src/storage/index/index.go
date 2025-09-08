package index

import (
	"fmt"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

type LinearProbingIndex struct {
	indexFileToken *txns.FileLockToken
	masterPage     *page.SlottedPage
	pool           bufferpool.BufferPool
	locker         txns.ILockManager
	logger         common.ITxnLoggerWithContext
}

var _ storage.Index = &LinearProbingIndex{}

func NewLinearProbingIndex(
	meta storage.IndexMeta,
	pool bufferpool.BufferPool,
	locker txns.ILockManager,
	logger common.ITxnLoggerWithContext,
) (*LinearProbingIndex, error) {
	cToken := txns.NewNilCatalogLockToken(logger.GetTxnID())
	index := &LinearProbingIndex{
		indexFileToken: txns.NewNilFileLockToken(cToken, meta.FileID),
		locker:         locker,
		logger:         logger,
	}

	err := index.initMasterPage()
	if err != nil {
		return nil, fmt.Errorf("failed to init master page: %w", err)
	}
	return index, nil
}

func getMasterPageIdent(fileID common.FileID) common.PageIdentity {
	return common.PageIdentity{
		FileID: fileID,
		PageID: common.PageID(0),
	}
}

func (i *LinearProbingIndex) initMasterPage() error {
	materPageIdent := getMasterPageIdent(i.indexFileToken.GetFileID())
	pToken := i.locker.LockPage(i.indexFileToken, materPageIdent.PageID, txns.PageLockShared)
	if pToken == nil {
		return fmt.Errorf("failed to lock page: %v", materPageIdent)
	}

	pg, err := i.pool.GetPage(materPageIdent)
	if err != nil {
		return fmt.Errorf("failed to get page: %w", err)
	}

	pg.RLock()
	if pg.NumSlots() == 4 {
		pg.RUnlock()
		return nil
	}
	pg.RUnlock()

	if !i.locker.UpgradePageLock(pToken, txns.PageLockExclusive) {
		return fmt.Errorf("failed to upgrade page lock: %v", materPageIdent)
	}
	err = i.pool.WithMarkDirty(
		i.logger.GetTxnID(),
		materPageIdent,
		pg,
		func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
			lockedPage.Clear()
			i.pool.MarkDirtyNoLogsAssumeLocked(materPageIdent)
			for range 3 {
				_, _, err := lockedPage.InsertWithLogs(
					utils.ToBytes[uint64](0),
					materPageIdent,
					i.logger,
				)
				if err != nil {
					err = fmt.Errorf("failed to insert slot: %w", err)
					return common.NewNilLogRecordLocation(), err
				}
			}
			_, loc, err := lockedPage.InsertWithLogs(
				utils.ToBytes[uint64](0),
				materPageIdent,
				i.logger,
			)
			if err != nil {
				err = fmt.Errorf("failed to insert slot: %w", err)
				return common.NewNilLogRecordLocation(), err
			}
			return loc, nil
		},
	)
	if err != nil {
		return fmt.Errorf("failed to mark dirty: %w", err)
	}
	i.masterPage = pg
	return nil
}

func (i *LinearProbingIndex) Get(key []byte) (common.RecordID, error) {
	return common.RecordID{}, nil
}

func (i *LinearProbingIndex) Delete(key []byte) error {
	return nil
}

func (i *LinearProbingIndex) Insert(key []byte, rid common.RecordID) error {
	return nil
}

func (i *LinearProbingIndex) Close() error {
	masterPageIdent := getMasterPageIdent(i.indexFileToken.GetFileID())
	i.pool.Unpin(masterPageIdent)
	return nil
}
