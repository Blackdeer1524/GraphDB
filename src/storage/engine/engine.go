package engine

import (
	"github.com/spf13/afero"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

type StorageEngine struct {
	catalog             storage.SystemCatalog
	pool                bufferpool.BufferPool
	diskMgrGetLastPage  func(fileID common.FileID) (common.PageID, error)
	diskMgrGetEmptyPage func(fileID common.FileID) (common.PageID, error)

	locker *txns.LockManager
	fs     afero.Fs

	loadIndex func(
		indexMeta storage.IndexMeta,
		pool bufferpool.BufferPool,
		locker *txns.LockManager,
		logger common.ITxnLoggerWithContext,
	) (storage.Index, error)
	debugAsserts bool
}

var _ storage.Engine = &StorageEngine{}

func New(
	sysCat storage.SystemCatalog,
	pool bufferpool.BufferPool,
	diskMgrGetLastPage func(fileID common.FileID) (common.PageID, error),
	diskMgrGetEmptyPage func(fileID common.FileID) (common.PageID, error),
	locker *txns.LockManager,
	fs afero.Fs,
	indexLoader func(
		indexMeta storage.IndexMeta,
		pool bufferpool.BufferPool,
		locker *txns.LockManager,
		logger common.ITxnLoggerWithContext,
	) (storage.Index, error),
	debugAsserts bool,
) *StorageEngine {
	return &StorageEngine{
		catalog:             sysCat,
		diskMgrGetLastPage:  diskMgrGetLastPage,
		diskMgrGetEmptyPage: diskMgrGetEmptyPage,
		locker:              locker,
		fs:                  fs,
		pool:                pool,
		loadIndex:           indexLoader,
		debugAsserts:        debugAsserts,
	}
}
