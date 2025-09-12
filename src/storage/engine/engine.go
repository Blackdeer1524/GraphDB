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
}

var _ storage.Engine = &StorageEngine{}

// func New(
// 	catalogBasePath string,
// 	pool bufferpool.BufferPool,
// 	locker *txns.LockManager,
// 	fs afero.Fs,
// 	indexLoader func(indexMeta storage.IndexMeta, locker *txns.LockManager, logger
// common.ITxnLoggerWithContext) (storage.Index, error),
// ) (*StorageEngine, error) {
// 	err := systemcatalog.InitSystemCatalog(catalogBasePath, fs)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to : %w", err)
// 	}
//
// 	versionFileName := systemcatalog.GetSystemCatalogVersionFileName(catalogBasePath)
// 	fileIDToFilePath := map[common.FileID]string{
// 		systemcatalog.CatalogVersionFileID: versionFileName,
// 	}
//
// 	diskMgr := disk.New(
// 		fileIDToFilePath,
// 		func(fileID common.FileID, pageID common.PageID) *page.SlottedPage {
// 			return page.NewSlottedPage()
// 		},
// 		fs,
// 	)
//
// 	sysCat, err := systemcatalog.New(catalogBasePath, fs, pool)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to create systemcatalog: %w", err)
// 	}
//
// 	diskMgr.UpdateFileMap(sysCat.GetFileIDToPathMap())
// 	return newInjectedEngine(sysCat, pool, diskMgr.InsertToFileMap, locker, fs, indexLoader), nil
// }

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
) *StorageEngine {
	return &StorageEngine{
		catalog:             sysCat,
		diskMgrGetLastPage:  diskMgrGetLastPage,
		diskMgrGetEmptyPage: diskMgrGetEmptyPage,
		locker:              locker,
		fs:                  fs,
		pool:                pool,
		loadIndex:           indexLoader,
	}
}
