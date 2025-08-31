package engine

import (
	"github.com/spf13/afero"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/storage/disk"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

type SystemCatalog interface {
	GetNewFileID() uint64
	GetBasePath() string

	GetTableMeta(name string) (storage.Table, error)
	TableExists(name string) (bool, error)
	AddTable(req storage.Table) error
	DropTable(name string) error

	GetIndexMeta(name string) (storage.Index, error)
	IndexExists(name string) (bool, error)
	AddIndex(req storage.Index) error
	DropIndex(name string) error

	Save(logger common.ITxnLoggerWithContext) error
	CurrentVersion() uint64
}

type StorageEngine struct {
	catalog SystemCatalog
	pool    bufferpool.BufferPool
	diskMgr *disk.Manager
	locker  *txns.LockManager
	fs      afero.Fs

	indexLoader func(indexMeta storage.Index, locker *txns.LockManager, logger common.ITxnLoggerWithContext) (common.Index, error)
}

func New(
	sysCat SystemCatalog,
	pool bufferpool.BufferPool,
	diskMgr *disk.Manager,
	locker *txns.LockManager,
	fs afero.Fs,
	indexLoader func(indexMeta storage.Index, locker *txns.LockManager, logger common.ITxnLoggerWithContext) (common.Index, error),
) *StorageEngine {
	return &StorageEngine{
		catalog:     sysCat,
		diskMgr:     diskMgr,
		locker:      locker,
		fs:          fs,
		pool:        pool,
		indexLoader: indexLoader,
	}
}
