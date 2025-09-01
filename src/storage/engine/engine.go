package engine

import (
	"fmt"

	"github.com/spf13/afero"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/storage/disk"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
	"github.com/Blackdeer1524/GraphDB/src/storage/systemcatalog"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

type StorageEngine struct {
	catalog storage.SystemCatalog
	pool    bufferpool.BufferPool
	diskMgr *disk.Manager
	locker  *txns.LockManager
	fs      afero.Fs

	indexLoader func(indexMeta storage.IndexMeta, locker *txns.LockManager, logger common.ITxnLoggerWithContext) (storage.Index, error)
}

// NewAggregationAssociativeArray implements storage.StorageEngine.
func (s *StorageEngine) NewAggregationAssociativeArray(
	common.TxnID,
) (storage.AssociativeArray[storage.VertexID, float64], error) {
	panic("unimplemented")
}

// NewBitMap implements storage.StorageEngine.
func (s *StorageEngine) NewBitMap(common.TxnID) (storage.BitMap, error) {
	panic("unimplemented")
}

// NewQueue implements storage.StorageEngine.
func (s *StorageEngine) NewQueue(common.TxnID) (storage.Queue, error) {
	panic("unimplemented")
}

// AllVerticesWithValue implements storage.StorageEngine.
func (s *StorageEngine) AllVerticesWithValue(
	t common.TxnID,
	field string,
	value []byte,
) (storage.VerticesIter, error) {
	panic("unimplemented")
}

// CountOfFilteredEdges implements storage.StorageEngine.
func (s *StorageEngine) CountOfFilteredEdges(
	t common.TxnID,
	v storage.VertexID,
	f storage.EdgeFilter,
) (uint64, error) {
	panic("unimplemented")
}

// GetAllVertices implements storage.StorageEngine.
func (s *StorageEngine) GetAllVertices(t common.TxnID) (storage.VerticesIter, error) {
	panic("unimplemented")
}

// GetNeighborsWithEdgeFilter implements storage.StorageEngine.
func (s *StorageEngine) GetNeighborsWithEdgeFilter(
	t common.TxnID,
	v storage.VertexID,
	filter storage.EdgeFilter,
) (storage.VerticesIter, error) {
	panic("unimplemented")
}

// Neighbours implements storage.StorageEngine.
func (s *StorageEngine) Neighbours(
	t common.TxnID,
	v storage.VertexID,
) (storage.NeighborIter, error) {
	panic("unimplemented")
}

var _ storage.StorageEngine = &StorageEngine{}

func New(
	catalogBasePath string,
	poolSize uint64,
	locker *txns.LockManager,
	fs afero.Fs,
	indexLoader func(indexMeta storage.IndexMeta, locker *txns.LockManager, logger common.ITxnLoggerWithContext) (storage.Index, error),
) (*StorageEngine, error) {
	err := systemcatalog.InitSystemCatalog(catalogBasePath, fs)
	if err != nil {
		return nil, fmt.Errorf("failed to : %w", err)
	}

	fileIDToFilePath := map[common.FileID]string{
		common.FileID(0): systemcatalog.GetSystemCatalogVersionFileName(catalogBasePath),
	}

	diskMgr := disk.New(
		fileIDToFilePath,
		func(fileID common.FileID, pageID common.PageID) *page.SlottedPage {
			// TODO: implement this
			return page.NewSlottedPage()
		},
	)

	bpManager := bufferpool.New(poolSize, &bufferpool.LRUReplacer{}, diskMgr)
	sysCat, err := systemcatalog.New(catalogBasePath, fs, bpManager)
	if err != nil {
		return nil, fmt.Errorf("failed to create systemcatalog: %w", err)
	}

	diskMgr.UpdateFileMap(sysCat.GetFileIDToPathMap())

	return newInjectedEngine(sysCat, bpManager, diskMgr, locker, fs, indexLoader), nil
}

func newInjectedEngine(
	sysCat storage.SystemCatalog,
	pool bufferpool.BufferPool,
	diskMgr *disk.Manager,
	locker *txns.LockManager,
	fs afero.Fs,
	indexLoader func(indexMeta storage.IndexMeta, locker *txns.LockManager, logger common.ITxnLoggerWithContext) (storage.Index, error),
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
