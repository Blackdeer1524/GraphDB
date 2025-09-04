package engine

import (
	"fmt"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

func (s *StorageEngine) GetVertexTableMeta(
	name string,
	cToken *txns.CatalogLockToken,
) (storage.VertexTableMeta, error) {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockShared) {
		err := fmt.Errorf("failed to upgrade catalog lock")
		return storage.VertexTableMeta{}, err
	}
	return s.catalog.GetVertexTableMeta(name)
}

func (s *StorageEngine) GetEdgeTableMeta(
	name string,
	cToken *txns.CatalogLockToken,
) (storage.EdgeTableMeta, error) {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockShared) {
		err := fmt.Errorf("failed to upgrade catalog lock")
		return storage.EdgeTableMeta{}, err
	}
	return s.catalog.GetEdgeTableMeta(name)
}

func (s *StorageEngine) GetDirectoryTableMeta(
	cToken *txns.CatalogLockToken,
	vertexTableFileID common.FileID,
) (storage.DirTableMeta, error) {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockShared) {
		err := fmt.Errorf("failed to upgrade catalog lock")
		return storage.DirTableMeta{}, err
	}
	return s.catalog.GetDirectoryTableMeta(vertexTableFileID)
}

func (s *StorageEngine) GetVertexIndexMeta(
	name string,
	cToken *txns.CatalogLockToken,
) (storage.IndexMeta, error) {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockShared) {
		err := fmt.Errorf("failed to upgrade catalog lock")
		return storage.IndexMeta{}, err
	}
	return s.catalog.GetVertexIndexMeta(name)
}

func (s *StorageEngine) GetVertexInternalIndexMeta(
	vertexTableFileID common.FileID,
	cToken *txns.CatalogLockToken,
) (storage.IndexMeta, error) {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockShared) {
		err := fmt.Errorf("failed to upgrade catalog lock")
		return storage.IndexMeta{}, err
	}
	return s.catalog.GetVertexIndexMeta(getTableInternalIndexName(vertexTableFileID))
}

func (s *StorageEngine) GetEdgeIndexMeta(
	name string,
	cToken *txns.CatalogLockToken,
) (storage.IndexMeta, error) {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockShared) {
		err := fmt.Errorf("failed to upgrade catalog lock")
		return storage.IndexMeta{}, err
	}
	return s.catalog.GetEdgeIndexMeta(name)
}

func (s *StorageEngine) GetEdgeInternalIndexMeta(
	edgeTableFileID common.FileID,
	cToken *txns.CatalogLockToken,
) (storage.IndexMeta, error) {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockShared) {
		err := fmt.Errorf("failed to upgrade catalog lock")
		return storage.IndexMeta{}, err
	}
	return s.catalog.GetEdgeIndexMeta(getTableInternalIndexName(edgeTableFileID))
}

func (s *StorageEngine) GetDirectoryInternalIndexMeta(
	dirTableFileID common.FileID,
	cToken *txns.CatalogLockToken,
) (storage.IndexMeta, error) {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockShared) {
		err := fmt.Errorf("failed to upgrade catalog lock")
		return storage.IndexMeta{}, err
	}

	indexName := getTableInternalIndexName(dirTableFileID)
	return s.catalog.GetDirectoryIndexMeta(indexName)
}
