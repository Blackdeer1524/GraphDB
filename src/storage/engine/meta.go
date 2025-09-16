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
		err := fmt.Errorf("failed to upgrade catalog lock: %w", txns.ErrDeadlockPrevention)
		return storage.VertexTableMeta{}, err
	}

	err := s.catalog.Load()
	if err != nil {
		return storage.VertexTableMeta{}, err
	}

	return s.catalog.GetVertexTableMeta(name)
}

func (s *StorageEngine) GetEdgeTableMeta(
	name string,
	cToken *txns.CatalogLockToken,
) (storage.EdgeTableMeta, error) {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockShared) {
		err := fmt.Errorf("failed to upgrade catalog lock: %w", txns.ErrDeadlockPrevention)
		return storage.EdgeTableMeta{}, err
	}

	err := s.catalog.Load()
	if err != nil {
		return storage.EdgeTableMeta{}, err
	}

	return s.catalog.GetEdgeTableMeta(name)
}

func (s *StorageEngine) GetDirTableMeta(
	cToken *txns.CatalogLockToken,
	vertexTableFileID common.FileID,
) (storage.DirTableMeta, error) {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockShared) {
		err := fmt.Errorf("failed to upgrade catalog lock: %w", txns.ErrDeadlockPrevention)
		return storage.DirTableMeta{}, err
	}

	err := s.catalog.Load()
	if err != nil {
		return storage.DirTableMeta{}, err
	}

	return s.catalog.GetDirTableMeta(vertexTableFileID)
}

func (s *StorageEngine) GetVertexTableIndexMeta(
	name string,
	cToken *txns.CatalogLockToken,
) (storage.IndexMeta, error) {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockShared) {
		err := fmt.Errorf("failed to upgrade catalog lock: %w", txns.ErrDeadlockPrevention)
		return storage.IndexMeta{}, err
	}

	err := s.catalog.Load()
	if err != nil {
		return storage.IndexMeta{}, err
	}

	return s.catalog.GetVertexTableIndexMeta(name)
}

func (s *StorageEngine) GetVertexTableSystemIndexMeta(
	vertexTableFileID common.FileID,
	cToken *txns.CatalogLockToken,
) (storage.IndexMeta, error) {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockShared) {
		err := fmt.Errorf("failed to upgrade catalog lock: %w", txns.ErrDeadlockPrevention)
		return storage.IndexMeta{}, err
	}

	err := s.catalog.Load()
	if err != nil {
		return storage.IndexMeta{}, err
	}

	return s.catalog.GetVertexTableIndexMeta(getTableSystemIndexName(vertexTableFileID))
}

func (s *StorageEngine) GetEdgeIndexMeta(
	name string,
	cToken *txns.CatalogLockToken,
) (storage.IndexMeta, error) {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockShared) {
		err := fmt.Errorf("failed to upgrade catalog lock: %w", txns.ErrDeadlockPrevention)
		return storage.IndexMeta{}, err
	}

	err := s.catalog.Load()
	if err != nil {
		return storage.IndexMeta{}, err
	}

	return s.catalog.GetEdgeIndexMeta(name)
}

func (s *StorageEngine) GetEdgeTableSystemIndexMeta(
	edgeTableFileID common.FileID,
	cToken *txns.CatalogLockToken,
) (storage.IndexMeta, error) {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockShared) {
		err := fmt.Errorf("failed to upgrade catalog lock: %w", txns.ErrDeadlockPrevention)
		return storage.IndexMeta{}, err
	}

	err := s.catalog.Load()
	if err != nil {
		return storage.IndexMeta{}, err
	}

	return s.catalog.GetEdgeIndexMeta(getTableSystemIndexName(edgeTableFileID))
}

func (s *StorageEngine) GetDirTableSystemIndexMeta(
	dirTableFileID common.FileID,
	cToken *txns.CatalogLockToken,
) (storage.IndexMeta, error) {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockShared) {
		err := fmt.Errorf("failed to upgrade catalog lock: %w", txns.ErrDeadlockPrevention)
		return storage.IndexMeta{}, err
	}

	err := s.catalog.Load()
	if err != nil {
		return storage.IndexMeta{}, err
	}

	indexName := getTableSystemIndexName(dirTableFileID)
	return s.catalog.GetDirIndexMeta(indexName)
}

func (s *StorageEngine) GetEdgeTableMetaByFileID(
	edgeTableID common.FileID,
	cToken *txns.CatalogLockToken,
) (storage.EdgeTableMeta, error) {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockShared) {
		err := fmt.Errorf("couldn't upgrade catalog lock: %w", txns.ErrDeadlockPrevention)
		return storage.EdgeTableMeta{}, err
	}

	err := s.catalog.Load()
	if err != nil {
		return storage.EdgeTableMeta{}, err
	}

	name, err := s.catalog.GetEdgeTableNameByFileID(edgeTableID)
	if err != nil {
		return storage.EdgeTableMeta{}, err
	}

	meta, err := s.catalog.GetEdgeTableMeta(name)
	if err != nil {
		return storage.EdgeTableMeta{}, err
	}
	return meta, nil
}

func (s *StorageEngine) GetVertexTableMetaByFileID(
	vertexTableID common.FileID,
	cToken *txns.CatalogLockToken,
) (storage.VertexTableMeta, error) {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockShared) {
		err := fmt.Errorf("couldn't upgrade catalog lock: %w", txns.ErrDeadlockPrevention)
		return storage.VertexTableMeta{}, err
	}

	err := s.catalog.Load()
	if err != nil {
		return storage.VertexTableMeta{}, err
	}

	name, err := s.catalog.GetVertexTableNameByFileID(vertexTableID)
	if err != nil {
		return storage.VertexTableMeta{}, err
	}

	meta, err := s.catalog.GetVertexTableMeta(name)
	if err != nil {
		return storage.VertexTableMeta{}, err
	}
	return meta, nil
}
