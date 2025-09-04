package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"unsafe"

	"github.com/spf13/afero"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/storage/systemcatalog"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

type tableType string

const (
	vertexTableType    tableType = "vertex"
	edgeTableType      tableType = "edge"
	directoryTableType tableType = "directory"
)

func GetVertexTableFilePath(basePath, vertTableName string) string {
	return GetTableFilePath(basePath, vertexTableType, vertTableName)
}

func GetEdgeTableFilePath(basePath, edgeTableName string) string {
	return GetTableFilePath(basePath, edgeTableType, edgeTableName)
}

func GetDirectoryTableFilePath(basePath string, vertexTableFileID common.FileID) string {
	dirTableName := systemcatalog.GetDirTableName(vertexTableFileID)
	return GetTableFilePath(basePath, directoryTableType, dirTableName)
}

func GetVertexTableIndexFilePath(basePath, indexName string) string {
	return getIndexFilePath(basePath, vertexTableType, indexName)
}

func GetEdgeTableIndexFilePath(basePath, indexName string) string {
	return getIndexFilePath(basePath, edgeTableType, indexName)
}

func getDirTableIndexFilePath(basePath, indexName string) string {
	return getIndexFilePath(basePath, directoryTableType, indexName)
}

func GetTableFilePath(basePath string, tableT tableType, prefixedName string) string {
	return filepath.Join(basePath, "tables", string(tableT), prefixedName+".tbl")
}

func getIndexFilePath(basePath string, indexType tableType, prefixedName string) string {
	return filepath.Join(basePath, "indexes", string(indexType), prefixedName+".idx")
}

func getTableInternalIndexName(tableID common.FileID) string {
	return "idx_internal_" + strconv.Itoa(int(tableID))
}

func prepareFSforTable(fs afero.Fs, tableFilePath string) error {
	fileExists := true

	_, err := fs.Stat(tableFilePath)
	if err != nil {
		fileExists = false

		if !os.IsNotExist(err) {
			return fmt.Errorf("unable to check if file exists: %w", err)
		}
	}

	if fileExists {
		err = fs.Remove(tableFilePath)
		if err != nil {
			return fmt.Errorf("unable to remove file: %w", err)
		}
	}

	dir := filepath.Dir(tableFilePath)

	err = fs.MkdirAll(dir, 0o755)
	if err != nil {
		return fmt.Errorf("unable to create directory %s: %w", dir, err)
	}

	file, err := fs.Create(tableFilePath)
	if err != nil {
		return fmt.Errorf("unable to create directory: %w", err)
	}

	err = file.Sync()
	if err != nil {
		return fmt.Errorf("unable to sync file: %w", err)
	}

	_ = file.Close()
	return nil
}

func (s *StorageEngine) CreateVertexTable(
	txnID common.TxnID,
	tableName string,
	schema storage.Schema,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) error {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockExclusive) {
		return errors.New("unable to upgrade catalog lock")
	}

	err := s.catalog.Load()
	if err != nil {
		return fmt.Errorf("unable to load catalog: %w", err)
	}

	basePath := s.catalog.GetBasePath()
	tableFilePath := GetVertexTableFilePath(basePath, tableName)
	tableFileID := s.catalog.GetNewFileID()

	// Existence of the file is not the proof of existence of the table
	// (we don't remove file on drop), and it is why we do not check if 
	// the table exists in file system.
	ok, err := s.catalog.VertexTableExists(tableName)
	if err != nil {
		return fmt.Errorf("unable to check if table exists: %w", err)
	}
	if ok {
		return fmt.Errorf("table %s already exists", tableName)
	}

	err = prepareFSforTable(s.fs, tableFilePath)
	if err != nil {
		return fmt.Errorf("unable to prepare file system for table: %w", err)
	}

	// update info in metadata
	tblCreateReq := storage.VertexTableMeta{
		Name:       tableName,
		PathToFile: tableFilePath,
		FileID:     tableFileID,
		Schema:     schema,
	}

	err = s.catalog.AddVertexTable(tblCreateReq)
	if err != nil {
		return fmt.Errorf("unable to add table to catalog: %w", err)
	}

	err = s.catalog.CommitChanges(logger)
	if err != nil {
		return fmt.Errorf("unable to save catalog: %w", err)
	}

	err = s.createInternalVertexIndex(txnID, tableName, tableFileID, cToken, logger)
	if err != nil {
		return fmt.Errorf("unable to create internal vertex index: %w", err)
	}

	err = s.createDirTable(txnID, tableFileID, cToken, logger)
	if err != nil {
		return fmt.Errorf("unable to create directory table: %w", err)
	}

	s.diskMgr.InsertToFileMap(common.FileID(tableFileID), tableFilePath)
	return nil
}

func (s *StorageEngine) createDirTable(
	txnID common.TxnID,
	vertexTableFileID common.FileID,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) error {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockExclusive) {
		return errors.New("unable to upgrade catalog lock")
	}

	err := s.catalog.Load()
	if err != nil {
		return fmt.Errorf("unable to load catalog: %w", err)
	}

	basePath := s.catalog.GetBasePath()
	tableFilePath := GetDirectoryTableFilePath(basePath, vertexTableFileID)
	dirTableFileID := s.catalog.GetNewFileID()

	// Existence of the file is not the proof of existence of the table
	// (we don't remove file on drop),
	// and it is why we do not check if the table exists in file system.
	ok, err := s.catalog.DirTableExists(vertexTableFileID)
	if err != nil {
		return fmt.Errorf("unable to check if table exists: %w", err)
	}
	if ok {
		return fmt.Errorf("table %s already exists", vertexTableFileID)
	}

	err = prepareFSforTable(s.fs, tableFilePath)
	if err != nil {
		return fmt.Errorf("unable to prepare file system for table: %w", err)
	}

	// update info in metadata
	tblCreateReq := storage.DirTableMeta{
		VertexTableID: vertexTableFileID,
		FileID:        dirTableFileID,
		PathToFile:    tableFilePath,
	}

	err = s.catalog.AddDirTable(tblCreateReq)
	if err != nil {
		return fmt.Errorf("unable to add table to catalog: %w", err)
	}

	err = s.catalog.CommitChanges(logger)
	if err != nil {
		return fmt.Errorf("unable to save catalog: %w", err)
	}

	err = s.createInternalDirTableIndex(txnID, vertexTableFileID, dirTableFileID, cToken, logger)
	if err != nil {
		return fmt.Errorf("unable to create internal directory index: %w", err)
	}

	s.diskMgr.InsertToFileMap(common.FileID(dirTableFileID), tableFilePath)
	return nil
}

func (s *StorageEngine) CreateEdgeTable(
	txnID common.TxnID,
	tableName string,
	schema storage.Schema,
	srcVertexTableFileID common.FileID,
	dstVertexTableFileID common.FileID,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) error {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockExclusive) {
		return errors.New("unable to upgrade catalog lock")
	}

	err := s.catalog.Load()
	if err != nil {
		return fmt.Errorf("unable to load catalog: %w", err)
	}

	basePath := s.catalog.GetBasePath()
	tableFilePath := GetEdgeTableFilePath(basePath, tableName)
	tableFileID := s.catalog.GetNewFileID()

	// Existence of the file is not the proof of existence of the table
	// (we don't remove file on drop),
	// and it is why we do not check if the table exists in file system.
	ok, err := s.catalog.EdgeTableExists(tableName)
	if err != nil {
		return fmt.Errorf("unable to check if table exists: %w", err)
	}
	if ok {
		return fmt.Errorf("table %s already exists", tableName)
	}

	err = prepareFSforTable(s.fs, tableFilePath)
	if err != nil {
		return fmt.Errorf("unable to prepare file system for table: %w", err)
	}

	// update info in metadata
	tblCreateReq := storage.EdgeTableMeta{
		PathToFile:      tableFilePath,
		FileID:          tableFileID,
		Schema:          schema,
		Name:            tableName,
		SrcVertexFileID: srcVertexTableFileID,
		DstVertexFileID: dstVertexTableFileID,
	}

	err = s.catalog.AddEdgeTable(tblCreateReq)
	if err != nil {
		return fmt.Errorf("unable to add table to catalog: %w", err)
	}

	err = s.catalog.CommitChanges(logger)
	if err != nil {
		return fmt.Errorf("unable to save catalog: %w", err)
	}

	s.diskMgr.InsertToFileMap(common.FileID(tableFileID), tableFilePath)
	return nil
}

func (s *StorageEngine) DropVertexTable(
	txnID common.TxnID,
	vertTableName string,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) error {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockExclusive) {
		return errors.New("unable to upgrade catalog lock")
	}

	err := s.catalog.Load()
	if err != nil {
		return fmt.Errorf("unable to load catalog: %w", err)
	}

	vertTableMeta, err := s.catalog.GetVertexTableMeta(vertTableName)
	if err != nil {
		return fmt.Errorf("unable to get table meta: %w", err)
	}

	err = s.catalog.DropVertexTable(vertTableName)
	if err != nil {
		return err
	}

	err = s.catalog.DropVertexIndex(getTableInternalIndexName(vertTableMeta.FileID))
	if err != nil {
		return fmt.Errorf("unable to drop system index: %w", err)
	}

	err = s.catalog.CommitChanges(logger)
	if err != nil {
		return fmt.Errorf("unable to save catalog: %w", err)
	}

	err = s.dropDirTable(txnID, vertTableMeta.FileID, cToken, logger)
	if err != nil {
		return fmt.Errorf("unable to drop directory table: %w", err)
	}
	return nil
}

func (s *StorageEngine) DropEdgeTable(
	txnID common.TxnID,
	name string,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) error {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockExclusive) {
		return errors.New("unable to upgrade catalog lock")
	}

	err := s.catalog.Load()
	if err != nil {
		return fmt.Errorf("unable to load catalog: %w", err)
	}

	err = s.catalog.DropEdgeTable(name)
	if err != nil {
		return err
	}

	edgeTableMeta, err := s.catalog.GetEdgeTableMeta(name)
	if err != nil {
		return err
	}

	err = s.catalog.DropEdgeIndex(getTableInternalIndexName(edgeTableMeta.FileID))
	if err != nil {
		return fmt.Errorf("unable to drop system index: %w", err)
	}

	err = s.catalog.CommitChanges(logger)
	if err != nil {
		return fmt.Errorf("unable to save catalog: %w", err)
	}
	return nil
}

func (s *StorageEngine) dropDirTable(
	txnID common.TxnID,
	vertexTableFileID common.FileID,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) error {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockExclusive) {
		return errors.New("unable to upgrade catalog lock")
	}

	err := s.catalog.Load()
	if err != nil {
		return fmt.Errorf("unable to load catalog: %w", err)
	}

	err = s.catalog.DropDirTable(vertexTableFileID)
	if err != nil {
		return err
	}

	err = s.catalog.DropDirIndex(getTableInternalIndexName(vertexTableFileID))
	if err != nil {
		return err
	}

	err = s.catalog.CommitChanges(logger)
	if err != nil {
		return fmt.Errorf("unable to save catalog: %w", err)
	}
	return nil
}

func (s *StorageEngine) CreateVertexIndex(
	txnID common.TxnID,
	indexName string,
	tableName string,
	columns []string,
	keyBytesCnt uint32,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) error {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockExclusive) {
		return errors.New("unable to upgrade catalog lock")
	}

	err := s.catalog.Load()
	if err != nil {
		return fmt.Errorf("unable to load catalog: %w", err)
	}

	basePath := s.catalog.GetBasePath()
	fileID := s.catalog.GetNewFileID()
	tableFilePath := GetVertexTableIndexFilePath(basePath, indexName)
	ok, err := s.catalog.VertexIndexExists(indexName)
	if err != nil {
		return fmt.Errorf("unable to check if index exists: %w", err)
	}
	if ok {
		return fmt.Errorf("index %s already exists", indexName)
	}

	err = prepareFSforTable(s.fs, tableFilePath)
	if err != nil {
		return fmt.Errorf("unable to prepare file system for table: %w", err)
	}

	idxCreateReq := storage.IndexMeta{
		Name:        indexName,
		PathToFile:  tableFilePath,
		FileID:      fileID,
		TableName:   tableName,
		Columns:     columns,
		KeyBytesCnt: keyBytesCnt,
	}
	err = s.catalog.AddVertexIndex(idxCreateReq)
	if err != nil {
		return fmt.Errorf("unable to add index to catalog: %w", err)
	}

	err = s.catalog.CommitChanges(logger)
	if err != nil {
		return fmt.Errorf("unable to save catalog: %w", err)
	}
	s.diskMgr.InsertToFileMap(common.FileID(fileID), tableFilePath)
	return nil
}

func (s *StorageEngine) createInternalVertexIndex(
	txnID common.TxnID,
	tableName string,
	vertexTableFileID common.FileID,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) error {
	columns := []string{"ID"}
	keyBytesCnt := uint32(unsafe.Sizeof(storage.VertexID{}))
	return s.CreateVertexIndex(
		txnID,
		getTableInternalIndexName(vertexTableFileID),
		tableName,
		columns,
		keyBytesCnt,
		cToken,
		logger,
	)
}

func (s *StorageEngine) CreateEdgeIndex(
	txnID common.TxnID,
	indexName string,
	tableName string,
	columns []string,
	keyBytesCnt uint32,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) error {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockExclusive) {
		return errors.New("unable to upgrade catalog lock")
	}

	err := s.catalog.Load()
	if err != nil {
		return fmt.Errorf("unable to load catalog: %w", err)
	}

	basePath := s.catalog.GetBasePath()
	fileID := s.catalog.GetNewFileID()
	tableFilePath := GetEdgeTableIndexFilePath(basePath, indexName)
	ok, err := s.catalog.EdgeIndexExists(indexName)
	if err != nil {
		return fmt.Errorf("unable to check if index exists: %w", err)
	}
	if ok {
		return fmt.Errorf("index %s already exists", indexName)
	}

	err = prepareFSforTable(s.fs, tableFilePath)
	if err != nil {
		return fmt.Errorf("unable to prepare file system for table: %w", err)
	}

	idxCreateReq := storage.IndexMeta{
		Name:        indexName,
		PathToFile:  tableFilePath,
		FileID:      fileID,
		TableName:   tableName,
		Columns:     columns,
		KeyBytesCnt: keyBytesCnt,
	}
	err = s.catalog.AddEdgeIndex(idxCreateReq)
	if err != nil {
		return fmt.Errorf("unable to add index to catalog: %w", err)
	}

	err = s.catalog.CommitChanges(logger)
	if err != nil {
		return fmt.Errorf("unable to save catalog: %w", err)
	}
	s.diskMgr.InsertToFileMap(common.FileID(fileID), tableFilePath)
	return nil
}

func (s *StorageEngine) createInternalEdgeIndex(
	txnID common.TxnID,
	tableName string,
	edgeTableFileID common.FileID,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) error {
	columns := []string{"ID"}
	keyBytesCnt := uint32(unsafe.Sizeof(storage.EdgeID{}))
	return s.CreateEdgeIndex(
		txnID,
		getTableInternalIndexName(edgeTableFileID),
		tableName,
		columns,
		keyBytesCnt,
		cToken,
		logger,
	)
}

func (s *StorageEngine) GetVertexTableInternalIndex(
	txnID common.TxnID,
	vertexTableFileID common.FileID,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) (storage.Index, error) {
	return s.GetVertexTableIndex(
		txnID,
		getTableInternalIndexName(vertexTableFileID),
		cToken,
		logger,
	)
}

func (s *StorageEngine) GetVertexTableIndex(
	txnID common.TxnID,
	indexName string,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) (storage.Index, error) {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockShared) {
		return nil, errors.New("unable to upgrade catalog lock")
	}

	err := s.catalog.Load()
	if err != nil {
		return nil, fmt.Errorf("unable to load catalog: %w", err)
	}

	ok, err := s.catalog.VertexIndexExists(indexName)
	if err != nil {
		return nil, fmt.Errorf("unable to check if index exists: %w", err)
	}

	if !ok {
		return nil, fmt.Errorf("index %s does not exist", indexName)
	}

	indexMeta, err := s.catalog.GetVertexIndexMeta(indexName)
	if err != nil {
		return nil, fmt.Errorf("unable to get index meta: %w", err)
	}

	return s.indexLoader(indexMeta, s.locker, logger)
}

func (s *StorageEngine) GetEdgeTableInternalIndex(
	txnID common.TxnID,
	edgeTableFileID common.FileID,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) (storage.Index, error) {
	return s.GetEdgeTableIndex(txnID, getTableInternalIndexName(edgeTableFileID), cToken, logger)
}

func (s *StorageEngine) GetEdgeTableIndex(
	txnID common.TxnID,
	indexName string,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) (storage.Index, error) {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockShared) {
		return nil, errors.New("unable to upgrade catalog lock")
	}

	err := s.catalog.Load()
	if err != nil {
		return nil, fmt.Errorf("unable to load catalog: %w", err)
	}

	ok, err := s.catalog.EdgeIndexExists(indexName)
	if err != nil {
		return nil, fmt.Errorf("unable to check if index exists: %w", err)
	}

	if !ok {
		return nil, fmt.Errorf("index %s does not exist", indexName)
	}

	indexMeta, err := s.catalog.GetEdgeIndexMeta(indexName)
	if err != nil {
		return nil, fmt.Errorf("unable to get index meta: %w", err)
	}

	return s.indexLoader(indexMeta, s.locker, logger)
}

func (s *StorageEngine) GetDirTableInternalIndex(
	txnID common.TxnID,
	dirTableFileID common.FileID,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) (storage.Index, error) {
	return s.getDirTableIndex(txnID, getTableInternalIndexName(dirTableFileID), cToken, logger)
}

func (s *StorageEngine) createDirTableIndex(
	txnID common.TxnID,
	indexName string,
	vertexTableFileID common.FileID,
	columns []string,
	keyBytesCnt uint32,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) error {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockExclusive) {
		return errors.New("unable to get system catalog X-lock")
	}

	err := s.catalog.Load()
	if err != nil {
		return fmt.Errorf("unable to load catalog: %w", err)
	}

	basePath := s.catalog.GetBasePath()
	fileID := s.catalog.GetNewFileID()
	tableFilePath := getDirTableIndexFilePath(basePath, indexName)
	ok, err := s.catalog.DirIndexExists(indexName)
	if err != nil {
		return fmt.Errorf("unable to check if index exists: %w", err)
	}
	if ok {
		return fmt.Errorf("index %s already exists", indexName)
	}

	err = prepareFSforTable(s.fs, tableFilePath)
	if err != nil {
		return fmt.Errorf("unable to prepare file system for table: %w", err)
	}

	idxCreateReq := storage.IndexMeta{
		Name:        indexName,
		PathToFile:  tableFilePath,
		FileID:      fileID,
		TableName:   systemcatalog.GetDirTableName(vertexTableFileID),
		Columns:     columns,
		KeyBytesCnt: keyBytesCnt,
	}
	err = s.catalog.AddDirIndex(idxCreateReq)
	if err != nil {
		return fmt.Errorf("unable to add index to catalog: %w", err)
	}

	err = s.catalog.CommitChanges(logger)
	if err != nil {
		return fmt.Errorf("unable to save catalog: %w", err)
	}
	s.diskMgr.InsertToFileMap(common.FileID(fileID), tableFilePath)
	return nil
}

func (s *StorageEngine) createInternalDirTableIndex(
	txnID common.TxnID,
	vertexTableFileID common.FileID,
	directoryTableFileID common.FileID,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) error {
	columns := []string{"ID"}
	keyBytesCnt := uint32(unsafe.Sizeof(storage.DirItemID{}))
	return s.createDirTableIndex(
		txnID,
		getTableInternalIndexName(directoryTableFileID),
		vertexTableFileID,
		columns,
		keyBytesCnt,
		cToken,
		logger,
	)
}

func (s *StorageEngine) getDirTableIndex(
	txnID common.TxnID,
	indexName string,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) (storage.Index, error) {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockShared) {
		return nil, errors.New("unable to upgrade catalog lock")
	}

	err := s.catalog.Load()
	if err != nil {
		return nil, fmt.Errorf("unable to load catalog: %w", err)
	}

	ok, err := s.catalog.DirIndexExists(indexName)
	if err != nil {
		return nil, fmt.Errorf("unable to check if index exists: %w", err)
	}

	if !ok {
		return nil, fmt.Errorf("index %s does not exist", indexName)
	}

	indexMeta, err := s.catalog.GetDirIndexMeta(indexName)
	if err != nil {
		return nil, fmt.Errorf("unable to get index meta: %w", err)
	}

	return s.indexLoader(indexMeta, s.locker, logger)
}

func (s *StorageEngine) DropVertexTableIndex(
	txnID common.TxnID,
	indexName string,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) error {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockExclusive) {
		return errors.New("unable to upgrade catalog lock")
	}

	err := s.catalog.Load()
	if err != nil {
		return fmt.Errorf("unable to load catalog: %w", err)
	}

	err = s.catalog.DropVertexIndex(indexName)
	if err != nil {
		return fmt.Errorf("unable to drop index: %w", err)
	}

	err = s.catalog.CommitChanges(logger)
	if err != nil {
		return fmt.Errorf("unable to save catalog: %w", err)
	}
	return nil
}

func (s *StorageEngine) DropEdgesTableIndex(
	txnID common.TxnID,
	indexName string,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) error {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockExclusive) {
		return errors.New("unable to get system catalog X-lock")
	}

	err := s.catalog.Load()
	if err != nil {
		return fmt.Errorf("unable to load catalog: %w", err)
	}

	err = s.catalog.DropEdgeIndex(indexName)
	if err != nil {
		return fmt.Errorf("unable to drop index: %w", err)
	}

	err = s.catalog.CommitChanges(logger)
	if err != nil {
		return fmt.Errorf("unable to save catalog: %w", err)
	}
	return nil
}

func (s *StorageEngine) dropDirTableIndex(
	txnID common.TxnID,
	indexName string,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) error {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockExclusive) {
		return errors.New("unable to get system catalog X-lock")
	}

	err := s.catalog.Load()
	if err != nil {
		return fmt.Errorf("unable to load catalog: %w", err)
	}

	err = s.catalog.DropDirIndex(indexName)
	if err != nil {
		return fmt.Errorf("unable to drop index: %w", err)
	}

	err = s.catalog.CommitChanges(logger)
	if err != nil {
		return fmt.Errorf("unable to save catalog: %w", err)
	}
	return nil
}
