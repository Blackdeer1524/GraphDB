package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/spf13/afero"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

func GetVertexTableFilePath(basePath, vertTableName string) string {
	return GetTableFilePath(basePath, "vertex", vertTableName)
}

func GetEdgeTableFilePath(basePath, edgeTableName string) string {
	return GetTableFilePath(basePath, "edge", edgeTableName)
}

func GetDirectoryTableFilePath(basePath, directoryTableName string) string {
	return GetTableFilePath(basePath, "directory", directoryTableName)
}

func GetVertexIndexFilePath(basePath, indexName string) string {
	return getIndexFilePath(basePath, "vertex", indexName)
}

func GetEdgeIndexFilePath(basePath, indexName string) string {
	return getIndexFilePath(basePath, "edge", indexName)
}

func GetDirectoryIndexFilePath(basePath, indexName string) string {
	return getIndexFilePath(basePath, "directory", indexName)
}

func GetTableFilePath(basePath, tableType string, prefixedName string) string {
	return filepath.Join(basePath, "tables", tableType, prefixedName+".tbl")
}

func getIndexFilePath(basePath, indexType, prefixedName string) string {
	return filepath.Join(basePath, "indexes", indexType, prefixedName+".idx")
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
	needToRollback := true

	basePath := s.catalog.GetBasePath()
	tableFilePath := GetVertexTableFilePath(basePath, tableName)
	tableFileID := s.catalog.GetNewFileID()

	// Existence of the file is not the proof of existence of the table
	// (we don't remove file on drop),
	// and it is why we do not check if the table exists in file system.
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

	defer func() {
		if needToRollback {
			_ = s.fs.Remove(tableFilePath)
		}
	}()

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
	defer func() {
		if needToRollback {
			_ = s.catalog.DropVertexTable(tableName)
		}
	}()

	err = s.catalog.Save(logger)
	if err != nil {
		return fmt.Errorf("unable to save catalog: %w", err)
	}

	s.diskMgr.InsertToFileMap(common.FileID(tableFileID), tableFilePath)

	err = s.createDirectoryTable(txnID, tableName, tableFileID, cToken, logger)
	if err != nil {
		return fmt.Errorf("unable to create directory table: %w", err)
	}

	needToRollback = false
	return nil
}

func (s *StorageEngine) createDirectoryTable(
	txnID common.TxnID,
	vertexTableName string,
	vertexTableFileID common.FileID,
	_ *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) error {
	needToRollback := true

	basePath := s.catalog.GetBasePath()
	tableFilePath := GetDirectoryTableFilePath(basePath, vertexTableName)
	tableFileID := s.catalog.GetNewFileID()

	// Existence of the file is not the proof of existence of the table
	// (we don't remove file on drop),
	// and it is why we do not check if the table exists in file system.
	ok, err := s.catalog.DirectoryTableExists(vertexTableName)
	if err != nil {
		return fmt.Errorf("unable to check if table exists: %w", err)
	}
	if ok {
		return fmt.Errorf("table %s already exists", vertexTableName)
	}

	err = prepareFSforTable(s.fs, tableFilePath)
	if err != nil {
		return fmt.Errorf("unable to prepare file system for table: %w", err)
	}

	defer func() {
		if needToRollback {
			_ = s.fs.Remove(tableFilePath)
		}
	}()

	// update info in metadata
	tblCreateReq := storage.DirectoryTableMeta{
		Name:              vertexTableName,
		PathToFile:        tableFilePath,
		FileID:            tableFileID,
		VertexTableFileID: vertexTableFileID,
	}

	err = s.catalog.AddDirectoryTable(tblCreateReq)
	if err != nil {
		return fmt.Errorf("unable to add table to catalog: %w", err)
	}
	defer func() {
		if needToRollback {
			_ = s.catalog.DropDirectoryTable(vertexTableName)
		}
	}()

	err = s.catalog.Save(logger)
	if err != nil {
		return fmt.Errorf("unable to save catalog: %w", err)
	}

	s.diskMgr.InsertToFileMap(common.FileID(tableFileID), tableFilePath)
	needToRollback = false
	return nil
}

func (s *StorageEngine) CreateEdgeTable(
	txnID common.TxnID,
	tableName string,
	schema storage.Schema,
	srcVertexTableFileID common.FileID,
	dstVertexTableFileID common.FileID,
	logger common.ITxnLoggerWithContext,
) error {
	needToRollback := true

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

	defer func() {
		if needToRollback {
			_ = s.fs.Remove(tableFilePath)
		}
	}()

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
	defer func() {
		if needToRollback {
			_ = s.catalog.DropEdgeTable(tableName)
		}
	}()

	err = s.catalog.Save(logger)
	if err != nil {
		return fmt.Errorf("unable to save catalog: %w", err)
	}

	s.diskMgr.InsertToFileMap(common.FileID(tableFileID), tableFilePath)
	needToRollback = false
	return nil
}

func (s *StorageEngine) DropVertexTable(
	txnID common.TxnID,
	vertTableName string,
	logger common.ITxnLoggerWithContext,
) error {
	cToken := s.locker.LockCatalog(txnID, txns.GranularLockExclusive)
	if cToken == nil {
		return errors.New("unable to get system catalog X-lock")
	}
	defer s.locker.Unlock(txnID)

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

	dirTableMeta, err := s.catalog.GetDirectoryTableMeta(vertTableName)
	if err != nil {
		return fmt.Errorf("unable to get table meta: %w", err)
	}

	err = s.catalog.DropDirectoryTable(vertTableName)
	if err != nil {
		return err
	}

	err = s.catalog.DropDirectoryIndex(getTableInternalIndexName(dirTableMeta.FileID))
	if err != nil {
		return err
	}

	return nil
}

func (s *StorageEngine) DropEdgeTable(
	txnID common.TxnID,
	name string,
	logger common.ITxnLoggerWithContext,
) error {
	cToken := s.locker.LockCatalog(txnID, txns.GranularLockExclusive)
	if cToken == nil {
		return errors.New("unable to get system catalog X-lock")
	}
	defer s.locker.Unlock(txnID)

	err := s.catalog.DropEdgeTable(name)
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

	return nil
}

func (s *StorageEngine) createIndex(
	txnID common.TxnID,
	name string,
	tableName string,
	columns []string,
	keyBytesCnt uint32,
	_ *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) error {
	needToRollback := true

	basePath := s.catalog.GetBasePath()
	fileID := s.catalog.GetNewFileID()
	tableFilePath := getIndexFilePath(basePath, name)

	// Existence of the file is not the proof of existence of the index
	// (we don't remove file on drop), and it is why we do not check if the
	// index exists in file system.
	ok, err := s.catalog.IndexExists(name)
	if err != nil {
		return fmt.Errorf("unable to check if index exists: %w", err)
	}

	if ok {
		return fmt.Errorf("index %s already exists", name)
	}

	fileExists := true

	_, err = s.fs.Stat(tableFilePath)
	if err != nil {
		fileExists = false

		if !os.IsNotExist(err) {
			return fmt.Errorf("unable to check if file exists: %w", err)
		}
	}

	if fileExists {
		err = s.fs.Remove(tableFilePath)
		if err != nil {
			return fmt.Errorf("unable to remove file: %w", err)
		}
	}

	dir := filepath.Dir(tableFilePath)

	err = s.fs.MkdirAll(dir, 0o755)
	if err != nil {
		return fmt.Errorf("unable to create directory %s: %w", dir, err)
	}

	file, err := s.fs.Create(tableFilePath)
	if err != nil {
		return fmt.Errorf("unable to create file: %w", err)
	}
	_ = file.Close()
	defer func() {
		if needToRollback {
			_ = s.fs.Remove(tableFilePath)
		}
	}()

	// update info in metadata

	idxCreateReq := storage.IndexMeta{
		Name:        name,
		PathToFile:  tableFilePath,
		FileID:      fileID,
		TableName:   tableName,
		Columns:     columns,
		KeyBytesCnt: keyBytesCnt,
	}

	err = s.catalog.AddIndex(idxCreateReq)
	if err != nil {
		return fmt.Errorf("unable to add index to catalog: %w", err)
	}
	defer func() {
		if needToRollback {
			_ = s.catalog.DropIndex(name)
		}
	}()

	err = s.catalog.Save(logger)
	if err != nil {
		return fmt.Errorf("unable to save catalog: %w", err)
	}

	s.diskMgr.InsertToFileMap(common.FileID(fileID), tableFilePath)
	needToRollback = false
	return nil
}

func (s *StorageEngine) CreateVertexIndex(
	txnID common.TxnID,
	indexName string,
	tableName string,
	columns []string,
	keyBytesCnt uint32,
	logger common.ITxnLoggerWithContext,
) error {
	cToken := s.locker.LockCatalog(txnID, txns.GranularLockExclusive)
	if cToken == nil {
		return errors.New("unable to get system catalog X-lock")
	}
	defer s.locker.Unlock(txnID)

	needToRollback := true
	basePath := s.catalog.GetBasePath()
	fileID := s.catalog.GetNewFileID()
	tableFilePath := GetVertexIndexFilePath(basePath, indexName)
	ok, err := s.catalog.VertexIndexExists(indexName)
	if err != nil {
		return fmt.Errorf("unable to check if index exists: %w", err)
	}
	if ok {
		return fmt.Errorf("index %s already exists", indexName)
	}

	prepareFSforTable(s.fs, tableFilePath)
	defer func() {
		if needToRollback {
			_ = s.fs.Remove(tableFilePath)
		}
	}()
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
	defer func() {
		if needToRollback {
			_ = s.catalog.DropVertexIndex(indexName)
		}
	}()
	err = s.catalog.Save(logger)
	if err != nil {
		return fmt.Errorf("unable to save catalog: %w", err)
	}
	s.diskMgr.InsertToFileMap(common.FileID(fileID), tableFilePath)
	needToRollback = false
	return nil
}

func (s *StorageEngine) CreateEdgeIndex(
	txnID common.TxnID,
	indexName string,
	tableName string,
	columns []string,
	keyBytesCnt uint32,
	logger common.ITxnLoggerWithContext,
) error {
	cToken := s.locker.LockCatalog(txnID, txns.GranularLockExclusive)
	if cToken == nil {
		return errors.New("unable to get system catalog X-lock")
	}
	defer s.locker.Unlock(txnID)

	needToRollback := true
	basePath := s.catalog.GetBasePath()
	fileID := s.catalog.GetNewFileID()
	tableFilePath := GetEdgeIndexFilePath(basePath, indexName)
	ok, err := s.catalog.EdgeIndexExists(indexName)
	if err != nil {
		return fmt.Errorf("unable to check if index exists: %w", err)
	}
	if ok {
		return fmt.Errorf("index %s already exists", indexName)
	}

	prepareFSforTable(s.fs, tableFilePath)
	defer func() {
		if needToRollback {
			_ = s.fs.Remove(tableFilePath)
		}
	}()
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
	defer func() {
		if needToRollback {
			_ = s.catalog.DropEdgeIndex(indexName)
		}
	}()
	err = s.catalog.Save(logger)
	if err != nil {
		return fmt.Errorf("unable to save catalog: %w", err)
	}
	s.diskMgr.InsertToFileMap(common.FileID(fileID), tableFilePath)
	needToRollback = false
	return nil
}

func (s *StorageEngine) GetVertexInternalIndex(
	txnID common.TxnID,
	vertexTableFileID common.FileID,
	logger common.ITxnLoggerWithContext,
) (storage.Index, error) {
	return s.GetVertexIndex(txnID, getTableInternalIndexName(vertexTableFileID), logger)
}

func (s *StorageEngine) GetVertexIndex(
	txnID common.TxnID,
	indexName string,
	logger common.ITxnLoggerWithContext,
) (storage.Index, error) {
	ctoken := s.locker.LockCatalog(txnID, txns.GranularLockShared)
	if ctoken == nil {
		return nil, errors.New("unable to get system catalog lock")
	}
	defer s.locker.Unlock(txnID)

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

func (s *StorageEngine) GetEdgeInternalIndex(
	txnID common.TxnID,
	edgeTableFileID common.FileID,
	logger common.ITxnLoggerWithContext,
) (storage.Index, error) {
	return s.GetEdgeIndex(txnID, getTableInternalIndexName(edgeTableFileID), logger)
}

func (s *StorageEngine) GetEdgeIndex(
	txnID common.TxnID,
	indexName string,
	logger common.ITxnLoggerWithContext,
) (storage.Index, error) {
	ctoken := s.locker.LockCatalog(txnID, txns.GranularLockShared)
	if ctoken == nil {
		return nil, errors.New("unable to get system catalog lock")
	}
	defer s.locker.Unlock(txnID)

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

func (s *StorageEngine) GetDirectoryInternalIndex(
	txnID common.TxnID,
	dirTableFileID common.FileID,
	logger common.ITxnLoggerWithContext,
) (storage.Index, error) {
	return s.GetDirectoryIndex(txnID, getTableInternalIndexName(dirTableFileID), logger)
}

func (s *StorageEngine) GetDirectoryIndex(
	txnID common.TxnID,
	indexName string,
	logger common.ITxnLoggerWithContext,
) (storage.Index, error) {
	ctoken := s.locker.LockCatalog(txnID, txns.GranularLockShared)
	if ctoken == nil {
		return nil, errors.New("unable to get system catalog lock")
	}
	defer s.locker.Unlock(txnID)

	ok, err := s.catalog.DirectoryIndexExists(indexName)
	if err != nil {
		return nil, fmt.Errorf("unable to check if index exists: %w", err)
	}

	if !ok {
		return nil, fmt.Errorf("index %s does not exist", indexName)
	}

	indexMeta, err := s.catalog.GetDirectoryIndexMeta(indexName)
	if err != nil {
		return nil, fmt.Errorf("unable to get index meta: %w", err)
	}

	return s.indexLoader(indexMeta, s.locker, logger)
}

func (s *StorageEngine) GetVertexTableInternalIndex(
	txnID common.TxnID,
	vertexTableFileID common.FileID,
	logger common.ITxnLoggerWithContext,
) (storage.Index, error) {
	return s.GetVertexIndex(txnID, getTableInternalIndexName(vertexTableFileID), logger)
}

func (s *StorageEngine) GetEdgeTableInternalIndex(
	txnID common.TxnID,
	edgeTableID common.FileID,
	logger common.ITxnLoggerWithContext,
) (storage.Index, error) {
	return s.GetEdgeIndex(txnID, getTableInternalIndexName(edgeTableID), logger)
}

func (s *StorageEngine) GetDirectoryTableInternalIndex(
	txnID common.TxnID,
	dirTableID common.FileID,
	logger common.ITxnLoggerWithContext,
) (storage.Index, error) {
	return s.GetDirectoryIndex(txnID, getTableInternalIndexName(dirTableID), logger)
}

func (s *StorageEngine) DropVertexTableIndex(
	txnID common.TxnID,
	indexName string,
	logger common.ITxnLoggerWithContext,
) error {
	cToken := s.locker.LockCatalog(txnID, txns.GranularLockExclusive)
	if cToken == nil {
		return errors.New("unable to get system catalog X-lock")
	}
	defer s.locker.Unlock(txnID)

	return s.catalog.DropVertexIndex(indexName)
}

func (s *StorageEngine) DropEdgesTableIndex(
	txnID common.TxnID,
	indexName string,
	logger common.ITxnLoggerWithContext,
) error {
	cToken := s.locker.LockCatalog(txnID, txns.GranularLockExclusive)
	if cToken == nil {
		return errors.New("unable to get system catalog X-lock")
	}
	defer s.locker.Unlock(txnID)

	return s.catalog.DropEdgeIndex(indexName)
}
