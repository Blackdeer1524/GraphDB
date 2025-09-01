package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"unsafe"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

func GetVertexTableFilePath(basePath, vertTableName string) string {
	return GetTableFilePath(basePath, formVertexTableName(vertTableName))
}

func GetEdgeTableFilePath(basePath, edgeTableName string) string {
	return GetTableFilePath(basePath, formEdgeTableName(edgeTableName))
}

func GetVertexIndexFilePath(basePath, indexName string) string {
	return getIndexFilePath(basePath, FormVertexIndexName(indexName))
}

func GetEdgeIndexFilePath(basePath, indexName string) string {
	return getIndexFilePath(basePath, FormEdgeIndexName(indexName))
}

func GetTableFilePath(basePath, prefixedName string) string {
	return filepath.Join(basePath, "tables", prefixedName+".tbl")
}

func getIndexFilePath(basePath, prefixedName string) string {
	return filepath.Join(basePath, "indexes", prefixedName+".idx")
}

func formVertexTableName(vertexName string) string {
	return "vertex_" + vertexName
}

func formEdgeTableName(tableName string) string {
	return "edge_" + tableName
}

func formDirectoryTableName(vertexTableName string) string {
	return "directory_" + vertexTableName
}

func FormVertexIndexName(vertexTableName string) string {
	return "idx_" + formVertexTableName(vertexTableName)
}

func FormEdgeIndexName(tableName string) string {
	return "idx_" + formEdgeTableName(tableName)
}

func getDirIndexName(vertTableID common.FileID) string {
	return "idx_internal_dir_" + strconv.Itoa(int(vertTableID))
}

func getTableInternalIndexName(tableID common.FileID) string {
	return "idx_internal_" + strconv.Itoa(int(tableID))
}

func (s *StorageEngine) createTable(
	txnID common.TxnID,
	name string,
	schema storage.Schema,
	logger common.ITxnLoggerWithContext,
) (common.FileID, error) {
	needToRollback := true
	if s.locker.LockCatalog(txnID, txns.GranularLockExclusive) == nil {
		return 0, errors.New("unable to get system catalog lock")
	}

	basePath := s.catalog.GetBasePath()
	fileID := s.catalog.GetNewFileID()
	tableFilePath := GetTableFilePath(basePath, name)

	// Existence of the file is not the proof of existence of the table
	// (we don't remove file on drop),
	// and it is why we do not check if the table exists in file system.
	ok, err := s.catalog.TableExists(name)
	if err != nil {
		return 0, fmt.Errorf("unable to check if table exists: %w", err)
	}

	if ok {
		return 0, fmt.Errorf("table %s already exists", name)
	}

	fileExists := true

	_, err = s.fs.Stat(tableFilePath)
	if err != nil {
		fileExists = false

		if !os.IsNotExist(err) {
			return 0, fmt.Errorf("unable to check if file exists: %w", err)
		}
	}

	if fileExists {
		err = s.fs.Remove(tableFilePath)
		if err != nil {
			return 0, fmt.Errorf("unable to remove file: %w", err)
		}
	}

	dir := filepath.Dir(tableFilePath)

	err = s.fs.MkdirAll(dir, 0o755)
	if err != nil {
		return 0, fmt.Errorf("unable to create directory %s: %w", dir, err)
	}

	file, err := s.fs.Create(tableFilePath)
	if err != nil {
		return 0, fmt.Errorf("unable to create directory: %w", err)
	}

	err = file.Sync()
	if err != nil {
		return 0, fmt.Errorf("unable to sync file: %w", err)
	}

	_ = file.Close()
	defer func() {
		if needToRollback {
			_ = s.fs.Remove(tableFilePath)
		}
	}()

	// update info in metadata
	tblCreateReq := storage.TableMeta{
		Name:       name,
		PathToFile: tableFilePath,
		FileID:     fileID,
		Schema:     schema,
	}

	err = s.catalog.AddTable(tblCreateReq)
	if err != nil {
		return 0, fmt.Errorf("unable to add table to catalog: %w", err)
	}
	defer func() {
		if needToRollback {
			_ = s.catalog.DropTable(name)
		}
	}()

	err = s.catalog.Save(logger)
	if err != nil {
		return 0, fmt.Errorf("unable to save catalog: %w", err)
	}

	s.diskMgr.InsertToFileMap(common.FileID(fileID), tableFilePath)
	needToRollback = false
	return fileID, nil
}

func (s *StorageEngine) CreateVertexTable(
	txnID common.TxnID,
	name string,
	schema storage.Schema,
	logger common.ITxnLoggerWithContext,
) error {
	vTableFileID, err := s.createTable(txnID, formVertexTableName(name), schema, logger)
	if err != nil {
		return err
	}

	const vIDSize = uint32(unsafe.Sizeof(storage.VertexID{}))
	err = s.createIndex(
		txnID,
		getTableInternalIndexName(vTableFileID),
		name,
		[]string{"ID"},
		vIDSize,
		logger,
	)

	if err != nil {
		return err
	}
	return s.createDirectoryTable(txnID, name, vTableFileID, logger)
}

func (s *StorageEngine) CreateEdgeTable(
	txnID common.TxnID,
	name string,
	schema storage.Schema,
	logger common.ITxnLoggerWithContext,
) error {
	fileID, err := s.createTable(txnID, formEdgeTableName(name), schema, logger)
	if err != nil {
		return err
	}

	const eIdSize = uint32(unsafe.Sizeof(storage.EdgeID{}))
	return s.createIndex(
		txnID,
		getTableInternalIndexName(fileID),
		name,
		[]string{"ID"},
		eIdSize,
		logger,
	)
}

func (s *StorageEngine) createDirectoryTable(
	txnID common.TxnID,
	vertTableName string,
	vertTableFileID common.FileID,
	logger common.ITxnLoggerWithContext,
) error {
	_, err := s.createTable(
		txnID,
		formDirectoryTableName(vertTableName),
		storage.Schema{},
		logger,
	)

	if err != nil {
		return err
	}

	const dirIDSize = uint32(unsafe.Sizeof(storage.DirItemID{}))
	return s.createIndex(
		txnID,
		getDirIndexName(vertTableFileID),
		vertTableName,
		[]string{"ID"},
		dirIDSize,
		logger,
	)
}

func (s *StorageEngine) dropTable(
	txnID common.TxnID,
	name string,
	logger common.ITxnLoggerWithContext,
) error {
	ctoken := s.locker.LockCatalog(txnID, txns.GranularLockExclusive)
	if ctoken == nil {
		return errors.New("unable to get system catalog lock")
	}

	// do not delete file, just remove from catalog
	err := s.catalog.DropTable(name)
	if err != nil {
		return fmt.Errorf("unable to drop vertex table: %w", err)
	}

	err = s.catalog.Save(logger)
	if err != nil {
		return fmt.Errorf("unable to save catalog: %w", err)
	}
	return nil
}

func (s *StorageEngine) dropDirectoryTable(
	txnID common.TxnID,
	vertTableName string,
	vertTableFileID common.FileID,
	logger common.ITxnLoggerWithContext,
) error {
	dirTableName := formDirectoryTableName(vertTableName)
	err := s.dropTable(txnID, dirTableName, logger)
	if err != nil {
		return err
	}
	err = s.dropIndex(txnID, getDirIndexName(vertTableFileID), logger)
	if err != nil {
		return err
	}
	return nil
}

func (s *StorageEngine) DropVertexTable(
	txnID common.TxnID,
	vertTableName string,
	logger common.ITxnLoggerWithContext,
) error {
	fullVertTableName := formVertexTableName(vertTableName)
	err := s.dropTable(txnID, fullVertTableName, logger)
	if err != nil {
		return err
	}

	vertTableMeta, err := s.catalog.GetTableMeta(fullVertTableName)
	if err != nil {
		return fmt.Errorf("unable to get table meta: %w", err)
	}

	err = s.dropIndex(txnID, getTableInternalIndexName(vertTableMeta.FileID), logger)
	if err != nil {
		return fmt.Errorf("unable to drop system index: %w", err)
	}

	err = s.dropDirectoryTable(txnID, vertTableName, vertTableMeta.FileID, logger)
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
	edgeTableName := formEdgeTableName(name)
	err := s.dropTable(txnID, edgeTableName, logger)
	if err != nil {
		return err
	}

	edgeTableMeta, err := s.catalog.GetTableMeta(edgeTableName)
	if err != nil {
		return err
	}

	return s.dropIndex(txnID, getTableInternalIndexName(edgeTableMeta.FileID), logger)
}

func (s *StorageEngine) createIndex(
	txnID common.TxnID,
	name string,
	tableName string,
	columns []string,
	keyBytesCnt uint32,
	logger common.ITxnLoggerWithContext,
) error {
	needToRollback := true
	ctoken := s.locker.LockCatalog(txnID, txns.GranularLockExclusive)
	if ctoken == nil {
		return errors.New("unable to get system catalog lock")
	}

	basePath := s.catalog.GetBasePath()
	fileID := s.catalog.GetNewFileID()
	tableFilePath := getIndexFilePath(basePath, name)

	// Existence of the file is not the proof of existence of the index (we don't remove file on
	// drop),
	// and it is why we do not check if the table exists in file system.
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

func (s *StorageEngine) CreateVertexTableIndex(
	txnID common.TxnID,
	indexName string,
	tableName string,
	columns []string,
	keyBytesCnt uint32,
	logger common.ITxnLoggerWithContext,
) error {
	return s.createIndex(
		txnID,
		FormVertexIndexName(indexName),
		formVertexTableName(tableName),
		columns,
		keyBytesCnt,
		logger,
	)
}

func (s *StorageEngine) CreateEdgesTableIndex(
	txnID common.TxnID,
	indexName string,
	tableName string,
	columns []string,
	keyBytesCnt uint32,
	logger common.ITxnLoggerWithContext,
) error {
	return s.createIndex(
		txnID,
		FormEdgeIndexName(indexName),
		formEdgeTableName(tableName),
		columns,
		keyBytesCnt,
		logger,
	)
}

func (s *StorageEngine) getIndex(
	txnID common.TxnID,
	indexName string,
	logger common.ITxnLoggerWithContext,
) (storage.Index, error) {
	ctoken := s.locker.LockCatalog(txnID, txns.GranularLockShared)
	if ctoken == nil {
		return nil, errors.New("unable to get system catalog lock")
	}

	ok, err := s.catalog.IndexExists(indexName)
	if err != nil {
		return nil, fmt.Errorf("unable to check if index exists: %w", err)
	}

	if !ok {
		return nil, fmt.Errorf("index %s does not exist", indexName)
	}

	indexMeta, err := s.catalog.GetIndexMeta(indexName)
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
	return s.getIndex(txnID, getTableInternalIndexName(vertexTableFileID), logger)
}

func (s *StorageEngine) GetEdgeTableInternalIndex(
	txnID common.TxnID,
	edgeTableID common.FileID,
	logger common.ITxnLoggerWithContext,
) (storage.Index, error) {
	return s.getIndex(txnID, getTableInternalIndexName(edgeTableID), logger)
}

func (s *StorageEngine) GetDirectoryIndex(
	txnID common.TxnID,
	vertTableID common.FileID,
	logger common.ITxnLoggerWithContext,
) (storage.Index, error) {
	return s.getIndex(txnID, getDirIndexName(vertTableID), logger)
}

func (s *StorageEngine) dropIndex(
	txnID common.TxnID,
	indexName string,
	logger common.ITxnLoggerWithContext,
) error {
	ctoken := s.locker.LockCatalog(txnID, txns.GranularLockExclusive)
	if ctoken == nil {
		return errors.New("unable to get system catalog lock")
	}

	// do not delete file, just remove from catalog

	err := s.catalog.DropIndex(indexName)
	if err != nil {
		return fmt.Errorf("unable to drop vertex table: %w", err)
	}

	err = s.catalog.Save(logger)
	if err != nil {
		return fmt.Errorf("unable to save catalog: %w", err)
	}

	return nil
}

func (s *StorageEngine) DropVertexTableIndex(
	txnID common.TxnID,
	indexName string,
	logger common.ITxnLoggerWithContext,
) error {
	return s.dropIndex(txnID, FormVertexIndexName(indexName), logger)
}

func (s *StorageEngine) DropEdgesTableIndex(
	txnID common.TxnID,
	name string,
	logger common.ITxnLoggerWithContext,
) error {
	return s.dropIndex(txnID, FormEdgeIndexName(name), logger)
}
