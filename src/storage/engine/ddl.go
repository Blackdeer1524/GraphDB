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
	return GetTableFilePath(basePath, getVertexTableName(vertTableName))
}

func GetEdgeTableFilePath(basePath, edgeTableName string) string {
	return GetTableFilePath(basePath, getEdgeTableName(edgeTableName))
}

func GetVertexIndexFilePath(basePath, indexName string) string {
	return getIndexFilePath(basePath, GetVertexIndexName(indexName))
}

func GetEdgeIndexFilePath(basePath, indexName string) string {
	return getIndexFilePath(basePath, GetEdgeIndexName(indexName))
}

func GetTableFilePath(basePath, prefixedName string) string {
	return filepath.Join(basePath, "tables", prefixedName+".tbl")
}

func getIndexFilePath(basePath, prefixedName string) string {
	return filepath.Join(basePath, "indexes", prefixedName+".idx")
}

func getVertexTableName(vertexTableName string) string {
	return "vertex_" + vertexTableName
}

func getEdgeTableName(tableName string) string {
	return "edge_" + tableName
}

func getDirectoryTableName(vertexTableName string) string {
	return "directory_" + vertexTableName
}

func GetVertexIndexName(vertexTableName string) string {
	return "idx_" + getVertexTableName(vertexTableName)
}

func GetEdgeIndexName(tableName string) string {
	return "idx_" + getEdgeTableName(tableName)
}

func getTableInternalIndexName(tableID common.FileID) string {
	return "idx_internal_" + strconv.Itoa(int(tableID))
}

func (s *StorageEngine) createTable(
	txnID common.TxnID,
	fullTableName string,
	tableFileID common.FileID,
	schema storage.Schema,
	_ *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) error {
	needToRollback := true

	basePath := s.catalog.GetBasePath()
	tableFilePath := GetTableFilePath(basePath, fullTableName)

	// Existence of the file is not the proof of existence of the table
	// (we don't remove file on drop),
	// and it is why we do not check if the table exists in file system.
	ok, err := s.catalog.TableExists(fullTableName)
	if err != nil {
		return fmt.Errorf("unable to check if table exists: %w", err)
	}

	if ok {
		return fmt.Errorf("table %s already exists", fullTableName)
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
		return fmt.Errorf("unable to create directory: %w", err)
	}

	err = file.Sync()
	if err != nil {
		return fmt.Errorf("unable to sync file: %w", err)
	}

	_ = file.Close()
	defer func() {
		if needToRollback {
			_ = s.fs.Remove(tableFilePath)
		}
	}()

	// update info in metadata
	tblCreateReq := storage.TableMeta{
		Name:       fullTableName,
		PathToFile: tableFilePath,
		FileID:     tableFileID,
		Schema:     schema,
	}

	err = s.catalog.AddTable(tblCreateReq)
	if err != nil {
		return fmt.Errorf("unable to add table to catalog: %w", err)
	}
	defer func() {
		if needToRollback {
			_ = s.catalog.DropTable(fullTableName)
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

func (s *StorageEngine) CreateVertexTable(
	txnID common.TxnID,
	tableName string,
	schema storage.Schema,
	logger common.ITxnLoggerWithContext,
) error {
	cToken := s.locker.LockCatalog(txnID, txns.GranularLockExclusive)
	if cToken == nil {
		return errors.New("unable to get system catalog X-lock")
	}
	defer s.locker.Unlock(txnID)

	vertexTableName := getVertexTableName(tableName)
	vTableFileID, dirTableFileID := s.catalog.GetNewFileIDPair()
	err := s.createTable(
		txnID,
		vertexTableName,
		vTableFileID,
		schema,
		cToken,
		logger,
	)
	if err != nil {
		return err
	}

	const vIDSize = uint32(unsafe.Sizeof(storage.VertexID{}))
	err = s.createIndex(
		txnID,
		getTableInternalIndexName(vTableFileID),
		vertexTableName,
		[]string{"ID"},
		vIDSize,
		cToken,
		logger,
	)
	if err != nil {
		return fmt.Errorf("unable to create vertex table index: %w", err)
	}

	dirTableName := getDirectoryTableName(tableName)
	err = s.createTable(
		txnID,
		dirTableName,
		dirTableFileID,
		storage.Schema{},
		cToken,
		logger,
	)
	if err != nil {
		return fmt.Errorf("unable to create directory table: %w", err)
	}

	const dirIDSize = uint32(unsafe.Sizeof(storage.DirItemID{}))
	return s.createIndex(
		txnID,
		getTableInternalIndexName(dirTableFileID),
		dirTableName,
		[]string{"ID"},
		dirIDSize,
		cToken,
		logger,
	)
}

func (s *StorageEngine) CreateEdgeTable(
	txnID common.TxnID,
	tableName string,
	schema storage.Schema,
	logger common.ITxnLoggerWithContext,
) error {
	cToken := s.locker.LockCatalog(txnID, txns.GranularLockExclusive)
	if cToken == nil {
		return errors.New("unable to get system catalog X-lock")
	}

	edgeTableName := getEdgeTableName(tableName)
	edgeTableFileID := s.catalog.GetNewFileID()
	err := s.createTable(
		txnID,
		edgeTableName,
		edgeTableFileID,
		schema,
		cToken,
		logger,
	)
	if err != nil {
		return err
	}

	const eIdSize = uint32(unsafe.Sizeof(storage.EdgeID{}))
	return s.createIndex(
		txnID,
		getTableInternalIndexName(edgeTableFileID),
		edgeTableName,
		[]string{"ID"},
		eIdSize,
		cToken,
		logger,
	)
}

func (s *StorageEngine) dropTable(
	txnID common.TxnID,
	name string,
	_ *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) error {
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

func (s *StorageEngine) DropVertexTable(
	txnID common.TxnID,
	vertTableName string,
	logger common.ITxnLoggerWithContext,
) error {
	fullVertTableName := getVertexTableName(vertTableName)

	cToken := s.locker.LockCatalog(txnID, txns.GranularLockExclusive)
	if cToken == nil {
		return errors.New("unable to get system catalog X-lock")
	}
	defer s.locker.Unlock(txnID)

	err := s.dropTable(txnID, fullVertTableName, cToken, logger)
	if err != nil {
		return err
	}

	vertTableMeta, err := s.catalog.GetTableMeta(fullVertTableName)
	if err != nil {
		return fmt.Errorf("unable to get table meta: %w", err)
	}

	err = s.dropIndex(txnID, getTableInternalIndexName(vertTableMeta.FileID), cToken, logger)
	if err != nil {
		return fmt.Errorf("unable to drop system index: %w", err)
	}

	dirTableName := getDirectoryTableName(vertTableName)
	err = s.dropTable(txnID, dirTableName, cToken, logger)
	if err != nil {
		return err
	}
	dirTableID := getDirTableID(vertTableMeta.FileID)
	err = s.dropIndex(txnID, getTableInternalIndexName(dirTableID), cToken, logger)
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
	edgeTableName := getEdgeTableName(name)

	cToken := s.locker.LockCatalog(txnID, txns.GranularLockExclusive)
	if cToken == nil {
		return errors.New("unable to get system catalog X-lock")
	}
	defer s.locker.Unlock(txnID)

	err := s.dropTable(txnID, edgeTableName, cToken, logger)
	if err != nil {
		return err
	}

	edgeTableMeta, err := s.catalog.GetTableMeta(edgeTableName)
	if err != nil {
		return err
	}

	return s.dropIndex(txnID, getTableInternalIndexName(edgeTableMeta.FileID), cToken, logger)
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

func (s *StorageEngine) CreateVertexTableIndex(
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

	return s.createIndex(
		txnID,
		GetVertexIndexName(indexName),
		getVertexTableName(tableName),
		columns,
		keyBytesCnt,
		cToken,
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
	cToken := s.locker.LockCatalog(txnID, txns.GranularLockExclusive)
	if cToken == nil {
		return errors.New("unable to get system catalog X-lock")
	}
	defer s.locker.Unlock(txnID)

	return s.createIndex(
		txnID,
		GetEdgeIndexName(indexName),
		getEdgeTableName(tableName),
		columns,
		keyBytesCnt,
		cToken,
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
	defer s.locker.Unlock(txnID)

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

func getDirTableID(vertTableID common.FileID) common.FileID {
	return vertTableID + 1
}

func (s *StorageEngine) GetDirectoryIndex(
	txnID common.TxnID,
	vertTableID common.FileID,
	logger common.ITxnLoggerWithContext,
) (storage.Index, error) {
	dirTableID := getDirTableID(vertTableID)
	return s.getIndex(txnID, getTableInternalIndexName(dirTableID), logger)
}

func (s *StorageEngine) dropIndex(
	txnID common.TxnID,
	indexName string,
	_ *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) error {
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
	cToken := s.locker.LockCatalog(txnID, txns.GranularLockExclusive)
	if cToken == nil {
		return errors.New("unable to get system catalog X-lock")
	}
	defer s.locker.Unlock(txnID)

	return s.dropIndex(txnID, GetVertexIndexName(indexName), cToken, logger)
}

func (s *StorageEngine) DropEdgesTableIndex(
	txnID common.TxnID,
	tableName string,
	logger common.ITxnLoggerWithContext,
) error {
	cToken := s.locker.LockCatalog(txnID, txns.GranularLockExclusive)
	if cToken == nil {
		return errors.New("unable to get system catalog X-lock")
	}
	defer s.locker.Unlock(txnID)

	return s.dropIndex(txnID, GetEdgeIndexName(tableName), cToken, logger)
}
