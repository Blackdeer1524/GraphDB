package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"unsafe"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

func GetTableFilePath(basePath, name string) string {
	return filepath.Join(basePath, "tables", name+".tbl")
}

func GetIndexFilePath(basePath, name string) string {
	return filepath.Join(basePath, "indexes", name+".idx")
}

func getVertexTableName(name string) string {
	return "vertex_" + name
}

func getEdgeTableName(name string) string {
	return "edge_" + name
}

func getDirectoryTableName(name string) string {
	return "directory_" + name
}

func getVertexIndexName(name string) string {
	return "idx_" + getVertexTableName(name)
}

func getEdgeIndexName(name string) string {
	return "idx_" + getEdgeTableName(name)
}

func getDirectoryIndexName(name string) string {
	return "idx_" + getDirectoryTableName(name)
}

func getVertexTableInternalIndexName(vertexTableName string) string {
	return "idx_internal_" + getVertexTableName(vertexTableName)
}

func getEdgeTableInternalIndexName(edgeTableName string) string {
	return "idx_internal_" + getEdgeTableName(edgeTableName)
}

func getDirectoryTableInternalIndexName(vertexTableName string) string {
	return "idx_internal_" + getDirectoryTableName(vertexTableName)
}

func (s *StorageEngine) createTable(
	txnID common.TxnID,
	name string,
	schema storage.Schema,
	logger common.ITxnLoggerWithContext,
) error {
	needToRollback := true
	if s.locker.LockCatalog(txnID, txns.GranularLockExclusive) == nil {
		return errors.New("unable to get system catalog lock")
	}

	basePath := s.catalog.GetBasePath()
	fileID := s.catalog.GetNewFileID()
	tableFilePath := GetTableFilePath(basePath, name)

	// Existence of the file is not the proof of existence of the table
	// (we don't remove file on drop),
	// and it is why we do not check if the table exists in file system.
	ok, err := s.catalog.TableExists(name)
	if err != nil {
		return fmt.Errorf("unable to check if table exists: %w", err)
	}

	if ok {
		return fmt.Errorf("table %s already exists", name)
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
	tblCreateReq := storage.Table{
		Name:       name,
		PathToFile: tableFilePath,
		FileID:     fileID,
		Schema:     schema,
	}

	err = s.catalog.AddTable(tblCreateReq)
	if err != nil {
		return fmt.Errorf("unable to add table to catalog: %w", err)
	}
	defer func() {
		if needToRollback {
			_ = s.catalog.DropTable(name)
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

func (s *StorageEngine) CreateVertexTable(
	txnID common.TxnID,
	name string,
	schema storage.Schema,
	logger common.ITxnLoggerWithContext,
) error {
	err := s.createTable(txnID, getVertexTableName(name), schema, logger)
	if err != nil {
		return err
	}

	const vIDSize = uint32(unsafe.Sizeof(storage.VertexID{}))
	err = s.createIndex(
		txnID,
		getVertexTableInternalIndexName(name),
		name,
		[]string{"ID"},
		vIDSize,
		logger,
	)

	if err != nil {
		return err
	}
	return s.createDirectoryTable(txnID, name, logger)
}

func (s *StorageEngine) CreateEdgesTable(
	txnID common.TxnID,
	name string,
	schema storage.Schema,
	logger common.ITxnLoggerWithContext,
) error {
	err := s.createTable(txnID, getEdgeTableName(name), schema, logger)
	if err != nil {
		return err
	}

	const eIdSize = uint32(unsafe.Sizeof(storage.EdgeID{}))
	return s.createIndex(
		txnID,
		getEdgeTableInternalIndexName(name),
		name,
		[]string{"ID"},
		eIdSize,
		logger,
	)
}

func (s *StorageEngine) createDirectoryTable(
	txnID common.TxnID,
	vertexTableName string,
	logger common.ITxnLoggerWithContext,
) error {
	err := s.createTable(
		txnID,
		getDirectoryTableName(vertexTableName),
		storage.Schema{},
		logger,
	)

	if err != nil {
		return err
	}

	const dirIDSize = uint32(unsafe.Sizeof(storage.DirItemID{}))
	return s.createIndex(
		txnID,
		getDirectoryTableInternalIndexName(vertexTableName),
		vertexTableName,
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

func (s *StorageEngine) DropVertexTable(
	txnID common.TxnID,
	name string,
	logger common.ITxnLoggerWithContext,
) error {
	err := s.dropTable(txnID, getVertexTableName(name), logger)
	if err != nil {
		return err
	}
	err = s.dropTable(txnID, getDirectoryTableName(name), logger)
	if err != nil {
		return err
	}

	return s.dropIndex(txnID, getDirectoryTableInternalIndexName(name), logger)
}

func (s *StorageEngine) DropEdgesTable(
	txnID common.TxnID,
	name string,
	logger common.ITxnLoggerWithContext,
) error {
	err := s.dropTable(txnID, getEdgeTableName(name), logger)
	if err != nil {
		return err
	}

	return s.dropIndex(txnID, getEdgeTableInternalIndexName(name), logger)
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
	tableFilePath := GetIndexFilePath(basePath, name)

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

	idxCreateReq := storage.Index{
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
		indexName,
		getVertexTableName(tableName),
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
		indexName,
		getEdgeTableName(tableName),
		columns,
		keyBytesCnt,
		logger,
	)
}

func (s *StorageEngine) getIndex(
	txnID common.TxnID,
	indexName string,
	logger common.ITxnLoggerWithContext,
) (common.Index, error) {
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
	return s.dropIndex(txnID, getVertexIndexName(indexName), logger)
}

func (s *StorageEngine) DropEdgesTableIndex(
	txnID common.TxnID,
	name string,
	logger common.ITxnLoggerWithContext,
) error {
	return s.dropIndex(txnID, getEdgeIndexName(name), logger)
}
