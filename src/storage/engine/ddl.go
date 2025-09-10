package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/spf13/afero"

	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
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

func getVertexTableFilePath(basePath, vertTableName string) string {
	return getTableFilePath(basePath, vertexTableType, vertTableName)
}

func getEdgeTableFilePath(basePath, edgeTableName string) string {
	return getTableFilePath(basePath, edgeTableType, edgeTableName)
}

func getDirectoryTableFilePath(basePath string, vertexTableFileID common.FileID) string {
	dirTableName := systemcatalog.GetDirTableName(vertexTableFileID)
	return getTableFilePath(basePath, directoryTableType, dirTableName)
}

func getVertexTableIndexFilePath(basePath, indexName string) string {
	return getIndexFilePath(basePath, vertexTableType, indexName)
}

func getEdgeTableIndexFilePath(basePath, indexName string) string {
	return getIndexFilePath(basePath, edgeTableType, indexName)
}

func getDirTableIndexFilePath(basePath, indexName string) string {
	return getIndexFilePath(basePath, directoryTableType, indexName)
}

func getTableFilePath(basePath string, tableType tableType, prefixedName string) string {
	return filepath.Join(basePath, "tables", string(tableType), prefixedName+".tbl")
}

func getIndexFilePath(basePath string, indexType tableType, prefixedName string) string {
	return filepath.Join(basePath, "indexes", string(indexType), prefixedName+".idx")
}

func getTableSystemIndexName(tableID common.FileID) string {
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
	tableFilePath := getVertexTableFilePath(basePath, tableName)
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

	err = s.createSystemVertexTableIndex(txnID, tableName, tableFileID, cToken, logger)
	if err != nil {
		return fmt.Errorf("unable to create internal vertex index: %w", err)
	}

	err = s.createDirTable(txnID, tableFileID, cToken, logger)
	if err != nil {
		return fmt.Errorf("unable to create directory table: %w", err)
	}

	s.diskMgrInsertToFileMap(common.FileID(tableFileID), tableFilePath)
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
	tableFilePath := getDirectoryTableFilePath(basePath, vertexTableFileID)
	dirTableFileID := s.catalog.GetNewFileID()

	// Existence of the file is not the proof of existence of the table
	// (we don't remove file on drop),
	// and it is why we do not check if the table exists in file system.
	ok, err := s.catalog.DirTableExists(vertexTableFileID)
	if err != nil {
		return fmt.Errorf("unable to check if table exists: %w", err)
	}
	if ok {
		return fmt.Errorf("table %v already exists", vertexTableFileID)
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

	err = s.createSystemDirTableIndex(txnID, vertexTableFileID, dirTableFileID, cToken, logger)
	if err != nil {
		return fmt.Errorf("unable to create internal directory index: %w", err)
	}

	s.diskMgrInsertToFileMap(common.FileID(dirTableFileID), tableFilePath)
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
	tableFilePath := getEdgeTableFilePath(basePath, tableName)
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

	err = s.createSystemEdgeTableIndex(txnID, tableName, tableFileID, cToken, logger)
	if err != nil {
		return fmt.Errorf("unable to create internal edge index: %w", err)
	}

	s.diskMgrInsertToFileMap(common.FileID(tableFileID), tableFilePath)
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

	err = s.catalog.DropVertexIndex(getTableSystemIndexName(vertTableMeta.FileID))
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

	err = s.catalog.DropEdgeIndex(getTableSystemIndexName(edgeTableMeta.FileID))
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

	dirTableMeta, err := s.catalog.GetDirTableMeta(vertexTableFileID)
	if err != nil {
		return err
	}

	err = s.catalog.DropDirIndex(getTableSystemIndexName(dirTableMeta.FileID))
	if err != nil {
		return err
	}

	err = s.catalog.CommitChanges(logger)
	if err != nil {
		return fmt.Errorf("unable to save catalog: %w", err)
	}
	return nil
}

func (s *StorageEngine) CreateVertexTableIndex(
	txnID common.TxnID,
	indexName string,
	tableName string,
	columns []string,
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
	indexFilePath := getVertexTableIndexFilePath(basePath, indexName)
	ok, err := s.catalog.VertexIndexExists(indexName)
	if err != nil {
		return fmt.Errorf("unable to check if index exists: %w", err)
	}
	if ok {
		return fmt.Errorf("index %s already exists", indexName)
	}

	vertexTableMeta, err := s.catalog.GetVertexTableMeta(tableName)
	if err != nil {
		return err
	}

	schemaMap := make(map[string]storage.ColumnType)
	for _, col := range vertexTableMeta.Schema {
		if _, ok := schemaMap[col.Name]; ok {
			assert.Assert(false, "duplicate column name: %s", col.Name)
		}
		schemaMap[col.Name] = col.Type
	}

	calculatedKeyBytesCnt := uint32(0)
	for _, colName := range columns {
		calculatedKeyBytesCnt += uint32(schemaMap[colName].Size())
	}

	err = prepareFSforTable(s.fs, indexFilePath)
	if err != nil {
		return fmt.Errorf("unable to prepare file system for table: %w", err)
	}

	idxCreateReq := storage.IndexMeta{
		Name:        indexName,
		PathToFile:  indexFilePath,
		FileID:      fileID,
		TableName:   tableName,
		Columns:     columns,
		KeyBytesCnt: calculatedKeyBytesCnt,
	}
	err = s.catalog.AddVertexIndex(idxCreateReq)
	if err != nil {
		return fmt.Errorf("unable to add index to catalog: %w", err)
	}

	err = s.catalog.CommitChanges(logger)
	if err != nil {
		return fmt.Errorf("unable to save catalog: %w", err)
	}
	s.diskMgrInsertToFileMap(common.FileID(fileID), indexFilePath)

	err = s.buildVertexIndex(txnID, vertexTableMeta.FileID, idxCreateReq, cToken, logger)
	if err != nil {
		return fmt.Errorf("unable to build index: %w", err)
	}

	return nil
}

func (s *StorageEngine) createSystemVertexTableIndex(
	txnID common.TxnID,
	tableName string,
	vertexTableFileID common.FileID,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) error {
	columns := []string{"ID"}
	return s.CreateVertexTableIndex(
		txnID,
		getTableSystemIndexName(vertexTableFileID),
		tableName,
		columns,
		cToken,
		logger,
	)
}

func (s *StorageEngine) CreateEdgeTableIndex(
	txnID common.TxnID,
	indexName string,
	tableName string,
	columns []string,
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
	indexFileID := s.catalog.GetNewFileID()
	indexFilePath := getEdgeTableIndexFilePath(basePath, indexName)
	ok, err := s.catalog.EdgeIndexExists(indexName)
	if err != nil {
		return fmt.Errorf("unable to check if index exists: %w", err)
	}
	if ok {
		return fmt.Errorf("index %s already exists", indexName)
	}

	edgeTableMeta, err := s.catalog.GetEdgeTableMeta(tableName)
	if err != nil {
		return err
	}

	schemaMap := make(map[string]storage.ColumnType)
	for _, col := range edgeTableMeta.Schema {
		if _, ok := schemaMap[col.Name]; ok {
			assert.Assert(false, "duplicate column name: %s", col.Name)
		}
		schemaMap[col.Name] = col.Type
	}

	keyBytesCnt := uint32(0)
	for _, colName := range columns {
		keyBytesCnt += uint32(schemaMap[colName].Size())
	}

	err = prepareFSforTable(s.fs, indexFilePath)
	if err != nil {
		return fmt.Errorf("unable to prepare file system for table: %w", err)
	}

	idxCreateReq := storage.IndexMeta{
		Name:        indexName,
		PathToFile:  indexFilePath,
		FileID:      indexFileID,
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
	s.diskMgrInsertToFileMap(common.FileID(indexFileID), indexFilePath)

	err = s.buildEdgeIndex(txnID, edgeTableMeta.FileID, idxCreateReq, cToken, logger)
	if err != nil {
		return fmt.Errorf("unable to build index: %w", err)
	}

	return nil
}

func (s *StorageEngine) buildEdgeIndex(
	txnID common.TxnID,
	edgeTableFileID common.FileID,
	indexMeta storage.IndexMeta,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) error {
	edgeTableToken := txns.NewNilFileLockToken(cToken, edgeTableFileID)
	edgesIter, err := s.GetAllEdges(txnID, edgeTableToken)
	if err != nil {
		return fmt.Errorf("unable to get all edges: %w", err)
	}
	defer edgesIter.Close()

	index, err := s.loadIndex(indexMeta, s.pool, s.locker, logger)
	if err != nil {
		return fmt.Errorf("unable to load index: %w", err)
	}
	defer index.Close()

	for ridEdgeErr := range edgesIter.Seq() {
		edgeRID, edge, err := ridEdgeErr.Destruct()
		if err != nil {
			return fmt.Errorf("unable to destruct edge: %w", err)
		}

		key, err := extractEdgeColumns(edge, indexMeta.Columns)
		if err != nil {
			return fmt.Errorf("unable to extract edge columns: %w", err)
		}

		assert.Assert(
			len(key) == int(indexMeta.KeyBytesCnt),
			"extracted edge columns length doesn't match index key bytes count",
		)

		if err := index.Insert(key, edgeRID); err != nil {
			return fmt.Errorf("unable to insert edge record: %w", err)
		}
	}

	return nil
}

func (s *StorageEngine) buildVertexIndex(
	txnID common.TxnID,
	vertexTableFileID common.FileID,
	indexMeta storage.IndexMeta,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) error {
	vertexTableToken := txns.NewNilFileLockToken(cToken, vertexTableFileID)
	verticesIter, err := s.GetAllVertices(txnID, vertexTableToken)
	if err != nil {
		return fmt.Errorf("unable to get all vertices: %w", err)
	}
	defer verticesIter.Close()

	index, err := s.loadIndex(indexMeta, s.pool, s.locker, logger)
	if err != nil {
		return fmt.Errorf("unable to load index: %w", err)
	}
	defer index.Close()

	for ridVertexErr := range verticesIter.Seq() {
		vertexRID, vertex, err := ridVertexErr.Destruct()
		if err != nil {
			return fmt.Errorf("unable to destruct vertex: %w", err)
		}

		key, err := extractVertexColumns(vertex, indexMeta.Columns)
		if err != nil {
			return fmt.Errorf("unable to extract vertex columns: %w", err)
		}

		assert.Assert(
			len(key) == int(indexMeta.KeyBytesCnt),
			"extracted vertex columns length doesn't match index key bytes count",
		)

		if err := index.Insert(key, vertexRID); err != nil {
			return fmt.Errorf("unable to insert vertex record: %w", err)
		}
	}

	return nil
}

func (s *StorageEngine) createSystemEdgeTableIndex(
	txnID common.TxnID,
	tableName string,
	edgeTableFileID common.FileID,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) error {
	columns := []string{"ID"}
	return s.CreateEdgeTableIndex(
		txnID,
		getTableSystemIndexName(edgeTableFileID),
		tableName,
		columns,
		cToken,
		logger,
	)
}

func (s *StorageEngine) GetVertexTableSystemIndex(
	txnID common.TxnID,
	vertexTableFileID common.FileID,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) (storage.Index, error) {
	return s.GetVertexTableIndex(
		txnID,
		getTableSystemIndexName(vertexTableFileID),
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

	indexMeta, err := s.catalog.GetVertexTableIndexMeta(indexName)
	if err != nil {
		return nil, fmt.Errorf("unable to get index meta: %w", err)
	}

	return s.loadIndex(indexMeta, s.pool, s.locker, logger)
}

func (s *StorageEngine) GetEdgeTableSystemIndex(
	txnID common.TxnID,
	edgeTableFileID common.FileID,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) (storage.Index, error) {
	return s.GetEdgeTableIndex(txnID, getTableSystemIndexName(edgeTableFileID), cToken, logger)
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

	return s.loadIndex(indexMeta, s.pool, s.locker, logger)
}

func (s *StorageEngine) GetDirTableSystemIndex(
	txnID common.TxnID,
	dirTableFileID common.FileID,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) (storage.Index, error) {
	return s.getDirTableIndex(txnID, getTableSystemIndexName(dirTableFileID), cToken, logger)
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
	indexFileID := s.catalog.GetNewFileID()
	indexFilePath := getDirTableIndexFilePath(basePath, indexName)
	ok, err := s.catalog.DirIndexExists(indexName)
	if err != nil {
		return fmt.Errorf("unable to check if index exists: %w", err)
	}
	if ok {
		return fmt.Errorf("index %s already exists", indexName)
	}

	// Directory tables have a fixed schema - only ID column
	calculatedKeyBytesCnt := uint32(0)
	for _, colName := range columns {
		if colName == "ID" {
			calculatedKeyBytesCnt += uint32(storage.DirItemSystemIDSize)
		} else {
			return fmt.Errorf("directory table only supports ID column, got: %s", colName)
		}
	}

	err = prepareFSforTable(s.fs, indexFilePath)
	if err != nil {
		return fmt.Errorf("unable to prepare file system for table: %w", err)
	}

	idxCreateReq := storage.IndexMeta{
		Name:        indexName,
		PathToFile:  indexFilePath,
		FileID:      indexFileID,
		TableName:   systemcatalog.GetDirTableName(vertexTableFileID),
		Columns:     columns,
		KeyBytesCnt: calculatedKeyBytesCnt,
	}
	err = s.catalog.AddDirIndex(idxCreateReq)
	if err != nil {
		return fmt.Errorf("unable to add index to catalog: %w", err)
	}

	err = s.catalog.CommitChanges(logger)
	if err != nil {
		return fmt.Errorf("unable to save catalog: %w", err)
	}
	s.diskMgrInsertToFileMap(common.FileID(indexFileID), indexFilePath)

	err = s.buildDirIndex(txnID, vertexTableFileID, idxCreateReq, cToken, logger)
	if err != nil {
		return fmt.Errorf("unable to build index: %w", err)
	}

	return nil
}

func (s *StorageEngine) buildDirIndex(
	txnID common.TxnID,
	dirTableFileID common.FileID,
	indexMeta storage.IndexMeta,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) error {
	dirTableToken := txns.NewNilFileLockToken(cToken, dirTableFileID)
	dirItemsIter, err := s.GetAllDirItems(txnID, dirTableToken)
	if err != nil {
		return fmt.Errorf("unable to get all directory items: %w", err)
	}
	defer dirItemsIter.Close()

	index, err := s.loadIndex(indexMeta, s.pool, s.locker, logger)
	if err != nil {
		return fmt.Errorf("unable to load index: %w", err)
	}
	defer index.Close()

	for ridDirItemErr := range dirItemsIter.Seq() {
		dirItemRID, dirItem, err := ridDirItemErr.Destruct()
		if err != nil {
			return fmt.Errorf("unable to destruct directory item: %w", err)
		}

		key, err := extractDirectoryColumns(dirItem, indexMeta.Columns)
		if err != nil {
			return fmt.Errorf("unable to extract directory columns: %w", err)
		}

		assert.Assert(
			len(key) == int(indexMeta.KeyBytesCnt),
			"extracted directory columns length doesn't match index key bytes count",
		)

		if err := index.Insert(key, dirItemRID); err != nil {
			return fmt.Errorf("unable to insert directory record: %w", err)
		}
	}

	return nil
}

func (s *StorageEngine) createSystemDirTableIndex(
	txnID common.TxnID,
	vertexTableFileID common.FileID,
	directoryTableFileID common.FileID,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) error {
	columns := []string{"ID"}
	keyBytesCnt := storage.DirItemSystemIDSize
	return s.createDirTableIndex(
		txnID,
		getTableSystemIndexName(directoryTableFileID),
		vertexTableFileID,
		columns,
		uint32(keyBytesCnt),
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

	return s.loadIndex(indexMeta, s.pool, s.locker, logger)
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

func (s *StorageEngine) DropEdgeTableIndex(
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
