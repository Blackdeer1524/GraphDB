package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/spf13/afero"

	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
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

func getVertexTableFilePath(basePath string, vertTableFileID common.FileID) string {
	return utils.GetFilePath(basePath, vertTableFileID)
}

func getEdgeTableFilePath(basePath string, edgeTableFileID common.FileID) string {
	return utils.GetFilePath(basePath, edgeTableFileID)
}

func getDirTableFilePath(basePath string, dirTableFileID common.FileID) string {
	return utils.GetFilePath(basePath, dirTableFileID)
}

func getVertexTableIndexFilePath(basePath string, indexFileID common.FileID) string {
	return utils.GetFilePath(basePath, indexFileID)
}

func getEdgeTableIndexFilePath(basePath string, indexFileID common.FileID) string {
	return utils.GetFilePath(basePath, indexFileID)
}

func getDirTableIndexFilePath(basePath string, indexFileID common.FileID) string {
	return utils.GetFilePath(basePath, indexFileID)
}

func getTableSystemIndexName(tableID common.FileID) string {
	return "idx_internal_" + strconv.Itoa(int(tableID))
}

func prepareFSforTable(fs afero.Fs, tableFilePath string) error {
	dir := filepath.Dir(tableFilePath)

	err := fs.MkdirAll(dir, 0o755)
	if err != nil {
		return fmt.Errorf("unable to create directory %s: %w", dir, err)
	}

	file, err := fs.OpenFile(tableFilePath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("unable to create directory: %w", err)
	}

	err = file.Sync()
	if err != nil {
		return fmt.Errorf("unable to sync file: %w", err)
	}

	return file.Close()
}

func (s *StorageEngine) CreateVertexTable(
	txnID common.TxnID,
	tableName string,
	schema storage.Schema,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) error {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockExclusive) {
		return fmt.Errorf("unable to upgrade catalog lock: %w", txns.ErrDeadlockPrevention)
	}

	err := s.catalog.Load()
	if err != nil {
		return fmt.Errorf("unable to load catalog: %w", err)
	}

	basePath := s.catalog.GetBasePath()
	tableFileID := s.catalog.GetNewFileID()
	tableFilePath := getVertexTableFilePath(basePath, tableFileID)

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
		Name:   tableName,
		FileID: tableFileID,
		Schema: schema,
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

	return nil
}

func (s *StorageEngine) createDirTable(
	txnID common.TxnID,
	vertexTableFileID common.FileID,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) error {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockExclusive) {
		return fmt.Errorf("unable to upgrade catalog lock: %w", txns.ErrDeadlockPrevention)
	}

	err := s.catalog.Load()
	if err != nil {
		return fmt.Errorf("unable to load catalog: %w", err)
	}

	dirTableFileID := s.catalog.GetNewFileID()
	basePath := s.catalog.GetBasePath()
	tableFilePath := getDirTableFilePath(basePath, dirTableFileID)

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
		return fmt.Errorf("unable to upgrade catalog lock: %w", txns.ErrDeadlockPrevention)
	}

	err := s.catalog.Load()
	if err != nil {
		return fmt.Errorf("unable to load catalog: %w", err)
	}

	basePath := s.catalog.GetBasePath()
	tableFileID := s.catalog.GetNewFileID()
	tableFilePath := getEdgeTableFilePath(basePath, tableFileID)

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
	return nil
}

func (s *StorageEngine) DropVertexTable(
	txnID common.TxnID,
	vertTableName string,
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) error {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockExclusive) {
		return fmt.Errorf("unable to upgrade catalog lock: %w", txns.ErrDeadlockPrevention)
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
		return fmt.Errorf("unable to upgrade catalog lock: %w", txns.ErrDeadlockPrevention)
	}

	err := s.catalog.Load()
	if err != nil {
		return fmt.Errorf("unable to load catalog: %w", err)
	}

	edgeTableMeta, err := s.catalog.GetEdgeTableMeta(name)
	if err != nil {
		return err
	}

	err = s.catalog.DropEdgeTable(name)
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
		return fmt.Errorf("unable to upgrade catalog lock: %w", txns.ErrDeadlockPrevention)
	}

	err := s.catalog.Load()
	if err != nil {
		return fmt.Errorf("unable to load catalog: %w", err)
	}

	dirTableMeta, err := s.catalog.GetDirTableMeta(vertexTableFileID)
	if err != nil {
		return err
	}

	err = s.catalog.DropDirTable(vertexTableFileID)
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
		return fmt.Errorf("unable to upgrade catalog lock: %w", txns.ErrDeadlockPrevention)
	}

	err := s.catalog.Load()
	if err != nil {
		return fmt.Errorf("unable to load catalog: %w", err)
	}

	basePath := s.catalog.GetBasePath()
	indexFileID := s.catalog.GetNewFileID()
	indexFilePath := getVertexTableIndexFilePath(basePath, indexFileID)
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

	schemaMap := storage.GetVertexSystemSchemaMap()
	for _, col := range vertexTableMeta.Schema {
		if _, ok := schemaMap[col.Name]; ok {
			err := fmt.Errorf("duplicate column name: %s", col.Name)
			return err
		}
		schemaMap[col.Name] = col.Type
	}

	calculatedKeyBytesCnt := uint32(0)
	for _, colName := range columns {
		colInfo, ok := schemaMap[colName]
		if !ok {
			return fmt.Errorf("column %s not found in table %s", colName, tableName)
		}
		calculatedKeyBytesCnt += uint32(colInfo.Size())
	}

	err = prepareFSforTable(s.fs, indexFilePath)
	if err != nil {
		return fmt.Errorf("unable to prepare file system for table: %w", err)
	}

	idxCreateReq := storage.IndexMeta{
		Name:        indexName,
		FileID:      indexFileID,
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
		return fmt.Errorf("unable to upgrade catalog lock: %w", txns.ErrDeadlockPrevention)
	}

	err := s.catalog.Load()
	if err != nil {
		return fmt.Errorf("unable to load catalog: %w", err)
	}

	basePath := s.catalog.GetBasePath()
	indexFileID := s.catalog.GetNewFileID()
	indexFilePath := getEdgeTableIndexFilePath(basePath, indexFileID)
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

	schemaMap := storage.GetEdgeSystemSchemaMap()
	for _, col := range edgeTableMeta.Schema {
		if _, ok := schemaMap[col.Name]; ok {
			err := fmt.Errorf("duplicate column name: %s", col.Name)
			return err
		}
		schemaMap[col.Name] = col.Type
	}

	keyBytesCnt := uint32(0)
	for _, colName := range columns {
		colInfo, ok := schemaMap[colName]
		if !ok {
			return fmt.Errorf("column %s not found in table %s", colName, tableName)
		}
		keyBytesCnt += uint32(colInfo.Size())
	}

	err = prepareFSforTable(s.fs, indexFilePath)
	if err != nil {
		return fmt.Errorf("unable to prepare file system for table: %w", err)
	}

	idxCreateReq := storage.IndexMeta{
		Name:        indexName,
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
		return nil, fmt.Errorf("unable to upgrade catalog lock: %w", txns.ErrDeadlockPrevention)
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
		return nil, fmt.Errorf("unable to upgrade catalog lock: %w", txns.ErrDeadlockPrevention)
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
	cToken *txns.CatalogLockToken,
	logger common.ITxnLoggerWithContext,
) error {
	if !s.locker.UpgradeCatalogLock(cToken, txns.GranularLockExclusive) {
		return fmt.Errorf("unable to get system catalog X-lock: %w", txns.ErrDeadlockPrevention)
	}

	err := s.catalog.Load()
	if err != nil {
		return fmt.Errorf("unable to load catalog: %w", err)
	}

	basePath := s.catalog.GetBasePath()
	indexFileID := s.catalog.GetNewFileID()
	indexFilePath := getDirTableIndexFilePath(basePath, indexFileID)
	ok, err := s.catalog.DirIndexExists(indexName)
	if err != nil {
		return fmt.Errorf("unable to check if index exists: %w", err)
	}
	if ok {
		return fmt.Errorf("index %s already exists", indexName)
	}

	schemaMap := storage.GetDirectoryItemSchemaMap()
	// Directory tables have a fixed schema - only ID column
	calculatedKeyBytesCnt := uint32(0)
	for _, colName := range columns {
		colInfo, ok := schemaMap[colName]
		if !ok {
			return fmt.Errorf(
				"column %s not found in directory table %s",
				colName,
				systemcatalog.GetDirTableName(vertexTableFileID),
			)
		}
		calculatedKeyBytesCnt += uint32(colInfo.Size())
	}

	err = prepareFSforTable(s.fs, indexFilePath)
	if err != nil {
		return fmt.Errorf("unable to prepare file system for table: %w", err)
	}

	dirTableMeta, err := s.catalog.GetDirTableMeta(vertexTableFileID)
	if err != nil {
		return err
	}

	idxCreateReq := storage.IndexMeta{
		Name:        indexName,
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

	err = s.buildDirIndex(txnID, dirTableMeta.FileID, idxCreateReq, cToken, logger)
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
	return s.createDirTableIndex(
		txnID,
		getTableSystemIndexName(directoryTableFileID),
		vertexTableFileID,
		columns,
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
		return nil, fmt.Errorf("unable to upgrade catalog lock: %w", txns.ErrDeadlockPrevention)
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
		return fmt.Errorf("unable to upgrade catalog lock: %w", txns.ErrDeadlockPrevention)
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
		return fmt.Errorf("unable to get system catalog X-lock: %w", txns.ErrDeadlockPrevention)
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
		return fmt.Errorf("unable to get system catalog X-lock: %w", txns.ErrDeadlockPrevention)
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
