package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/afero"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/storage/disk"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
	"github.com/Blackdeer1524/GraphDB/src/storage/systemcatalog"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

type DirectoryItemInternalFields struct {
	ID         storage.DirItemID
	NextItemID storage.DirItemID
	PrevItemID storage.DirItemID
}

type DirectoryItemDataFields struct {
	VertexID   storage.VertexID
	EdgeFileID common.FileID
	EdgeID     storage.EdgeID
}

type DirectoryItem struct {
	DirectoryItemInternalFields
	DirectoryItemDataFields
}

var directorySchema = storage.Schema{
	{Name: "ID", Type: storage.ColumnTypeUUID},
	{Name: "NextItemID", Type: storage.ColumnTypeUUID},
	{Name: "PrevItemID", Type: storage.ColumnTypeUUID},
	{Name: "VertexID", Type: storage.ColumnTypeUUID},
	{Name: "EdgeFileID", Type: storage.ColumnTypeUint64},
	{Name: "EdgeID", Type: storage.ColumnTypeUUID},
}

type EdgeInternalFields struct {
	ID              storage.EdgeID
	DirectoryItemID storage.DirItemID
	SrcVertexID     storage.VertexID
	DstVertexID     storage.VertexID
	NextEdgeID      storage.EdgeID
	PrevEdgeID      storage.EdgeID
}

func NewEdgeInternalFields(
	ID storage.EdgeID,
	DirectoryItemID storage.DirItemID,
	SrcVertexID storage.VertexID,
	DstVertexID storage.VertexID,
	PrevEdgeID storage.EdgeID,
	NextEdgeID storage.EdgeID,
) EdgeInternalFields {
	return EdgeInternalFields{
		ID:              ID,
		DirectoryItemID: DirectoryItemID,
		SrcVertexID:     SrcVertexID,
		DstVertexID:     DstVertexID,
		NextEdgeID:      NextEdgeID,
		PrevEdgeID:      PrevEdgeID,
	}
}

var edgeInternalSchema = storage.Schema{
	{Name: "ID", Type: storage.ColumnTypeUUID},
	{Name: "DirectoryItemID", Type: storage.ColumnTypeUUID},
	{Name: "SrcVertexID", Type: storage.ColumnTypeUUID},
	{Name: "DstVertexID", Type: storage.ColumnTypeUUID},
	{Name: "NextEdgeID", Type: storage.ColumnTypeUUID},
	{Name: "PrevEdgeID", Type: storage.ColumnTypeUUID},
}

type VertexInternalFields struct {
	ID          storage.VertexID
	DirItemID storage.DirItemID
}

var vertexInternalSchema = storage.Schema{
	{Name: "ID", Type: storage.ColumnTypeUUID},
	{Name: "DirectoryID", Type: storage.ColumnTypeUUID},
}

type SystemCatalog interface {
	GetNewFileID() uint64
	GetBasePath() string

	GetTableMeta(name string) (storage.Table, error)
	TableExists(name string) (bool, error)
	AddTable(req storage.Table) error
	DropTable(name string) error

	GetIndexMeta(name string) (storage.Index, error)
	IndexExists(name string) (bool, error)
	AddIndex(req storage.Index) error
	DropIndex(name string) error

	Save(logger common.ITxnLoggerWithContext) error
	CurrentVersion() uint64
}

type StorageEngine struct {
	catalog SystemCatalog
	diskMgr *disk.Manager
	locker  *txns.LockManager
	fs      afero.Fs
}

func New(
	catalogBasePath string,
	poolSize uint64,
	locker *txns.LockManager,
	fs afero.Fs,
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

	return &StorageEngine{
		catalog: sysCat,
		diskMgr: diskMgr,
		locker:  locker,
		fs:      fs,
	}, nil
}

func GetVertexTableFilePath(basePath, name string) string {
	return filepath.Join(basePath, "tables", "vertex", name+".tbl")
}

func GetEdgeTableFilePath(basePath, name string) string {
	return filepath.Join(basePath, "tables", "edge", name+".tbl")
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

func (s *StorageEngine) createTable(
	txnID common.TxnID,
	name string,
	schema storage.Schema,
	logger common.ITxnLoggerWithContext,
) error {
	needToRollback := true
	if s.locker.LockCatalog(txnID, txns.GRANULAR_LOCK_EXCLUSIVE) == nil {
		return errors.New("unable to get system catalog lock")
	}

	name = getVertexTableName(name)

	basePath := s.catalog.GetBasePath()
	fileID := s.catalog.GetNewFileID()
	tableFilePath := GetVertexTableFilePath(basePath, name)

	// Existence of the file is not the proof of existence of the table
	// (we don't remove file on drop),
	// and it is why we do not check if the table exists in file system.
	ok, err := s.catalog.TableExists(name)
	if err != nil {
		return fmt.Errorf("unable to check if vertex table exists: %w", err)
	}

	if ok {
		return fmt.Errorf("vertex table %s already exists", name)
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
		return fmt.Errorf("unable to add vertex table to catalog: %w", err)
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
	return s.createTable(txnID, getVertexTableName(name), schema, logger)
}

func (s *StorageEngine) CreateEdgesTable(
	txnID common.TxnID,
	name string,
	schema storage.Schema,
	logger common.ITxnLoggerWithContext,
) error {
	return s.createTable(txnID, getEdgeTableName(name), schema, logger)
}

func (s *StorageEngine) dropTable(
	txnID common.TxnID,
	name string,
	logger common.ITxnLoggerWithContext,
) error {
	ctoken := s.locker.LockCatalog(txnID, txns.GRANULAR_LOCK_EXCLUSIVE)
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
	name = getVertexTableName(name)
	return s.dropTable(txnID, name, logger)
}

func (s *StorageEngine) DropEdgesTable(
	txnID common.TxnID,
	name string,
	logger common.ITxnLoggerWithContext,
) error {
	name = getEdgeTableName(name)
	return s.dropTable(txnID, name, logger)
}

func getDirectoryTableName(name string) string {
	return "directory_" + name
}

func (s *StorageEngine) CreateDirectoryTable(
	txnID common.TxnID,
	vertexTableName string,
	logger common.ITxnLoggerWithContext,
) error {
	return s.createTable(
		txnID,
		getDirectoryTableName(vertexTableName),
		directorySchema,
		logger,
	)
}

func (s *StorageEngine) DropDirectoryTable(
	txnID common.TxnID,
	vertexTableName string,
	logger common.ITxnLoggerWithContext,
) error {
	return s.dropTable(txnID, getDirectoryTableName(vertexTableName), logger)
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
	ctoken := s.locker.LockCatalog(txnID, txns.GRANULAR_LOCK_EXCLUSIVE)
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
		return fmt.Errorf("unable to check if in table exists: %w", err)
	}

	if ok {
		return fmt.Errorf("vertex table %s already exists", name)
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
	name string,
	tableName string,
	columns []string,
	keyBytesCnt uint32,
	logger common.ITxnLoggerWithContext,
) error {
	if err := s.createIndex(txnID, getVertexTableName(name), tableName, columns, keyBytesCnt, logger); err != nil {
		return err
	}
	return nil
}

func (s *StorageEngine) CreateEdgesTableIndex(
	txnID common.TxnID,
	name string,
	tableName string,
	columns []string,
	keyBytesCnt uint32,
	logger common.ITxnLoggerWithContext,
) error {
	return s.createIndex(txnID, getEdgeTableName(name), tableName, columns, keyBytesCnt, logger)
}

func (s *StorageEngine) dropIndex(
	txnID common.TxnID,
	name string,
	logger common.ITxnLoggerWithContext,
) error {
	ctoken := s.locker.LockCatalog(txnID, txns.GRANULAR_LOCK_EXCLUSIVE)
	if ctoken == nil {
		return errors.New("unable to get system catalog lock")
	}

	// do not delete file, just remove from catalog

	err := s.catalog.DropIndex(name)
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
	name string,
	logger common.ITxnLoggerWithContext,
) error {
	return s.dropIndex(txnID, getVertexTableName(name), logger)
}

func (s *StorageEngine) DropEdgesTableIndex(
	txnID common.TxnID,
	name string,
	logger common.ITxnLoggerWithContext,
) error {
	return s.dropIndex(txnID, getEdgeTableName(name), logger)
}
