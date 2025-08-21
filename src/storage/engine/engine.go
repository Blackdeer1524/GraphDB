package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/storage/disk"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
	"github.com/Blackdeer1524/GraphDB/src/storage/systemcatalog"
	"github.com/Blackdeer1524/GraphDB/src/txns"
	"github.com/spf13/afero"
)

type Locker interface {
	GetPageLock(req txns.PageLockRequest) bool
	UpgradePageLock(req txns.PageLockRequest) bool

	GetSystemCatalogLock(req txns.SystemCatalogLockRequest) bool
}

type SystemCatalog interface {
	GetNewFileID() uint64
	GetBasePath() string

	GetVertexTableMeta(name string) (storage.VertexTable, error)
	VertexTableExists(name string) (bool, error)
	AddVertexTable(req storage.VertexTable) error
	DropVertexTable(name string) error

	GetEdgeTableMeta(name string) (storage.EdgeTable, error)
	EdgeTableExists(name string) (bool, error)
	AddEdgeTable(req storage.EdgeTable) error
	DropEdgeTable(name string) error

	IndexExists(name string) (bool, error)
	AddIndex(req storage.Index) error
	DropIndex(name string) error

	Save() error
	CurrentVersion() uint64
}

type StorageEngine struct {
	lock    Locker
	catalog SystemCatalog
	diskMgr *disk.Manager[*page.SlottedPage]

	fs afero.Fs
}

func New(basePath string, poolSize uint64, fs afero.Fs, l Locker, d afero.Fs) (*StorageEngine, error) {
	err := systemcatalog.InitSystemCatalog(basePath, fs)
	if err != nil {
		return nil, fmt.Errorf("failed to : %w", err)
	}

	fileIDToFilePath := map[common.FileID]string{
		common.FileID(0): systemcatalog.GetSystemCatalogVersionFileName(basePath),
	}

	diskMgr := disk.New[*page.SlottedPage](fileIDToFilePath, page.NewSlottedPage)

	bpManager, err := bufferpool.New(poolSize, &bufferpool.MockReplacer{}, diskMgr)
	if err != nil {
		return nil, fmt.Errorf("failed to create bufferpool: %w", err)
	}

	sysCat, err := systemcatalog.New(basePath, fs, bpManager)
	if err != nil {
		return nil, fmt.Errorf("failed to create systemcatalog: %w", err)
	}

	diskMgr.UpdateFileMap(sysCat.GetFileIDToPathMap())

	return &StorageEngine{
		catalog: sysCat,
		lock:    l,
		fs:      d,
		diskMgr: diskMgr,
	}, nil
}

func getVertexTableFilePath(basePath, name string) string {
	return filepath.Join(basePath, "tables", "vertex", name+".tbl")
}

func getEdgeTableFilePath(basePath, name string) string {
	return filepath.Join(basePath, "tables", "edge", name+".tbl")
}

func getIndexFilePath(basePath, name string) string {
	return filepath.Join(basePath, "indexes", name+".idx")
}

func (s *StorageEngine) CreateVertexTable(txnID common.TxnID, name string, schema storage.Schema) error {
	needToRollback := true

	systemCatalogLockRequest := txns.SystemCatalogLockRequest{
		TxnID:    txnID,
		LockMode: txns.SystemCatalogExclusive,
	}

	if !s.lock.GetSystemCatalogLock(systemCatalogLockRequest) {
		return errors.New("unable to get system catalog lock")
	}

	basePath := s.catalog.GetBasePath()
	fileID := s.catalog.GetNewFileID()

	tableFilePath := getVertexTableFilePath(basePath, name)

	// Existence of the file is not the proof of existence of the table (we don't remove file on drop),
	// and it is why we do not check if the table exists in system catalog.
	ok, err := s.catalog.VertexTableExists(name)
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
	if err := os.MkdirAll(dir, 0o755); err != nil {
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

	tblCreateReq := storage.VertexTable{
		Name:       name,
		PathToFile: tableFilePath,
		FileID:     fileID,
		Schema:     schema,
	}

	err = s.catalog.AddVertexTable(tblCreateReq)
	if err != nil {
		return fmt.Errorf("unable to add vertex table to catalog: %w", err)
	}
	defer func() {
		if needToRollback {
			_ = s.catalog.DropVertexTable(name)
		}
	}()

	err = s.catalog.Save()
	if err != nil {
		return fmt.Errorf("unable to save catalog: %w", err)
	}

	s.diskMgr.InsertToFileMap(common.FileID(fileID), tableFilePath)

	needToRollback = false

	return nil
}

func (s *StorageEngine) DropVertexTable(txnID common.TxnID, name string) error {
	systemCatalogLockRequest := txns.SystemCatalogLockRequest{
		TxnID:    txnID,
		LockMode: txns.SystemCatalogExclusive,
	}

	if !s.lock.GetSystemCatalogLock(systemCatalogLockRequest) {
		return errors.New("unable to get system catalog lock")
	}

	// do not delete file, just remove from catalog

	err := s.catalog.DropVertexTable(name)
	if err != nil {
		return fmt.Errorf("unable to drop vertex table: %w", err)
	}

	err = s.catalog.Save()
	if err != nil {
		return fmt.Errorf("unable to save catalog: %w", err)
	}

	return nil
}

func (s *StorageEngine) CreateEdgesTable(txnID common.TxnID, name string, schema storage.Schema) error {
	needToRollback := true

	systemCatalogLockRequest := txns.SystemCatalogLockRequest{
		TxnID:    txnID,
		LockMode: txns.SystemCatalogExclusive,
	}

	if !s.lock.GetSystemCatalogLock(systemCatalogLockRequest) {
		return errors.New("unable to get system catalog lock")
	}

	basePath := s.catalog.GetBasePath()
	fileID := s.catalog.GetNewFileID()

	tableFilePath := getEdgeTableFilePath(basePath, name)

	// Existence of the file is not the proof of existence of the table (we don't remove file on drop),
	// and it is why we do not check if the table exists in system catalog.
	ok, err := s.catalog.EdgeTableExists(name)
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
		return fmt.Errorf("file %s already exists", tableFilePath)
	}

	file, err := s.fs.Create(tableFilePath)
	if err != nil {
		return fmt.Errorf("unable to create directory: %w", err)
	}
	_ = file.Close()
	defer func() {
		if needToRollback {
			_ = s.fs.Remove(tableFilePath)
		}
	}()

	// update info in metadata

	tblCreateReq := storage.EdgeTable{
		Name:       name,
		PathToFile: tableFilePath,
		FileID:     fileID,
		Schema:     schema,
	}

	err = s.catalog.AddEdgeTable(tblCreateReq)
	if err != nil {
		return fmt.Errorf("unable to add vertex table to catalog: %w", err)
	}
	defer func() {
		if needToRollback {
			_ = s.catalog.DropEdgeTable(name)
		}
	}()

	err = s.catalog.Save()
	if err != nil {
		return fmt.Errorf("unable to save catalog: %w", err)
	}

	s.diskMgr.InsertToFileMap(common.FileID(fileID), tableFilePath)

	needToRollback = false

	return nil
}

func (s *StorageEngine) DropEdgesTable(txnID common.TxnID, name string) error {
	systemCatalogLockRequest := txns.SystemCatalogLockRequest{
		TxnID:    txnID,
		LockMode: txns.SystemCatalogExclusive,
	}

	if !s.lock.GetSystemCatalogLock(systemCatalogLockRequest) {
		return errors.New("unable to get system catalog lock")
	}

	// do not delete file, just remove from catalog

	err := s.catalog.DropEdgeTable(name)
	if err != nil {
		return fmt.Errorf("unable to drop vertex table: %w", err)
	}

	err = s.catalog.Save()
	if err != nil {
		return fmt.Errorf("unable to save catalog: %w", err)
	}

	return nil
}

func (s *StorageEngine) CreateIndex(
	txnID common.TxnID,
	name string,
	tableName string,
	tableKind string,
	columns []string,
	keyBytesCnt uint32,
) error {
	needToRollback := true

	systemCatalogLockRequest := txns.SystemCatalogLockRequest{
		TxnID:    txnID,
		LockMode: txns.SystemCatalogExclusive,
	}

	if !s.lock.GetSystemCatalogLock(systemCatalogLockRequest) {
		return errors.New("unable to get system catalog lock")
	}

	basePath := s.catalog.GetBasePath()
	fileID := s.catalog.GetNewFileID()

	tableFilePath := getIndexFilePath(basePath, name)

	// Existence of the file is not the proof of existence of the index (we don't remove file on drop),
	// and it is why we do not check if the table exists in system catalog.
	ok, err := s.catalog.EdgeTableExists(name)
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
		return fmt.Errorf("file %s already exists", tableFilePath)
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
		TableKind:   tableKind,
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

	err = s.catalog.Save()
	if err != nil {
		return fmt.Errorf("unable to save catalog: %w", err)
	}

	s.diskMgr.InsertToFileMap(common.FileID(fileID), tableFilePath)

	needToRollback = false

	return nil
}

func (s *StorageEngine) DropIndex(txnID common.TxnID, name string) error {
	systemCatalogLockRequest := txns.SystemCatalogLockRequest{
		TxnID:    txnID,
		LockMode: txns.SystemCatalogExclusive,
	}

	if !s.lock.GetSystemCatalogLock(systemCatalogLockRequest) {
		return errors.New("unable to get system catalog lock")
	}

	// do not delete file, just remove from catalog

	err := s.catalog.DropIndex(name)
	if err != nil {
		return fmt.Errorf("unable to drop vertex table: %w", err)
	}

	err = s.catalog.Save()
	if err != nil {
		return fmt.Errorf("unable to save catalog: %w", err)
	}

	return nil
}
