package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage/disk"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
	"github.com/Blackdeer1524/GraphDB/src/storage/systemcatalog"
	"github.com/spf13/afero"

	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

type Locker interface {
	GetPageLock(req txns.PageLockRequest) bool
	UpgradePageLock(req txns.PageLockRequest) bool

	GetSystemCatalogLock(req txns.SystemCatalogLockRequest) bool
}

type SystemCatalog interface {
	GetNewFileID() uint64
	GetBasePath() string

	AddVertexTable(req storage.VertexTable) error
	DropVertexTable(name string) error
	AddEdgeTable(req storage.EdgeTable) error
	DropEdgeTable(name string) error
	AddIndex(req storage.Index) error
	DropIndex(name string) error

	Save() error
}

type Filesystem interface {
	Stat(name string) (os.FileInfo, error)
	Create(name string) (*os.File, error)
	Remove(name string) error
	MkdirAll(path string, perm os.FileMode) error
	IsFileExists(path string) (bool, error)
}

type StorageEngine struct {
	lock    Locker
	catalog SystemCatalog

	disk Filesystem
}

func New(basePath string, poolSize uint64, fs afero.Fs, l Locker, d Filesystem) (*StorageEngine, error) {
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
		disk:    d,
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

	fileExists, err := s.disk.IsFileExists(tableFilePath)
	if err != nil {
		return fmt.Errorf("unable to check if file exists: %w", err)
	}

	if fileExists {
		return fmt.Errorf("file %s already exists", tableFilePath)
	}

	file, err := s.disk.Create(tableFilePath)
	if err != nil {
		return fmt.Errorf("unable to create directory: %w", err)
	}
	_ = file.Close()
	defer func() {
		if needToRollback {
			_ = s.disk.Remove(tableFilePath)
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

	fileExists, err := s.disk.IsFileExists(tableFilePath)
	if err != nil {
		return fmt.Errorf("unable to check if file exists: %w", err)
	}

	if fileExists {
		return fmt.Errorf("file %s already exists", tableFilePath)
	}

	file, err := s.disk.Create(tableFilePath)
	if err != nil {
		return fmt.Errorf("unable to create directory: %w", err)
	}
	_ = file.Close()
	defer func() {
		if needToRollback {
			_ = s.disk.Remove(tableFilePath)
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

	fileExists, err := s.disk.IsFileExists(tableFilePath)
	if err != nil {
		return fmt.Errorf("unable to check if file exists: %w", err)
	}

	if fileExists {
		return fmt.Errorf("file %s already exists", tableFilePath)
	}

	file, err := s.disk.Create(tableFilePath)
	if err != nil {
		return fmt.Errorf("unable to create file: %w", err)
	}
	_ = file.Close()
	defer func() {
		if needToRollback {
			_ = s.disk.Remove(tableFilePath)
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
