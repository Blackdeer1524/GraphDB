package engine

import (
	"errors"
	"fmt"
	"github.com/Blackdeer1524/GraphDB/src/storage/systemcatalog"
	"github.com/Blackdeer1524/GraphDB/src/txns"
	"os"
	"path/filepath"
)

type Locker interface {
	GetPageLock(req txns.PageLockRequest) bool
	UpgradePageLock(req txns.PageLockRequest) bool

	GetSystemCatalogLock(req txns.SystemCatalogLockRequest) bool
}

type SystemCatalog interface {
	GetNewFileID() uint64
	GetBasePath() string
	AddVertexTable(req systemcatalog.VertexTable) error
	DropVertexTable(name string) error
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

func New(s SystemCatalog, l Locker, d Filesystem) *StorageEngine {
	return &StorageEngine{
		catalog: s,
		lock:    l,
		disk:    d,
	}
}

func getVertexTableFilePath(basePath, name string) string {
	return filepath.Join(basePath, "tables", "vertex", name+".tbl")
}

func (s *StorageEngine) CreateVertexTable(txnID txns.TxnID, name string, schema systemcatalog.Schema) error {
	needToRollback := true

	systemCatalogLockRequest := txns.SystemCatalogLockRequest{
		TxnID: txnID,
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
	file.Close()
	defer func() {
		if needToRollback {
			_ = s.disk.Remove(tableFilePath)
		}
	}()

	// update info in metadata

	tblCreateReq := systemcatalog.VertexTable{
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

func (s *StorageEngine) DropVertexTable() {
	panic("unimplemented")
}

func (s *StorageEngine) CreateEdgesTable() {
	panic("unimplemented")
}

func (s *StorageEngine) DropEdgesTable() {
	panic("unimplemented")
}
