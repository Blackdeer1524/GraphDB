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

type StorageEngine struct {
	lock    Locker
	catalog *systemcatalog.Manager
}

func New(s *systemcatalog.Manager, l Locker) *StorageEngine {
	return &StorageEngine{
		catalog: s,
	}
}

func getVertexTableFilePath(basePath, name string) string {
	return filepath.Join(basePath, "tables", "vertex", name+".tbl")
}

func isFileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}

	if os.IsNotExist(err) {
		return false, nil
	}

	return false, err
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

	fileExists, err := isFileExists(tableFilePath)
	if err != nil {
		return fmt.Errorf("unable to check if file exists: %w", err)
	}

	if fileExists {
		return fmt.Errorf("file %s already exists", tableFilePath)
	}

	file, err := os.Create(tableFilePath)
	if err != nil {
		return fmt.Errorf("unable to create directory: %w")
	}
	file.Close()
	defer func() {
		if needToRollback {
			_ = os.Remove(tableFilePath)
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
