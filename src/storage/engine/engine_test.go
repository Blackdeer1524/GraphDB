package engine

import (
	"github.com/Blackdeer1524/GraphDB/src/storage/systemcatalog"
	"github.com/Blackdeer1524/GraphDB/src/txns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"os"
	"testing"
)

func TestCreateVertexTable_Success(t *testing.T) {
	locker := &mockLocker{}
	catalog := &mockCatalog{}
	disk := &mockDisk{}

	basePath := "/base/path"
	fileID := uint64(42)
	name := "users"
	txnID := txns.TxnID(1)
	schema := systemcatalog.Schema{}

	// expectations
	locker.On("GetSystemCatalogLock", txns.SystemCatalogLockRequest{TxnID: txnID}).Return(true)
	catalog.On("GetBasePath").Return(basePath)
	catalog.On("GetNewFileID").Return(fileID)

	tablePath := getVertexTableFilePath(basePath, name)
	disk.On("IsFileExists", tablePath).Return(false, nil)

	// create temp file to return from Create
	tmpFile, err := os.CreateTemp(t.TempDir(), "tbl-*")
	assert.NoError(t, err)
	disk.On("Create", tablePath).Return(tmpFile, nil)

	// AddVertexTable - we assert key fields
	catalog.On("AddVertexTable", mock.MatchedBy(func(v systemcatalog.VertexTable) bool {
		return v.Name == name && v.PathToFile == tablePath && v.FileID == fileID
	})).Return(nil)

	catalog.On("Save").Return(nil)

	// assemble engine
	se := New(catalog, locker, disk)

	// call
	err = se.CreateVertexTable(txnID, name, schema)
	assert.NoError(t, err)

	// verify expectations / side-effects
	locker.AssertExpectations(t)
	catalog.AssertExpectations(t)
	disk.AssertExpectations(t)

	// on success removal/drop should NOT be called
	disk.AssertNotCalled(t, "Remove", tablePath)
	catalog.AssertNotCalled(t, "DropVertexTable", name)
}

func TestCreateVertexTable_FailLock(t *testing.T) {
	locker := &mockLocker{}
	catalog := &mockCatalog{}
	disk := &mockDisk{}

	name := "users"
	txnID := txns.TxnID(2)
	schema := systemcatalog.Schema{}

	// lock denied
	locker.On("GetSystemCatalogLock", txns.SystemCatalogLockRequest{TxnID: txnID}).Return(false)

	se := New(catalog, locker, disk)

	err := se.CreateVertexTable(txnID, name, schema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unable to get system catalog lock")

	// nothing else should be called
	catalog.AssertNotCalled(t, "GetBasePath")
	disk.AssertNotCalled(t, "IsFileExists")
}

func TestCreateVertexTable_IsFileExistsError(t *testing.T) {
	locker := &mockLocker{}
	catalog := &mockCatalog{}
	disk := &mockDisk{}

	basePath := "/base/path"
	name := "users"
	txnID := txns.TxnID(3)
	schema := systemcatalog.Schema{}

	locker.On("GetSystemCatalogLock", txns.SystemCatalogLockRequest{TxnID: txnID}).Return(true)
	catalog.On("GetBasePath").Return(basePath)
	catalog.On("GetNewFileID").Return(uint64(10))

	tablePath := getVertexTableFilePath(basePath, name)
	disk.On("IsFileExists", tablePath).Return(false, assert.AnError) // simulate error

	se := New(catalog, locker, disk)

	err := se.CreateVertexTable(txnID, name, schema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unable to check if file exists")

	locker.AssertExpectations(t)
	catalog.AssertExpectations(t)
	disk.AssertExpectations(t)
}

func TestCreateVertexTable_FileAlreadyExists(t *testing.T) {
	locker := &mockLocker{}
	catalog := &mockCatalog{}
	disk := &mockDisk{}

	basePath := "/base/path"
	name := "users"
	txnID := txns.TxnID(4)
	schema := systemcatalog.Schema{}

	locker.On("GetSystemCatalogLock", txns.SystemCatalogLockRequest{TxnID: txnID}).Return(true)
	catalog.On("GetBasePath").Return(basePath)
	catalog.On("GetNewFileID").Return(uint64(11))

	tablePath := getVertexTableFilePath(basePath, name)
	disk.On("IsFileExists", tablePath).Return(true, nil)

	se := New(catalog, locker, disk)

	err := se.CreateVertexTable(txnID, name, schema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestCreateVertexTable_CreateError(t *testing.T) {
	locker := &mockLocker{}
	catalog := &mockCatalog{}
	disk := &mockDisk{}

	basePath := "/base/path"
	name := "users"
	txnID := txns.TxnID(5)
	schema := systemcatalog.Schema{}

	locker.On("GetSystemCatalogLock", txns.SystemCatalogLockRequest{TxnID: txnID}).Return(true)
	catalog.On("GetBasePath").Return(basePath)
	catalog.On("GetNewFileID").Return(uint64(12))

	tablePath := getVertexTableFilePath(basePath, name)
	disk.On("IsFileExists", tablePath).Return(false, nil)
	disk.On("Create", tablePath).Return((*os.File)(nil), assert.AnError) // create fails

	se := New(catalog, locker, disk)

	err := se.CreateVertexTable(txnID, name, schema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unable to create directory")
}

func TestCreateVertexTable_AddVertexTableError_RollsBackFile(t *testing.T) {
	locker := &mockLocker{}
	catalog := &mockCatalog{}
	disk := &mockDisk{}

	basePath := "/base/path"
	fileID := uint64(99)
	name := "users"
	txnID := txns.TxnID(6)
	schema := systemcatalog.Schema{}

	locker.On("GetSystemCatalogLock", txns.SystemCatalogLockRequest{TxnID: txnID}).Return(true)
	catalog.On("GetBasePath").Return(basePath)
	catalog.On("GetNewFileID").Return(fileID)

	tablePath := getVertexTableFilePath(basePath, name)
	disk.On("IsFileExists", tablePath).Return(false, nil)

	tmpFile, err := os.CreateTemp(t.TempDir(), "tbl-*")
	assert.NoError(t, err)
	disk.On("Create", tablePath).Return(tmpFile, nil)

	// AddVertexTable fails -> we expect disk.Remove to be called by defer
	catalog.On("AddVertexTable", mock.Anything).Return(assert.AnError)
	disk.On("Remove", tablePath).Return(nil)

	se := New(catalog, locker, disk)

	err = se.CreateVertexTable(txnID, name, schema)
	assert.Error(t, err)

	// verify rollback called
	disk.AssertCalled(t, "Remove", tablePath)
	// DropVertexTable should NOT be called because AddVertexTable failed and defer wasn't installed
	catalog.AssertNotCalled(t, "DropVertexTable", name)

	locker.AssertExpectations(t)
	catalog.AssertExpectations(t)
	disk.AssertExpectations(t)
}

func TestCreateVertexTable_SaveError_RollsBackFileAndCatalogEntry(t *testing.T) {
	locker := &mockLocker{}
	catalog := &mockCatalog{}
	disk := &mockDisk{}

	basePath := "/base/path"
	fileID := uint64(100)
	name := "users"
	txnID := txns.TxnID(7)
	schema := systemcatalog.Schema{}

	locker.On("GetSystemCatalogLock", txns.SystemCatalogLockRequest{TxnID: txnID}).Return(true)
	catalog.On("GetBasePath").Return(basePath)
	catalog.On("GetNewFileID").Return(fileID)

	tablePath := getVertexTableFilePath(basePath, name)
	disk.On("IsFileExists", tablePath).Return(false, nil)

	tmpFile, err := os.CreateTemp(t.TempDir(), "tbl-*")
	assert.NoError(t, err)
	disk.On("Create", tablePath).Return(tmpFile, nil)

	// AddVertexTable succeeds, but Save fails -> both Remove and DropVertexTable should be called by defers
	catalog.On("AddVertexTable", mock.MatchedBy(func(v systemcatalog.VertexTable) bool {
		return v.Name == name && v.PathToFile == tablePath && v.FileID == fileID
	})).Return(nil)

	catalog.On("Save").Return(assert.AnError)
	// defers
	disk.On("Remove", tablePath).Return(nil)
	catalog.On("DropVertexTable", name).Return(nil)

	se := New(catalog, locker, disk)

	err = se.CreateVertexTable(txnID, name, schema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unable to save catalog")

	// verify rollback called
	disk.AssertCalled(t, "Remove", tablePath)
	catalog.AssertCalled(t, "DropVertexTable", name)

	locker.AssertExpectations(t)
	catalog.AssertExpectations(t)
	disk.AssertExpectations(t)
}
