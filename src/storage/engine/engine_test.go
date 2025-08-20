package engine

import (
	"os"
	"testing"

	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/txns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func Test_getVertexTableFilePath(t *testing.T) {
	ans := getVertexTableFilePath("/var/lib/graphdb", "friends")

	assert.Equal(t, "/var/lib/graphdb/tables/vertex/friends.tbl", ans)
}

func Test_getEdgeTableFilePath(t *testing.T) {
	ans := getEdgeTableFilePath("/var/lib/graphdb", "friends")

	assert.Equal(t, "/var/lib/graphdb/tables/edge/friends.tbl", ans)
}

/* Test for CreateVertexTable */

func TestCreateVertexTable_Success(t *testing.T) {
	locker := &mockLocker{}
	catalog := &mockCatalog{}
	disk := &mockDisk{}

	basePath := "/base/path"
	fileID := uint64(42)
	name := "users"
	txnID := txns.TxnID(1)
	schema := storage.Schema{}

	// expectations
	locker.On("GetSystemCatalogLock", txns.SystemCatalogLockRequest{
		TxnID:    txnID,
		LockMode: txns.SystemCatalogExclusive,
	}).Return(true)
	catalog.On("GetBasePath").Return(basePath)
	catalog.On("GetNewFileID").Return(fileID)

	tablePath := getVertexTableFilePath(basePath, name)
	disk.On("IsFileExists", tablePath).Return(false, nil)

	// create temp file to return from Create
	tmpFile, err := os.CreateTemp(t.TempDir(), "tbl-*")
	assert.NoError(t, err)
	disk.On("Create", tablePath).Return(tmpFile, nil)

	// AddVertexTable - we assert key fields
	catalog.On("AddVertexTable", mock.MatchedBy(func(v storage.VertexTable) bool {
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
	schema := storage.Schema{}

	// lock denied
	locker.On("GetSystemCatalogLock", txns.SystemCatalogLockRequest{
		TxnID:    txnID,
		LockMode: txns.SystemCatalogExclusive,
	}).Return(false)

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
	schema := storage.Schema{}

	locker.On("GetSystemCatalogLock", txns.SystemCatalogLockRequest{
		TxnID:    txnID,
		LockMode: txns.SystemCatalogExclusive,
	}).Return(true)
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
	schema := storage.Schema{}

	locker.On("GetSystemCatalogLock", txns.SystemCatalogLockRequest{
		TxnID:    txnID,
		LockMode: txns.SystemCatalogExclusive,
	}).Return(true)
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
	schema := storage.Schema{}

	locker.On("GetSystemCatalogLock", txns.SystemCatalogLockRequest{
		TxnID:    txnID,
		LockMode: txns.SystemCatalogExclusive,
	}).Return(true)
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
	schema := storage.Schema{}

	locker.On("GetSystemCatalogLock", txns.SystemCatalogLockRequest{
		TxnID:    txnID,
		LockMode: txns.SystemCatalogExclusive,
	}).Return(true)
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
	schema := storage.Schema{}

	locker.On("GetSystemCatalogLock", txns.SystemCatalogLockRequest{
		TxnID:    txnID,
		LockMode: txns.SystemCatalogExclusive,
	}).Return(true)
	catalog.On("GetBasePath").Return(basePath)
	catalog.On("GetNewFileID").Return(fileID)

	tablePath := getVertexTableFilePath(basePath, name)
	disk.On("IsFileExists", tablePath).Return(false, nil)

	tmpFile, err := os.CreateTemp(t.TempDir(), "tbl-*")
	assert.NoError(t, err)
	disk.On("Create", tablePath).Return(tmpFile, nil)

	// AddVertexTable succeeds, but Save fails -> both Remove and DropVertexTable should be called by defers
	catalog.On("AddVertexTable", mock.MatchedBy(func(v storage.VertexTable) bool {
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

/* Test for CreateEdgesTable */

func TestCreateEdgesTable_Success(t *testing.T) {
	locker := &mockLocker{}
	catalog := &mockCatalog{}
	disk := &mockDisk{}

	basePath := "/base/path"
	fileID := uint64(77)
	name := "edges"
	txnID := txns.TxnID(1)
	schema := storage.Schema{}

	locker.On("GetSystemCatalogLock", txns.SystemCatalogLockRequest{
		TxnID:    txnID,
		LockMode: txns.SystemCatalogExclusive,
	}).Return(true)

	catalog.On("GetBasePath").Return(basePath)
	catalog.On("GetNewFileID").Return(fileID)

	tablePath := getEdgeTableFilePath(basePath, name)
	disk.On("IsFileExists", tablePath).Return(false, nil)

	tmpFile, err := os.CreateTemp(t.TempDir(), "edge-*")
	assert.NoError(t, err)
	disk.On("Create", tablePath).Return(tmpFile, nil)

	catalog.On("AddEdgeTable", mock.MatchedBy(func(v storage.EdgeTable) bool {
		return v.Name == name && v.PathToFile == tablePath && v.FileID == fileID
	})).Return(nil)

	catalog.On("Save").Return(nil)

	se := &StorageEngine{lock: locker, catalog: catalog, disk: disk}

	err = se.CreateEdgesTable(txnID, name, schema)
	assert.NoError(t, err)

	locker.AssertExpectations(t)
	catalog.AssertExpectations(t)
	disk.AssertExpectations(t)

	disk.AssertNotCalled(t, "Remove", tablePath)
	catalog.AssertNotCalled(t, "DropEdgeTable", name)
}

func TestCreateEdgesTable_FailLock(t *testing.T) {
	locker := &mockLocker{}
	catalog := &mockCatalog{}
	disk := &mockDisk{}

	name := "edges"
	txnID := txns.TxnID(2)
	schema := storage.Schema{}

	locker.On("GetSystemCatalogLock", txns.SystemCatalogLockRequest{
		TxnID:    txnID,
		LockMode: txns.SystemCatalogExclusive,
	}).Return(false)

	se := &StorageEngine{lock: locker, catalog: catalog, disk: disk}

	err := se.CreateEdgesTable(txnID, name, schema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unable to get system catalog lock")

	catalog.AssertNotCalled(t, "GetBasePath")
	disk.AssertNotCalled(t, "IsFileExists")
}

func TestCreateEdgesTable_IsFileExistsError(t *testing.T) {
	locker := &mockLocker{}
	catalog := &mockCatalog{}
	disk := &mockDisk{}

	basePath := "/base/path"
	name := "edges"
	txnID := txns.TxnID(3)
	schema := storage.Schema{}

	locker.On("GetSystemCatalogLock", txns.SystemCatalogLockRequest{
		TxnID:    txnID,
		LockMode: txns.SystemCatalogExclusive,
	}).Return(true)

	catalog.On("GetBasePath").Return(basePath)
	catalog.On("GetNewFileID").Return(uint64(5))

	tablePath := getEdgeTableFilePath(basePath, name)
	disk.On("IsFileExists", tablePath).Return(false, assert.AnError)

	se := &StorageEngine{lock: locker, catalog: catalog, disk: disk}

	err := se.CreateEdgesTable(txnID, name, schema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unable to check if file exists")
}

func TestCreateEdgesTable_FileAlreadyExists(t *testing.T) {
	locker := &mockLocker{}
	catalog := &mockCatalog{}
	disk := &mockDisk{}

	basePath := "/base/path"
	name := "edges"
	txnID := txns.TxnID(4)
	schema := storage.Schema{}

	locker.On("GetSystemCatalogLock", txns.SystemCatalogLockRequest{
		TxnID:    txnID,
		LockMode: txns.SystemCatalogExclusive,
	}).Return(true)

	catalog.On("GetBasePath").Return(basePath)
	catalog.On("GetNewFileID").Return(uint64(6))

	tablePath := getEdgeTableFilePath(basePath, name)
	disk.On("IsFileExists", tablePath).Return(true, nil)

	se := &StorageEngine{lock: locker, catalog: catalog, disk: disk}

	err := se.CreateEdgesTable(txnID, name, schema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestCreateEdgesTable_CreateError(t *testing.T) {
	locker := &mockLocker{}
	catalog := &mockCatalog{}
	disk := &mockDisk{}

	basePath := "/base/path"
	name := "edges"
	txnID := txns.TxnID(5)
	schema := storage.Schema{}

	locker.On("GetSystemCatalogLock", txns.SystemCatalogLockRequest{
		TxnID:    txnID,
		LockMode: txns.SystemCatalogExclusive,
	}).Return(true)

	catalog.On("GetBasePath").Return(basePath)
	catalog.On("GetNewFileID").Return(uint64(7))

	tablePath := getEdgeTableFilePath(basePath, name)
	disk.On("IsFileExists", tablePath).Return(false, nil)
	disk.On("Create", tablePath).Return((*os.File)(nil), assert.AnError)

	se := &StorageEngine{lock: locker, catalog: catalog, disk: disk}

	err := se.CreateEdgesTable(txnID, name, schema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unable to create directory")
}

func TestCreateEdgesTable_AddEdgeTableError_RollsBackFile(t *testing.T) {
	locker := &mockLocker{}
	catalog := &mockCatalog{}
	disk := &mockDisk{}

	basePath := "/base/path"
	fileID := uint64(8)
	name := "edges"
	txnID := txns.TxnID(6)
	schema := storage.Schema{}

	locker.On("GetSystemCatalogLock", txns.SystemCatalogLockRequest{
		TxnID:    txnID,
		LockMode: txns.SystemCatalogExclusive,
	}).Return(true)

	catalog.On("GetBasePath").Return(basePath)
	catalog.On("GetNewFileID").Return(fileID)

	tablePath := getEdgeTableFilePath(basePath, name)
	disk.On("IsFileExists", tablePath).Return(false, nil)

	tmpFile, err := os.CreateTemp(t.TempDir(), "edge-*")
	assert.NoError(t, err)
	disk.On("Create", tablePath).Return(tmpFile, nil)

	catalog.On("AddEdgeTable", mock.Anything).Return(assert.AnError)
	disk.On("Remove", tablePath).Return(nil)

	se := &StorageEngine{lock: locker, catalog: catalog, disk: disk}

	err = se.CreateEdgesTable(txnID, name, schema)
	assert.Error(t, err)

	disk.AssertCalled(t, "Remove", tablePath)
	catalog.AssertNotCalled(t, "DropEdgeTable", name)
}

func TestCreateEdgesTable_SaveError_RollsBackFileAndCatalogEntry(t *testing.T) {
	locker := &mockLocker{}
	catalog := &mockCatalog{}
	disk := &mockDisk{}

	basePath := "/base/path"
	fileID := uint64(9)
	name := "edges"
	txnID := txns.TxnID(7)
	schema := storage.Schema{}

	locker.On("GetSystemCatalogLock", txns.SystemCatalogLockRequest{
		TxnID:    txnID,
		LockMode: txns.SystemCatalogExclusive,
	}).Return(true)

	catalog.On("GetBasePath").Return(basePath)
	catalog.On("GetNewFileID").Return(fileID)

	tablePath := getEdgeTableFilePath(basePath, name)
	disk.On("IsFileExists", tablePath).Return(false, nil)

	tmpFile, err := os.CreateTemp(t.TempDir(), "edge-*")
	assert.NoError(t, err)
	disk.On("Create", tablePath).Return(tmpFile, nil)

	catalog.On("AddEdgeTable", mock.Anything).Return(nil)
	catalog.On("Save").Return(assert.AnError)

	disk.On("Remove", tablePath).Return(nil)
	catalog.On("DropEdgeTable", name).Return(nil)

	se := &StorageEngine{lock: locker, catalog: catalog, disk: disk}

	err = se.CreateEdgesTable(txnID, name, schema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unable to save catalog")

	disk.AssertCalled(t, "Remove", tablePath)
	catalog.AssertCalled(t, "DropEdgeTable", name)
}

/* Test for IndexCreate */

func TestCreateIndex_Success(t *testing.T) {
	lock := new(mockLocker)
	cat := new(mockCatalog)
	fs := new(mockDisk)

	txnID := txns.TxnID(1)
	name := "idx1"
	tableName := "tbl"
	tableKind := "vertex"
	columns := []string{"col1"}
	keyBytes := uint32(8)

	basePath := "/tmp"
	filePath := getIndexFilePath(basePath, name)

	lock.On("GetSystemCatalogLock", mock.Anything).Return(true)

	cat.On("GetBasePath").Return(basePath)
	cat.On("GetNewFileID").Return(uint64(1))
	cat.On("AddIndex", mock.Anything).Return(nil)
	cat.On("Save").Return(nil)

	fs.On("IsFileExists", filePath).Return(false, nil)
	fs.On("Create", filePath).Return(os.NewFile(0, ""), nil)

	se := New(cat, lock, fs)

	err := se.CreateIndex(txnID, name, tableName, tableKind, columns, keyBytes)
	assert.NoError(t, err)

	lock.AssertExpectations(t)
	cat.AssertExpectations(t)
	fs.AssertExpectations(t)
}

func TestCreateIndex_NoLock(t *testing.T) {
	lock := new(mockLocker)
	cat := new(mockCatalog)
	fs := new(mockDisk)

	lock.On("GetSystemCatalogLock", mock.Anything).Return(false)

	se := New(cat, lock, fs)
	err := se.CreateIndex(1, "idx1", "tbl", "vertex", nil, 4)

	assert.EqualError(t, err, "unable to get system catalog lock")
}
