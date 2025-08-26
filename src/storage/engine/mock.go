package engine

import (
	"os"

	"github.com/stretchr/testify/mock"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

// --- Mocks ---

type mockLocker struct {
	mock.Mock
}

func (m *mockLocker) LockCatalog(
	txnID common.TxnID,
	lockMode txns.GranularLockMode,
) *txns.CatalogLockToken {
	args := m.Called(txnID, lockMode)
	return args.Get(0).(*txns.CatalogLockToken)
}

func (m *mockLocker) LockFile(
	t *txns.CatalogLockToken,
	fileID common.FileID,
	lockMode txns.GranularLockMode,
) *txns.FileLockToken {
	args := m.Called(t, fileID, lockMode)
	return args.Get(0).(*txns.FileLockToken)
}

func (m *mockLocker) LockPage(
	ft *txns.FileLockToken,
	pageID common.PageID,
	lockMode txns.PageLockMode,
) *txns.PageLockToken {
	args := m.Called(ft, pageID, lockMode)
	return args.Get(0).(*txns.PageLockToken)
}

func (m *mockLocker) Unlock(t *txns.CatalogLockToken) {
	m.Called(t)
}

func (m *mockLocker) UpgradeCatalogLock(
	t *txns.CatalogLockToken,
	lockMode txns.GranularLockMode,
) bool {
	args := m.Called(t, lockMode)
	return args.Bool(0)
}

func (m *mockLocker) UpgradeFileLock(ft *txns.FileLockToken, lockMode txns.GranularLockMode) bool {
	args := m.Called(ft, lockMode)
	return args.Bool(0)
}

func (m *mockLocker) UpgradePageLock(pt *txns.PageLockToken) bool {
	args := m.Called(pt)
	return args.Bool(0)
}

var _ Locker = &mockLocker{}

type mockCatalog struct {
	mock.Mock
}

func (m *mockCatalog) AddIndex(req storage.Index) error {
	args := m.Called(req)
	return args.Error(0)
}

func (m *mockCatalog) DropIndex(name string) error {
	args := m.Called(name)
	return args.Error(0)
}

func (m *mockCatalog) GetNewFileID() uint64 {
	args := m.Called()
	return args.Get(0).(uint64)
}

func (m *mockCatalog) GetBasePath() string {
	args := m.Called()
	return args.String(0)
}

func (m *mockCatalog) AddVertexTable(req storage.VertexTable) error {
	args := m.Called(req)
	return args.Error(0)
}

func (m *mockCatalog) DropVertexTable(name string) error {
	args := m.Called(name)
	return args.Error(0)
}

func (m *mockCatalog) AddEdgeTable(req storage.EdgeTable) error {
	args := m.Called(req)
	return args.Error(0)
}

func (m *mockCatalog) DropEdgeTable(name string) error {
	args := m.Called(name)
	return args.Error(0)
}

func (m *mockCatalog) Save() error {
	args := m.Called()
	return args.Error(0)
}

type mockDisk struct {
	mock.Mock
}

func (m *mockDisk) Stat(name string) (os.FileInfo, error) {
	args := m.Called(name)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(os.FileInfo), args.Error(1)
}

func (m *mockDisk) Create(name string) (*os.File, error) {
	args := m.Called(name)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*os.File), args.Error(1)
}

func (m *mockDisk) Remove(name string) error {
	args := m.Called(name)
	return args.Error(0)
}

func (m *mockDisk) MkdirAll(path string, perm os.FileMode) error {
	args := m.Called(path, perm)
	return args.Error(0)
}

func (m *mockDisk) IsFileExists(path string) (bool, error) {
	args := m.Called(path)
	return args.Bool(0), args.Error(1)
}
