package engine

import (
	"os"

	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/txns"
	"github.com/stretchr/testify/mock"
)

// --- Mocks ---

type mockLocker struct {
	mock.Mock
}

func (m *mockLocker) GetPageLock(req txns.PageLockRequest) bool {
	args := m.Called(req)
	return args.Bool(0)
}

func (m *mockLocker) UpgradePageLock(req txns.PageLockRequest) bool {
	args := m.Called(req)
	return args.Bool(0)
}

func (m *mockLocker) GetSystemCatalogLock(req txns.SystemCatalogLockRequest) bool {
	args := m.Called(req)
	return args.Bool(0)
}

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
