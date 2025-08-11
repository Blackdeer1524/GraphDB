package hash

import (
	"github.com/Blackdeer1524/GraphDB/src/txns"
	"github.com/stretchr/testify/mock"
)

// Мок для StorageEngine
type mockStorageEngine struct {
	mock.Mock
}

func (m *mockStorageEngine) GetPage(pageID uint64, fileID uint64) (*mockPage, error) {
	args := m.Called(pageID, fileID)
	return args.Get(0).(*mockPage), args.Error(1)
}

func (m *mockStorageEngine) UnpinPage(pageID uint64, fileID uint64) error {
	args := m.Called(pageID, fileID)
	return args.Error(0)
}

func (m *mockStorageEngine) WritePage(pageID uint64, fileID uint64, page *mockPage) error {
	args := m.Called(pageID, fileID, page)
	return args.Error(0)
}

// Мок для Page
type mockPage struct {
	data    []byte
	dirty   bool
	locked  bool
	rlocked bool
}

func (p *mockPage) GetData() []byte {
	return p.data
}

func (p *mockPage) SetData(d []byte) {
	p.data = d
}

func (p *mockPage) SetDirtiness(val bool) {
	p.dirty = val
}

func (p *mockPage) IsDirty() bool {
	return p.dirty
}

func (p *mockPage) Lock() {
	p.locked = true
}

func (p *mockPage) Unlock() {
	p.locked = false
}

func (p *mockPage) RLock() {
	p.rlocked = true
}

func (p *mockPage) RUnlock() {
	p.rlocked = false
}

// Мок для Locker
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

func (m *mockLocker) GetPageUnlock(req txns.PageLockRequest) bool {
	args := m.Called(req)
	return args.Bool(0)
}
