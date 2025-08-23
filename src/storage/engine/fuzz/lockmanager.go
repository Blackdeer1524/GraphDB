package fuzz

import (
	"github.com/Blackdeer1524/GraphDB/src/txns"
	"sync"
)

type MockRWMutexLockManager struct {
	mu *sync.RWMutex
}

func newMockRWMutexLockManager() *MockRWMutexLockManager {
	return &MockRWMutexLockManager{
		mu: new(sync.RWMutex),
	}
}

func (m *MockRWMutexLockManager) GetSystemCatalogLock(req txns.SystemCatalogLockRequest) bool {
	m.mu.Lock()

	return true
}

func (m *MockRWMutexLockManager) GetPageLock(req txns.PageLockRequest) bool {
	m.mu.Lock()

	return true
}

func (m *MockRWMutexLockManager) UpgradePageLock(req txns.PageLockRequest) bool {
	m.mu.Lock()

	return true
}

func (m *MockRWMutexLockManager) UnlockAll() {
	m.mu.Unlock()
}
