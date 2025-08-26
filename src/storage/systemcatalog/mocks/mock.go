package mocks

import (
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage/disk"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

type MockDataBufferPool struct {
	dirties []common.PageIdentity
	pages   map[common.PageIdentity]*page.SlottedPage

	Disk *disk.Manager[*page.SlottedPage]
}

func (bp *MockDataBufferPool) MarkDirty(id common.PageIdentity) {
	bp.dirties = append(bp.dirties, id)
}

func (bp *MockDataBufferPool) GetPage(id common.PageIdentity) (*page.SlottedPage, error) {
	p, err := bp.Disk.ReadPage(id)
	if err != nil {
		return nil, err
	}

	return p, nil
}

type MockLockManager struct {
	AllowLock bool
}

func (m *MockLockManager) GetSystemCatalogLock(req txns.SystemCatalogLockRequest) bool {
	return m.AllowLock
}

func (m *MockLockManager) GetPageLock(req txns.PageLockRequest) bool {
	return true
}

func (m *MockLockManager) UpgradePageLock(req txns.PageLockRequest) bool {
	return true
}
