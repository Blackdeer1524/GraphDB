package bufferpool

import (
	"github.com/stretchr/testify/mock"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
)

// Мок DiskManager
type MockDiskManager struct {
	mock.Mock
}

func (m *MockDiskManager) BulkWritePageAssumeLockedBegin(
	fileID common.FileID,
) (func(page *page.SlottedPage, pageID common.PageID) error, func() error, error) {
	args := m.Called(fileID)
	return args.Get(0).(func(page *page.SlottedPage, pageID common.PageID) error), args.Get(1).(func() error), args.Error(
		2,
	)
}

var (
	_ common.DiskManager[*page.SlottedPage] = &MockDiskManager{}
)

func (m *MockDiskManager) ReadPage(
	pg *page.SlottedPage,
	pageIdent common.PageIdentity,
) error {
	args := m.Called(pg, pageIdent)
	return args.Error(0)
}

func (m *MockDiskManager) ReadPageAssumeLocked(
	pg *page.SlottedPage,
	pageIdent common.PageIdentity,
) error {
	args := m.Called(pg, pageIdent)
	return args.Error(0)
}

func (m *MockDiskManager) Lock() {
	m.Called()
}

func (m *MockDiskManager) Unlock() {
	m.Called()
}

func (m *MockDiskManager) GetPageNoNew(
	pg *page.SlottedPage,
	pageIdent common.PageIdentity,
) error {
	args := m.Called(pg, pageIdent)
	return args.Error(0)
}

func (m *MockDiskManager) GetPageNoNewAssumeLocked(
	pg *page.SlottedPage,
	pageIdent common.PageIdentity,
) error {
	args := m.Called(pg, pageIdent)
	return args.Error(0)
}

func (m *MockDiskManager) WritePageAssumeLocked(
	page *page.SlottedPage,
	pageIdent common.PageIdentity,
) error {
	args := m.Called(page, pageIdent)
	return args.Error(0)
}

// Мок Replacer
type MockReplacer struct {
	mock.Mock
}

func (m *MockReplacer) Pin(pageIdent common.PageIdentity) {
	m.Called(pageIdent)
}

func (m *MockReplacer) Unpin(pageIdent common.PageIdentity) {
	m.Called(pageIdent)
}

func (m *MockReplacer) ChooseVictim() (common.PageIdentity, error) {
	args := m.Called()
	return args.Get(0).(common.PageIdentity), args.Error(1)
}

func (m *MockReplacer) GetSize() uint64 {
	args := m.Called()
	return args.Get(0).(uint64)
}
