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

func (m *MockDiskManager) ReadPage(
	pageIdent common.PageIdentity,
) (*page.SlottedPage, error) {
	args := m.Called(pageIdent)
	return args.Get(0).(*page.SlottedPage), args.Error(1)
}

func (m *MockDiskManager) WritePage(
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
