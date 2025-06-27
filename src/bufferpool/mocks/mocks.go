package mocks

import (
	"github.com/stretchr/testify/mock"
)

type SlottedPage struct {
	data   []byte
	fileID uint64
	pageID uint64
	dirty  bool
}

func (p *SlottedPage) GetData() []byte       { return p.data }
func (p *SlottedPage) SetData(d []byte)      { p.data = d }
func (p *SlottedPage) SetDirtiness(val bool) { p.dirty = val }
func (p *SlottedPage) IsDirty() bool         { return p.dirty }
func (p *SlottedPage) GetFileID() uint64     { return p.fileID }
func (p *SlottedPage) GetPageID() uint64     { return p.pageID }
func (p *SlottedPage) Lock()                 {}
func (p *SlottedPage) Unlock()               {}
func (p *SlottedPage) RLock()                {}
func (p *SlottedPage) RUnlock()              {}

// Фабрика для создания SlottedPage
func NewSlottedPage(fileID, pageID uint64) *SlottedPage {
	return &SlottedPage{
		fileID: fileID,
		pageID: pageID,
		data:   []byte{},
		dirty:  false,
	}
}

// Мок DiskManager
type MockDiskManager struct {
	mock.Mock
}

func (m *MockDiskManager) ReadPage(
	fileID, pageID uint64,
) (*SlottedPage, error) {
	args := m.Called(fileID, pageID)
	return args.Get(0).(*SlottedPage), args.Error(1)
}

func (m *MockDiskManager) WritePage(page *SlottedPage) error {
	args := m.Called(page)
	return args.Error(0)
}

// Мок Replacer
type MockReplacer struct {
	mock.Mock
}

func (m *MockReplacer) Pin(frameID uint64) {
	m.Called(frameID)
}

func (m *MockReplacer) Unpin(frameID uint64) {
	m.Called(frameID)
}

func (m *MockReplacer) ChooseVictim() (uint64, error) {
	args := m.Called()
	return args.Get(0).(uint64), args.Error(1)
}

func (m *MockReplacer) GetSize() uint64 {
	args := m.Called()
	return args.Get(0).(uint64)
}
