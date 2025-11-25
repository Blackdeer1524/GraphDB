package mocks

import (
	"errors"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
)

type MockStorageEngine struct {
	Pages map[uint64]*page.SlottedPage
	err   error
}

func (m *MockStorageEngine) GetPage(pageID uint64, fileID uint64) (*page.SlottedPage, error) {
	if m.err != nil {
		return nil, m.err
	}
	p, ok := m.Pages[pageID]
	if !ok {
		return nil, errors.New("page not found")
	}
	return p, nil
}
func (m *MockStorageEngine) UnpinPage(pageID uint64, fileID uint64) error { return nil }

func (m *MockStorageEngine) WritePage(pageID uint64, fileID uint64, p *page.SlottedPage) error {
	m.Pages[pageID] = p

	return nil
}

func (m *MockStorageEngine) Unpin(pageID common.PageIdentity) {
	return
}
