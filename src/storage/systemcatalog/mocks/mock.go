package mocks

import (
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
)

type MockDataBufferPool struct {
	dirties []common.PageIdentity
	pages   map[common.PageIdentity]*page.SlottedPage
}

func (bp *MockDataBufferPool) MarkDirty(id common.PageIdentity) {
	bp.dirties = append(bp.dirties, id)
}

func (bp *MockDataBufferPool) GetPage(common.PageIdentity) (*page.SlottedPage, error) {
	return nil, nil
}
