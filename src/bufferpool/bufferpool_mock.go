package bufferpool

import (
	"fmt"
	"sync"

	"github.com/Blackdeer1524/GraphDB/src/storage/page"
)

var (
	_ BufferPool[*page.SlottedPage] = &BufferPool_mock{}
)

type BufferPool_mock struct {
	pages    sync.Map // map[PageIdentity]*page.SlottedPage
	unpinned sync.Map // map[PageIdentity]bool
}

func NewBufferPoolMock() *BufferPool_mock {
	return &BufferPool_mock{}
}

func (b *BufferPool_mock) Unpin(pageID PageIdentity) error {
	b.unpinned.Store(pageID, true)
	return nil
}

func (b *BufferPool_mock) GetPage(
	pageID PageIdentity,
) (*page.SlottedPage, error) {
	val, ok := b.pages.Load(pageID)

	var p *page.SlottedPage

	if !ok {
		p = page.NewSlottedPage(pageID.FileID, pageID.PageID)
		b.pages.Store(pageID, p)
	} else {
		p = val.(*page.SlottedPage)
	}

	b.unpinned.Store(pageID, false)

	return p, nil
}

func (b *BufferPool_mock) GetPageNoCreate(
	pageID PageIdentity,
) (*page.SlottedPage, error) {
	val, ok := b.pages.Load(pageID)
	if !ok {
		return nil, ErrNoSuchPage
	}

	b.unpinned.Store(pageID, false)

	return val.(*page.SlottedPage), nil
}

func (b *BufferPool_mock) FlushPage(pageID PageIdentity) error {
	return nil
}

func (b *BufferPool_mock) EnsureAllPagesUnpinned() error {
	pinnedIDs := []PageIdentity{}

	b.pages.Range(func(key, _ interface{}) bool {
		pageID := key.(PageIdentity)
		val, ok := b.unpinned.Load(pageID)

		if !ok || val == false {
			pinnedIDs = append(pinnedIDs, pageID)
		}

		return true
	})

	if len(pinnedIDs) > 0 {
		return fmt.Errorf("not all pages were unpinned: %+v", pinnedIDs)
	}

	return nil
}
