package bufferpool

import (
	"fmt"
	"sync"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
)

type BufferPool_mock struct {
	pagesMu sync.RWMutex
	pages   map[common.PageIdentity]*page.SlottedPage

	pinCountMu sync.RWMutex
	pinCounts  map[common.PageIdentity]int
}

var (
	_ BufferPool[*page.SlottedPage] = &BufferPool_mock{}
)

func NewBufferPoolMock() *BufferPool_mock {
	return &BufferPool_mock{
		pages:     make(map[common.PageIdentity]*page.SlottedPage),
		pinCounts: make(map[common.PageIdentity]int),
	}
}

func (b *BufferPool_mock) Unpin(pageID common.PageIdentity) error {
	b.pinCountMu.Lock()
	defer b.pinCountMu.Unlock()

	pinCount, ok := b.pinCounts[pageID]
	if !ok {
		return fmt.Errorf("page %v not found in pin counts", pageID)
	}

	if pinCount <= 0 {
		return fmt.Errorf("page %v has already been unpinned", pageID)
	}

	newPinCount := pinCount - 1
	b.pinCounts[pageID] = newPinCount
	return nil
}

func (b *BufferPool_mock) GetPage(
	pageID common.PageIdentity,
) (*page.SlottedPage, error) {
	b.pagesMu.RLock()
	p, exists := b.pages[pageID]
	b.pagesMu.RUnlock()

	if exists {
		b.pinCountMu.Lock()
		b.pinCounts[pageID]++
		b.pinCountMu.Unlock()
		return p, nil
	}

	b.pinCountMu.Lock()
	b.pagesMu.Lock()

	p, exists = b.pages[pageID]
	if exists {
		b.pinCounts[pageID]++
		b.pagesMu.Unlock()
		b.pinCountMu.Unlock()
		return p, nil
	}

	p = page.NewSlottedPage()
	b.pages[pageID] = p
	b.pinCounts[pageID] = 1

	b.pagesMu.Unlock()
	b.pinCountMu.Unlock()
	return p, nil
}

func (b *BufferPool_mock) GetPageNoCreate(
	pageID common.PageIdentity,
) (*page.SlottedPage, error) {
	b.pagesMu.RLock()
	p, exists := b.pages[pageID]
	b.pagesMu.RUnlock()

	if !exists {
		return nil, ErrNoSuchPage
	}

	b.pinCountMu.Lock()
	b.pinCounts[pageID]++
	b.pinCountMu.Unlock()
	return p, nil
}

func (b *BufferPool_mock) FlushPage(pageID common.PageIdentity) error {
	return nil
}

func (b *BufferPool_mock) EnsureAllPagesUnpinned() error {
	b.pinCountMu.RLock()
	defer b.pinCountMu.RUnlock()

	pinnedIDs := map[common.PageIdentity]int{}
	for pageID, pinCount := range b.pinCounts {
		if pinCount != 0 {
			pinnedIDs[pageID] = pinCount
		}
	}

	if len(pinnedIDs) > 0 {
		return fmt.Errorf(
			"not all pages were properly unpinned: %+v",
			pinnedIDs,
		)
	}

	return nil
}
