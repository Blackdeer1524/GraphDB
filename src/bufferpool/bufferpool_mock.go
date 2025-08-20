package bufferpool

import (
	"errors"
	"fmt"
	"sync"

	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
)

type BufferPool_mock struct {
	pagesMu sync.RWMutex
	pages   map[common.PageIdentity]*page.SlottedPage
	isDirty map[common.PageIdentity]bool

	pinCountMu sync.RWMutex
	pinCounts  map[common.PageIdentity]int

	leakedPages map[common.PageIdentity]struct{}
}

var (
	_ BufferPool = &BufferPool_mock{}
)

func NewBufferPoolMock(leakedPages []common.PageIdentity) *BufferPool_mock {
	m := make(map[common.PageIdentity]struct{}, len(leakedPages))
	for _, id := range leakedPages {
		m[id] = struct{}{}
	}

	return &BufferPool_mock{
		pages:       make(map[common.PageIdentity]*page.SlottedPage),
		pinCounts:   make(map[common.PageIdentity]int),
		isDirty:     make(map[common.PageIdentity]bool),
		leakedPages: m,
	}
}

func (b *BufferPool_mock) Unpin(pageID common.PageIdentity) {
	b.pinCountMu.Lock()
	defer b.pinCountMu.Unlock()

	pinCount, ok := b.pinCounts[pageID]
	assert.Assert(ok, "page %v not found in pin counts", pageID)
	assert.Assert(pinCount > 0, "page %v has already been unpinned", pageID)

	newPinCount := pinCount - 1
	b.pinCounts[pageID] = newPinCount
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
	b.isDirty[pageID] = false

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

// can only be called before Unpin and after page.unlock
func (b *BufferPool_mock) FlushPage(pageID common.PageIdentity) error {
	b.pagesMu.Lock()
	defer b.pagesMu.Unlock()

	_, ok := b.pages[pageID]
	if !ok {
		return ErrNoSuchPage
	}

	b.isDirty[pageID] = false

	return nil
}

func (b *BufferPool_mock) MarkDirty(pageID common.PageIdentity) {
	b.pagesMu.Lock()
	defer b.pagesMu.Unlock()

	_, ok := b.isDirty[pageID]
	assert.Assert(ok)

	b.isDirty[pageID] = true
}

func (b *BufferPool_mock) EnsureAllPagesUnpinnedAndUnlocked() error {
	b.pagesMu.Lock()
	defer b.pagesMu.Unlock()

	b.pinCountMu.RLock()
	defer b.pinCountMu.RUnlock()

	pinnedIDs := map[common.PageIdentity]int{}
	unpinnedLeaked := map[common.PageIdentity]struct{}{}
	notPinnedPages := map[common.PageIdentity]struct{}{}
	lockedPages := map[common.PageIdentity]struct{}{}

	for pageID, page := range b.pages {
		pinCount, ok := b.pinCounts[pageID]
		if !ok {
			notPinnedPages[pageID] = struct{}{}
		} else if _, ok := b.leakedPages[pageID]; ok {
			if pinCount <= 0 {
				unpinnedLeaked[pageID] = struct{}{}
			}
		} else {
			if pinCount != 0 {
				pinnedIDs[pageID] = pinCount
			}
		}
		if !page.TryLock() {
			lockedPages[pageID] = struct{}{}
		} else {
			page.Unlock()
		}
	}

	var err error
	if len(pinnedIDs) > 0 {
		err = fmt.Errorf(
			"not all pages were properly unpinned: %+v",
			pinnedIDs,
		)
	}

	if len(unpinnedLeaked) > 0 {
		err = errors.Join(err, fmt.Errorf(
			"not all leaked pages were properly unpinned: %+v",
			unpinnedLeaked,
		))
	}

	if len(notPinnedPages) > 0 {
		err = errors.Join(err, fmt.Errorf(
			"found pages in the page table that weren't found in the pinCount table: %+v",
			notPinnedPages,
		))
	}

	if len(lockedPages) > 0 {
		err = errors.Join(err, fmt.Errorf(
			"found pages that were locked and not properly unlocked: %+v",
			lockedPages,
		))
	}

	return err
}
