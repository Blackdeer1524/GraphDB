package bufferpool

import (
	"errors"
	"fmt"
	"maps"
	"sync"

	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
)

const noFrame = ^uint64(0)

type Replacer interface {
	Pin(pageID common.PageIdentity)
	Unpin(pageID common.PageIdentity)
	ChooseVictim() (common.PageIdentity, error)
	GetSize() uint64
}

type BufferPool interface {
	Unpin(common.PageIdentity)
	GetPage(common.PageIdentity) (*page.SlottedPage, error)
	GetPageNoCreate(common.PageIdentity) (*page.SlottedPage, error)
	WithMarkDirty(common.PageIdentity, func() (common.LogRecordLocInfo, error)) error
	GetDirtyPageTable() map[common.PageIdentity]common.LogRecordLocInfo
	FlushPage(common.PageIdentity) error
	FlushAllPages() error
}

type frameInfo struct {
	frameID  uint64
	pinCount uint64
	isDirty  bool
}

type Manager struct {
	poolSize    uint64
	pageTable   map[common.PageIdentity]frameInfo
	frames      []page.SlottedPage
	emptyFrames []uint64

	dirtyPageTable map[common.PageIdentity]common.LogRecordLocInfo

	replacer Replacer

	diskManager common.DiskManager[*page.SlottedPage]

	mu sync.Mutex
}

func New(
	poolSize uint64,
	replacer Replacer,
	diskManager common.DiskManager[*page.SlottedPage],
) *Manager {
	assert.Assert(poolSize > 0, "pool size must be greater than zero")

	emptyFrames := make([]uint64, poolSize)
	for i := range poolSize {
		emptyFrames[i] = uint64(i)
	}

	m := &Manager{
		poolSize:    poolSize,
		pageTable:   map[common.PageIdentity]frameInfo{},
		frames:      make([]page.SlottedPage, poolSize),
		emptyFrames: emptyFrames,
		replacer:    replacer,
		diskManager: diskManager,
		mu:          sync.Mutex{},
	}

	return m
}

var (
	_ BufferPool = &Manager{}
)

func (m *Manager) Unpin(pIdent common.PageIdentity) {
	m.mu.Lock()
	defer m.mu.Unlock()

	frameInfo, ok := m.pageTable[pIdent]
	assert.Assert(ok, "coulnd't unpin page %+v: page not found")
	assert.Assert(frameInfo.pinCount > 0, "invalid pin count")

	frameInfo.pinCount--
	m.pageTable[pIdent] = frameInfo
	if frameInfo.pinCount == 0 {
		m.replacer.Unpin(pIdent)
	}
}

func (m *Manager) pin(pIdent common.PageIdentity) {
	frameInfo, ok := m.pageTable[pIdent]

	assert.Assert(ok, "no frame for page: %v", pIdent)

	frameInfo.pinCount++
	m.pageTable[pIdent] = frameInfo
	m.replacer.Pin(pIdent)
}

func (m *Manager) GetPageNoCreate(
	pIdent common.PageIdentity,
) (*page.SlottedPage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if frameInfo, ok := m.pageTable[pIdent]; ok {
		m.pin(pIdent)
		return &m.frames[frameInfo.frameID], nil
	}

	frameID := m.reserveFrame()
	if frameID != noFrame {
		page := &m.frames[frameID]
		err := m.diskManager.GetPageNoNew(page, pIdent)
		if err != nil {
			m.emptyFrames = append(m.emptyFrames, frameID)
			return nil, err
		}

		m.pageTable[pIdent] = frameInfo{
			frameID:  frameID,
			pinCount: 1,
			isDirty:  false,
		}
		m.replacer.Pin(pIdent)

		return page, nil
	}

	victimPageIdent, err := m.replacer.ChooseVictim()
	if err != nil {
		return nil, err
	}

	victimInfo, ok := m.pageTable[victimPageIdent]
	assert.Assert(ok)

	victimPage := &m.frames[victimInfo.frameID]
	if victimInfo.isDirty {
		err = m.diskManager.WritePage(victimPage, victimPageIdent)
		if err != nil {
			return nil, err
		}
	}
	delete(m.pageTable, victimPageIdent)

	page := &m.frames[victimInfo.frameID]
	err = m.diskManager.ReadPage(page, pIdent)
	if err != nil {
		return nil, err
	}

	m.pageTable[pIdent] = frameInfo{
		frameID:  victimInfo.frameID,
		pinCount: 1,
		isDirty:  false,
	}
	m.replacer.Pin(pIdent)
	return page, nil
}

func (m *Manager) GetPage(
	pIdent common.PageIdentity,
) (*page.SlottedPage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if frameInfo, ok := m.pageTable[pIdent]; ok {
		m.pin(pIdent)
		return &m.frames[frameInfo.frameID], nil
	}

	frameID := m.reserveFrame()
	if frameID != noFrame {
		page := &m.frames[frameID]
		err := m.diskManager.ReadPage(page, pIdent)
		if err != nil {
			m.emptyFrames = append(m.emptyFrames, frameID)
			return nil, err
		}

		m.pageTable[pIdent] = frameInfo{
			frameID:  frameID,
			pinCount: 1,
			isDirty:  false,
		}
		m.replacer.Pin(pIdent)

		return page, nil
	}

	victimPageIdent, err := m.replacer.ChooseVictim()
	if err != nil {
		return nil, err
	}

	victimInfo, ok := m.pageTable[victimPageIdent]
	assert.Assert(ok)

	victimPage := &m.frames[victimInfo.frameID]
	if victimInfo.isDirty {
		err = m.diskManager.WritePage(victimPage, victimPageIdent)
		if err != nil {
			return nil, err
		}
	}
	delete(m.pageTable, victimPageIdent)

	page := &m.frames[victimInfo.frameID]
	err = m.diskManager.ReadPage(page, pIdent)
	if err != nil {
		return nil, err
	}

	m.pageTable[pIdent] = frameInfo{
		frameID:  victimInfo.frameID,
		pinCount: 1,
		isDirty:  false,
	}
	m.replacer.Pin(pIdent)
	return page, nil
}

func (m *Manager) WithMarkDirty(
	pageIdent common.PageIdentity,
	fn func() (common.LogRecordLocInfo, error),
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	loc, err := fn()
	if err != nil {
		return err
	}

	frameInfo, ok := m.pageTable[pageIdent]
	assert.Assert(
		ok,
		"couldn't mark page %+v as dirty: page not found",
		pageIdent,
	)
	frameInfo.isDirty = true
	m.pageTable[pageIdent] = frameInfo
	if _, ok := m.dirtyPageTable[pageIdent]; !ok {
		m.dirtyPageTable[pageIdent] = loc
	}

	return nil
}

func (m *Manager) reserveFrame() uint64 {
	if len(m.emptyFrames) > 0 {
		id := m.emptyFrames[len(m.emptyFrames)-1]
		m.emptyFrames = m.emptyFrames[:len(m.emptyFrames)-1]
		return id
	}

	return noFrame
}

func (m *Manager) FlushPage(pIdent common.PageIdentity) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	frameInfo, ok := m.pageTable[pIdent]
	if !ok {
		return fmt.Errorf("no such page: %+v", pIdent)
	}

	if !frameInfo.isDirty {
		return nil
	}

	frame := &m.frames[frameInfo.frameID]
	err := m.diskManager.WritePage(frame, pIdent)
	if err != nil {
		return fmt.Errorf("failed to write page to disk: %w", err)
	}

	frameInfo.isDirty = false
	m.pageTable[pIdent] = frameInfo
	return nil
}

func (m *Manager) FlushAllPages() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var err error
	for pgIdent, pgInfo := range m.pageTable {
		if !pgInfo.isDirty {
			continue
		}

		frame := &m.frames[pgInfo.frameID]
		if !frame.TryLock() {
			continue
		}
		err = errors.Join(err, m.diskManager.WritePage(frame, pgIdent))
		frame.Unlock()
		pgInfo.isDirty = false
		m.pageTable[pgIdent] = pgInfo

		delete(m.dirtyPageTable, pgIdent)
	}

	return err
}

func (m *Manager) GetDirtyPageTable() map[common.PageIdentity]common.LogRecordLocInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	return maps.Clone(m.dirtyPageTable)
}

type DebugBufferPool struct {
	m           *Manager
	leakedPages map[common.PageIdentity]struct{}
}

var (
	_ BufferPool = &DebugBufferPool{}
)

func NewDebugBufferPool(
	m *Manager,
	leakedPages map[common.PageIdentity]struct{},
) *DebugBufferPool {
	return &DebugBufferPool{m: m, leakedPages: leakedPages}
}

func (d *DebugBufferPool) FlushAllPages() error {
	return d.m.FlushAllPages()
}

func (d *DebugBufferPool) FlushPage(pIdent common.PageIdentity) error {
	return d.m.FlushPage(pIdent)
}

func (d *DebugBufferPool) GetDirtyPageTable() map[common.PageIdentity]common.LogRecordLocInfo {
	return d.m.GetDirtyPageTable()
}

func (d *DebugBufferPool) GetPage(pIdent common.PageIdentity) (*page.SlottedPage, error) {
	return d.m.GetPage(pIdent)
}

func (d *DebugBufferPool) GetPageNoCreate(pIdent common.PageIdentity) (*page.SlottedPage, error) {
	return d.m.GetPageNoCreate(pIdent)
}

func (d *DebugBufferPool) Unpin(pIdent common.PageIdentity) {
	d.m.Unpin(pIdent)
}

func (d *DebugBufferPool) WithMarkDirty(
	pIdent common.PageIdentity,
	fn func() (common.LogRecordLocInfo, error),
) error {
	return d.m.WithMarkDirty(pIdent, fn)
}

func (d *DebugBufferPool) EnsureAllPagesUnpinnedAndUnlocked() error {
	d.m.mu.Lock()
	defer d.m.mu.Unlock()

	pinnedIDs := map[common.PageIdentity]uint64{}
	unpinnedLeaked := map[common.PageIdentity]struct{}{}
	notPinnedPages := map[common.PageIdentity]struct{}{}
	lockedPages := map[common.PageIdentity]struct{}{}

	for pageID, pageInfo := range d.m.pageTable {
		pinCount := pageInfo.pinCount
		if _, ok := d.leakedPages[pageID]; ok {
			if pinCount <= 0 {
				unpinnedLeaked[pageID] = struct{}{}
			}
		} else {
			if pinCount != 0 {
				pinnedIDs[pageID] = pinCount
			}
		}
		page := &d.m.frames[pageInfo.frameID]
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
