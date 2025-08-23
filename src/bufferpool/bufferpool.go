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

var ErrNoSuchPage = errors.New("no such page")

type Page interface {
	GetData() []byte
	SetData(d []byte)

	// latch methods
	Lock()
	Unlock()
	RLock()
	RUnlock()
}

var (
	_ Page = &page.SlottedPage{}
)

type Replacer interface {
	Pin(pageID common.PageIdentity)
	Unpin(pageID common.PageIdentity)
	ChooseVictim() (common.PageIdentity, error)
	GetSize() uint64
}

type DiskManager[T Page] interface {
	ReadPage(pageIdent common.PageIdentity) (T, error)
	WritePage(page T, pageIdent common.PageIdentity) error
}

type BufferPool interface {
	Unpin(common.PageIdentity)
	GetPage(common.PageIdentity) (*page.SlottedPage, error)
	GetPageNoCreate(common.PageIdentity) (*page.SlottedPage, error)
	WithMarkDirty(
		common.PageIdentity,
		func() (common.LogRecordLocInfo, error),
	) error
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

	diskManager DiskManager[*page.SlottedPage]

	fastPath sync.Mutex
	slowPath sync.Mutex
}

func New(
	poolSize uint64,
	replacer Replacer,
	diskManager DiskManager[*page.SlottedPage],
) (*Manager, error) {
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
		fastPath:    sync.Mutex{},
		slowPath:    sync.Mutex{},
	}

	return m, nil
}

var (
	_ BufferPool = &Manager{}
)

func (m *Manager) Unpin(pIdent common.PageIdentity) {
	m.fastPath.Lock()
	defer m.fastPath.Unlock()

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
	// WARN: m has to locked!
	frameInfo, ok := m.pageTable[pIdent]

	assert.Assert(ok, "no frame for page: %v", pIdent)

	frameInfo.pinCount++
	m.pageTable[pIdent] = frameInfo
	m.replacer.Pin(pIdent)
}

func (m *Manager) GetPageNoCreate(
	pageID common.PageIdentity,
) (*page.SlottedPage, error) {
	panic("NOT IMPLEMENTED")
}

func (m *Manager) GetPage(
	pIdent common.PageIdentity,
) (*page.SlottedPage, error) {
	m.fastPath.Lock()

	if frameInfo, ok := m.pageTable[pIdent]; ok {
		m.pin(pIdent)
		m.fastPath.Unlock()

		return &m.frames[frameInfo.frameID], nil
	}

	m.fastPath.Unlock()

	m.slowPath.Lock()
	defer m.slowPath.Unlock()

	m.fastPath.Lock()
	if frameInfo, ok := m.pageTable[pIdent]; ok {
		m.pin(pIdent)
		m.fastPath.Unlock()

		return &m.frames[frameInfo.frameID], nil
	}
	m.fastPath.Unlock()

	frameID := m.reserveFrame()
	if frameID != noFrame {
		page, err := m.diskManager.ReadPage(pIdent)
		if err != nil {
			m.fastPath.Lock()
			m.emptyFrames = append(m.emptyFrames, frameID)
			m.fastPath.Unlock()
			return nil, err
		}

		m.frames[frameID] = *page
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

	page, err := m.diskManager.ReadPage(pIdent)
	if err != nil {
		return nil, err
	}

	m.frames[victimInfo.frameID] = *page
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
	m.fastPath.Lock()
	defer m.fastPath.Unlock()

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
	m.fastPath.Lock()
	defer m.fastPath.Unlock()

	if len(m.emptyFrames) > 0 {
		id := m.emptyFrames[len(m.emptyFrames)-1]
		m.emptyFrames = m.emptyFrames[:len(m.emptyFrames)-1]
		return id
	}

	return noFrame
}

func (m *Manager) FlushPage(pIdent common.PageIdentity) error {
	m.fastPath.Lock()
	defer m.fastPath.Unlock()

	frameInfo, ok := m.pageTable[pIdent]
	if !ok {
		return ErrNoSuchPage
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
	m.fastPath.Lock()
	defer m.fastPath.Unlock()

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
	m.fastPath.Lock()
	defer m.fastPath.Unlock()
	return maps.Clone(m.dirtyPageTable)
}
