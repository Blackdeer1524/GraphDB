package bufferpool

import (
	"errors"
	"fmt"
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

type BufferPool[T Page] interface {
	Unpin(common.PageIdentity) error
	GetPage(common.PageIdentity) (T, error)
	GetPageNoCreate(common.PageIdentity) (T, error)
	FlushPage(common.PageIdentity) error
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

	logfile common.FileID

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

	return &Manager{
		poolSize:    poolSize,
		pageTable:   map[common.PageIdentity]frameInfo{},
		frames:      make([]page.SlottedPage, poolSize),
		emptyFrames: emptyFrames,
		replacer:    replacer,
		diskManager: diskManager,
		fastPath:    sync.Mutex{},
		slowPath:    sync.Mutex{},
		logfile:     0,
	}, nil
}

var (
	_ BufferPool[*page.SlottedPage] = &Manager{}
)

func (m *Manager) Unpin(pIdent common.PageIdentity) error {
	m.fastPath.Lock()
	defer m.fastPath.Unlock()

	frameInfo, ok := m.pageTable[pIdent]
	if !ok {
		return ErrNoSuchPage
	}

	assert.Assert(frameInfo.pinCount > 0, "invalid pin count")

	frameInfo.pinCount--
	m.pageTable[pIdent] = frameInfo
	if frameInfo.pinCount == 0 {
		// TODO: переделать это через ref count?
		m.replacer.Unpin(pIdent)
	}

	return nil
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
			return nil, err
		}
		page.UnsafeInitLatch()

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
		err = m.diskManager.WritePage(
			victimPage,
			victimPageIdent,
		)
		if err != nil {
			return nil, err
		}
	}
	delete(m.pageTable, victimPageIdent)

	page, err := m.diskManager.ReadPage(pIdent)
	if err != nil {
		return nil, err
	}
	page.UnsafeInitLatch()

	m.frames[victimInfo.frameID] = *page
	m.pageTable[pIdent] = frameInfo{
		frameID:  victimInfo.frameID,
		pinCount: 1,
		isDirty:  false,
	}
	m.replacer.Pin(pIdent)
	return page, nil
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
		return fmt.Errorf("no frame for such page: %v", pIdent)
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
		frame.Lock()
		err = errors.Join(err, m.diskManager.WritePage(frame, pgIdent))
		frame.Unlock()

		pgInfo.isDirty = false
		m.pageTable[pgIdent] = pgInfo
	}

	return nil
}
