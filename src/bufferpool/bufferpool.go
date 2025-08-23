package bufferpool

import (
	"errors"
	"fmt"
	"github.com/petermattis/goid"
	"log"
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
	UnpinAssumeLocked(common.PageIdentity)
	GetPage(common.PageIdentity) (*page.SlottedPage, error)
	GetPageAssumeLocked(common.PageIdentity) (*page.SlottedPage, error)
	GetPageNoCreate(common.TxnID, common.PageIdentity) (*page.SlottedPage, error)
	GetPageNoCreateAssumeLocked(common.TxnID, common.PageIdentity) (*page.SlottedPage, error)
	WithMarkDirty(common.PageIdentity, func() (common.LogRecordLocInfo, error)) error
	GetDirtyPageTable() map[common.PageIdentity]common.LogRecordLocInfo
	FlushPage(common.PageIdentity) error
	FlushAllPages() error
}

type frameInfo struct {
	frameID  uint64
	pinCount uint64
}

type Manager struct {
	poolSize    uint64
	pageTable   map[common.PageIdentity]frameInfo
	frames      []page.SlottedPage
	emptyFrames []uint64

	DPT map[common.PageIdentity]common.LogRecordLocInfo

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
		DPT:         map[common.PageIdentity]common.LogRecordLocInfo{},
	}

	return m
}

var (
	_ BufferPool = &Manager{}
)

func (m *Manager) Unpin(pIdent common.PageIdentity) {
	log.Printf("goid=%d pool unpin before lock", goid.Get())

	m.mu.Lock()
	defer m.mu.Unlock()

	log.Printf("goid=%d pool unpin after lock", goid.Get())

	m.UnpinAssumeLocked(pIdent)
}

func (m *Manager) UnpinAssumeLocked(pIdent common.PageIdentity) {
	log.Printf("goid=%d UnpinAssumeLocked before assert", goid.Get())

	frameInfo, ok := m.pageTable[pIdent]
	assert.Assert(ok, "coulnd't unpin page %+v: page not found")
	assert.Assert(frameInfo.pinCount > 0, "invalid pin count")

	log.Printf("goid=%d UnpinAssumeLocked after assert", goid.Get())

	frameInfo.pinCount--
	m.pageTable[pIdent] = frameInfo
	if frameInfo.pinCount == 0 {
		log.Printf("goid=%d UnpinAssumeLocked before Unpin", goid.Get())

		m.replacer.Unpin(pIdent)
	}

	log.Printf("goid=%d UnpinAssumeLocked end", goid.Get())
}

func (m *Manager) pin(txID common.TxnID, pIdent common.PageIdentity) {
	frameInfo, ok := m.pageTable[pIdent]

	assert.Assert(ok, "no frame for page: %v", pIdent)

	frameInfo.pinCount++
	m.pageTable[pIdent] = frameInfo
	m.replacer.Pin(pIdent)
}

func (m *Manager) GetPageNoCreate(
	txID common.TxnID,
	requestedPage common.PageIdentity,
) (*page.SlottedPage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.GetPageNoCreateAssumeLocked(txID, requestedPage)
}

func (m *Manager) GetPageNoCreateAssumeLocked(
	txID common.TxnID,
	requestedPage common.PageIdentity,
) (*page.SlottedPage, error) {
	if frameInfo, ok := m.pageTable[requestedPage]; ok {
		m.pin(txID, requestedPage)

		return &m.frames[frameInfo.frameID], nil
	}

	frameID := m.reserveFrame()
	if frameID != noFrame {
		p := &m.frames[frameID]

		err := m.diskManager.GetPageNoNew(p, requestedPage)
		if err != nil {
			m.emptyFrames = append(m.emptyFrames, frameID)
			return nil, err
		}

		m.pageTable[requestedPage] = frameInfo{
			frameID:  frameID,
			pinCount: 1,
		}
		m.replacer.Pin(requestedPage)

		return p, nil
	}

	victimPageIdent, err := m.replacer.ChooseVictim()
	if err != nil {
		return nil, err
	}

	victimInfo, ok := m.pageTable[victimPageIdent]
	assert.Assert(ok, "victim page %+v not found", victimPageIdent)
	assert.Assert(victimInfo.pinCount == 0, "victim page %+v is pinned", victimPageIdent)

	victimPage := &m.frames[victimInfo.frameID]
	if _, ok := m.DPT[victimPageIdent]; ok {
		err = m.diskManager.WritePage(victimPage, victimPageIdent)
		if err != nil {
			m.replacer.Pin(victimPageIdent)
			m.replacer.Unpin(victimPageIdent)
			return nil, err
		}
		delete(m.DPT, victimPageIdent)
	}
	delete(m.pageTable, victimPageIdent)

	err = m.diskManager.GetPageNoNew(victimPage, requestedPage)
	if err != nil {
		m.emptyFrames = append(m.emptyFrames, victimInfo.frameID)
		return nil, err
	}

	m.pageTable[requestedPage] = frameInfo{
		frameID:  victimInfo.frameID,
		pinCount: 1,
	}
	m.replacer.Pin(requestedPage)

	return victimPage, nil
}

func (m *Manager) GetPage(
	requestedPage common.PageIdentity,
) (*page.SlottedPage, error) {
	log.Printf("goid=%d pool getpage start", goid.Get())

	m.mu.Lock()
	defer m.mu.Unlock()

	log.Printf("goid=%d pool getpage", goid.Get())

	return m.GetPageAssumeLocked(requestedPage)
}

func (m *Manager) GetPageAssumeLocked(
	requestedPage common.PageIdentity,
) (*page.SlottedPage, error) {
	log.Printf("goid=%d GetPageAssumeLocked pagetable getting", goid.Get())

	if frameInfo, ok := m.pageTable[requestedPage]; ok {
		m.pin(0, requestedPage)
		return &m.frames[frameInfo.frameID], nil
	}

	log.Printf("goid=%d GetPageAssumeLocked before reserve frame", goid.Get())

	frameID := m.reserveFrame()
	if frameID != noFrame {
		log.Printf("goid=%d GetPageAssumeLocked no noFrame", goid.Get())

		page := &m.frames[frameID]

		log.Printf("goid=%d GetPageAssumeLocked before ReadPage", goid.Get())

		err := m.diskManager.ReadPage(page, requestedPage)

		log.Printf("goid=%d GetPageAssumeLocked after ReadPage", goid.Get())

		if err != nil {
			m.emptyFrames = append(m.emptyFrames, frameID)
			return nil, err
		}

		m.pageTable[requestedPage] = frameInfo{
			frameID:  frameID,
			pinCount: 1,
		}
		m.replacer.Pin(requestedPage)

		return page, nil
	}

	log.Printf("goid=%d GetPageAssumeLocked before ChooseVictim", goid.Get())

	victimPageIdent, err := m.replacer.ChooseVictim()
	if err != nil {
		return nil, err
	}

	log.Printf("goid=%d GetPageAssumeLocked after ReadPage", goid.Get())

	victimInfo, ok := m.pageTable[victimPageIdent]
	assert.Assert(ok, "victim page %+v not found", victimPageIdent)
	assert.Assert(victimInfo.pinCount == 0, "victim page %+v is pinned", victimPageIdent)

	log.Printf("goid=%d GetPageAssumeLocked after assert", goid.Get())

	victimPage := &m.frames[victimInfo.frameID]
	if _, ok := m.DPT[victimPageIdent]; ok {
		err = m.diskManager.WritePage(victimPage, victimPageIdent)
		if err != nil {
			m.replacer.Pin(victimPageIdent)
			m.replacer.Unpin(victimPageIdent)
			return nil, err
		}
		delete(m.DPT, victimPageIdent)
	}
	delete(m.pageTable, victimPageIdent)

	log.Printf("goid=%d GetPageAssumeLocked before ReadPage", goid.Get())

	err = m.diskManager.ReadPage(victimPage, requestedPage)
	if err != nil {
		m.emptyFrames = append(m.emptyFrames, victimInfo.frameID)
		return nil, err
	}

	log.Printf("goid=%d GetPageAssumeLocked after ReadPage", goid.Get())

	m.pageTable[requestedPage] = frameInfo{
		frameID:  victimInfo.frameID,
		pinCount: 1,
	}

	log.Printf("goid=%d GetPageAssumeLocked before Pin", goid.Get())

	m.replacer.Pin(requestedPage)

	log.Printf("goid=%d GetPageAssumeLocked after Pin", goid.Get())

	return victimPage, nil
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

	if _, ok := m.DPT[pageIdent]; !ok {
		m.DPT[pageIdent] = loc
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

	if _, ok := m.DPT[pIdent]; !ok {
		return nil
	}

	frame := &m.frames[frameInfo.frameID]
	err := m.diskManager.WritePage(frame, pIdent)
	if err != nil {
		return fmt.Errorf("failed to write page to disk: %w", err)
	}

	delete(m.DPT, pIdent)
	return nil
}

func (m *Manager) FlushAllPages() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var err error
	for pgIdent, pgInfo := range m.pageTable {
		if _, ok := m.DPT[pgIdent]; !ok {
			continue
		}

		frame := &m.frames[pgInfo.frameID]
		if !frame.TryLock() {
			continue
		}
		err = errors.Join(err, m.diskManager.WritePage(frame, pgIdent))
		frame.Unlock()
		delete(m.DPT, pgIdent)
	}

	return err
}

func (m *Manager) GetDirtyPageTable() map[common.PageIdentity]common.LogRecordLocInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	return maps.Clone(m.DPT)
}

type DebugBufferPool struct {
	m           *Manager
	leakedPages map[common.PageIdentity]struct{}
}

var (
	_ BufferPool = &DebugBufferPool{}
)

func (d *DebugBufferPool) GetPageAssumeLocked(
	pIdent common.PageIdentity,
) (*page.SlottedPage, error) {
	return d.m.GetPageAssumeLocked(pIdent)
}

func (d *DebugBufferPool) GetPageNoCreateAssumeLocked(
	id common.TxnID,
	pIdent common.PageIdentity,
) (*page.SlottedPage, error) {
	return d.m.GetPageNoCreateAssumeLocked(id, pIdent)
}

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

func (d *DebugBufferPool) GetPageNoCreate(txID common.TxnID, pIdent common.PageIdentity) (*page.SlottedPage, error) {
	return d.m.GetPageNoCreate(txID, pIdent)
}

func (d *DebugBufferPool) Unpin(pIdent common.PageIdentity) {
	d.m.Unpin(pIdent)
}

func (d *DebugBufferPool) UnpinAssumeLocked(pIdent common.PageIdentity) {
	d.m.UnpinAssumeLocked(pIdent)
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
