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
	ChooseVictim() (common.PageIdentity, error) // returns ErrNoVictimAvailable if no victim is available
	GetSize() uint64
}

type BufferPool interface {
	SetLogger(logger common.ITxnLogger)
	Unpin(common.PageIdentity)
	UnpinAssumeLocked(common.PageIdentity)
	GetPage(common.PageIdentity) (*page.SlottedPage, error)
	GetPageAssumeLocked(common.PageIdentity) (*page.SlottedPage, error)
	GetPageNoCreate(common.PageIdentity) (*page.SlottedPage, error)
	GetPageNoCreateAssumeLocked(common.PageIdentity) (*page.SlottedPage, error)
	WithMarkDirty(
		common.TxnID,
		common.PageIdentity,
		*page.SlottedPage,
		func(*page.SlottedPage) (common.LogRecordLocInfo, error),
	) error
	MarkDirtyNoLogsAssumeLocked(common.PageIdentity)
	WithMarkDirtyLogPage(func() (common.LogRecordLocInfo, error)) (common.LogRecordLocInfo, error)
	GetDPTandATT() (map[common.PageIdentity]common.LogRecordLocInfo, map[common.TxnID]common.LogRecordLocInfo)
	FlushAllPages() error
	FlushLogs() error
}

type frameInfo struct {
	frameID  uint64
	pinCount uint64
}

type Manager struct {
	poolSize uint64

	mu          sync.Mutex
	pageTable   map[common.PageIdentity]frameInfo
	frames      []page.SlottedPage
	emptyFrames []uint64

	DPT map[common.PageIdentity]common.LogRecordLocInfo
	ATT map[common.TxnID]common.LogRecordLocInfo

	replacer    Replacer
	diskManager common.DiskManager[*page.SlottedPage]
	logger      common.ITxnLogger
}

func (m *Manager) MarkDirtyNoLogsAssumeLocked(pIdent common.PageIdentity) {
	if _, ok := m.DPT[pIdent]; !ok {
		m.DPT[pIdent] = common.NewNilLogRecordLocation()
	}
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
		mu:          sync.Mutex{},
		pageTable:   map[common.PageIdentity]frameInfo{},
		frames:      make([]page.SlottedPage, poolSize),
		emptyFrames: emptyFrames,
		replacer:    replacer,
		diskManager: diskManager,
		DPT:         map[common.PageIdentity]common.LogRecordLocInfo{},
		ATT:         map[common.TxnID]common.LogRecordLocInfo{},
		logger:      common.DummyLogger(),
	}

	return m
}

func (m *Manager) SetLogger(logger common.ITxnLogger) {
	m.logger = logger
}

var (
	_ BufferPool = &Manager{}
)

func (m *Manager) Unpin(pIdent common.PageIdentity) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.UnpinAssumeLocked(pIdent)
}

func (m *Manager) UnpinAssumeLocked(pIdent common.PageIdentity) {
	frameInfo, ok := m.pageTable[pIdent]
	assert.Assert(ok, "coulnd't unpin page %+v: page not found", pIdent)
	assert.Assert(
		frameInfo.pinCount > 0,
		"invalid pin count for page %+v: %d",
		pIdent,
		frameInfo.pinCount,
	)

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
	requestedPage common.PageIdentity,
) (*page.SlottedPage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if frameInfo, ok := m.pageTable[requestedPage]; ok {
		m.pin(requestedPage)
		return &m.frames[frameInfo.frameID], nil
	}

	frameID := m.reserveFrame()
	if frameID != noFrame {
		page := &m.frames[frameID]
		err := m.diskManager.GetPageNoNew(page, requestedPage)
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

	victimPageIdent, err := m.replacer.ChooseVictim()
	if err != nil {
		if errors.Is(err, ErrNoVictimAvailable) {
			return nil, ErrNoSpaceLeft
		}
		return nil, err
	}

	victimInfo, ok := m.pageTable[victimPageIdent]
	assert.Assert(ok, "victim page %+v not found", victimPageIdent)
	assert.Assert(
		victimInfo.pinCount == 0,
		"victim page %+v is pinned",
		victimPageIdent,
	)

	victimPage := &m.frames[victimInfo.frameID]
	err = m.flushPage(victimPage, victimPageIdent)
	if err != nil {
		m.replacer.Pin(victimPageIdent)
		m.replacer.Unpin(victimPageIdent)
		return nil, err
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

func (m *Manager) GetPageNoCreateAssumeLocked(
	requestedPage common.PageIdentity,
) (*page.SlottedPage, error) {
	if frameInfo, ok := m.pageTable[requestedPage]; ok {
		m.pin(requestedPage)
		return &m.frames[frameInfo.frameID], nil
	}

	frameID := m.reserveFrame()
	if frameID != noFrame {
		page := &m.frames[frameID]
		err := m.diskManager.GetPageNoNewAssumeLocked(page, requestedPage)
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

	victimPageIdent, err := m.replacer.ChooseVictim()
	if err != nil {
		if errors.Is(err, ErrNoVictimAvailable) {
			return nil, ErrNoSpaceLeft
		}
		return nil, err
	}

	victimInfo, ok := m.pageTable[victimPageIdent]
	assert.Assert(ok, "victim page %+v not found", victimPageIdent)
	assert.Assert(
		victimInfo.pinCount == 0,
		"victim page %+v is pinned",
		victimPageIdent,
	)

	victimPage := &m.frames[victimInfo.frameID]
	err = m.flushPageAssumeDiskLocked(victimPage, victimPageIdent)
	if err != nil {
		m.replacer.Pin(victimPageIdent)
		m.replacer.Unpin(victimPageIdent)
		return nil, err
	}
	delete(m.pageTable, victimPageIdent)

	err = m.diskManager.GetPageNoNewAssumeLocked(victimPage, requestedPage)
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
	m.mu.Lock()
	defer m.mu.Unlock()

	if frameInfo, ok := m.pageTable[requestedPage]; ok {
		m.pin(requestedPage)
		return &m.frames[frameInfo.frameID], nil
	}

	frameID := m.reserveFrame()
	if frameID != noFrame {
		page := &m.frames[frameID]
		err := m.diskManager.ReadPage(page, requestedPage)
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

	victimPageIdent, err := m.replacer.ChooseVictim()
	if err != nil {
		if errors.Is(err, ErrNoVictimAvailable) {
			return nil, ErrNoSpaceLeft
		}
		return nil, err
	}

	victimInfo, ok := m.pageTable[victimPageIdent]
	assert.Assert(ok, "victim page %+v not found", victimPageIdent)
	assert.Assert(
		victimInfo.pinCount == 0,
		"victim page %+v is pinned",
		victimPageIdent,
	)

	victimPage := &m.frames[victimInfo.frameID]

	err = m.flushPage(victimPage, victimPageIdent)
	if err != nil {
		m.replacer.Pin(victimPageIdent)
		m.replacer.Unpin(victimPageIdent)
		return nil, err
	}
	delete(m.pageTable, victimPageIdent)

	err = m.diskManager.ReadPage(victimPage, requestedPage)
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

func (m *Manager) GetPageAssumeLocked(
	requestedPage common.PageIdentity,
) (*page.SlottedPage, error) {
	if frameInfo, ok := m.pageTable[requestedPage]; ok {
		m.pin(requestedPage)
		return &m.frames[frameInfo.frameID], nil
	}

	frameID := m.reserveFrame()
	if frameID != noFrame {
		page := &m.frames[frameID]
		err := m.diskManager.ReadPageAssumeLocked(page, requestedPage)
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

	victimPageIdent, err := m.replacer.ChooseVictim()
	if err != nil {
		if errors.Is(err, ErrNoVictimAvailable) {
			return nil, ErrNoSpaceLeft
		}
		return nil, err
	}

	victimInfo, ok := m.pageTable[victimPageIdent]
	assert.Assert(ok, "victim page %+v not found", victimPageIdent)
	assert.Assert(
		victimInfo.pinCount == 0,
		"victim page %+v is pinned",
		victimPageIdent,
	)

	victimPage := &m.frames[victimInfo.frameID]

	err = m.flushPageAssumeDiskLocked(victimPage, victimPageIdent)
	if err != nil {
		m.replacer.Pin(victimPageIdent)
		m.replacer.Unpin(victimPageIdent)
		return nil, err
	}
	delete(m.pageTable, victimPageIdent)

	err = m.diskManager.ReadPageAssumeLocked(victimPage, requestedPage)
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

var ErrNoSpaceLeft = errors.New("no space left in the buffer pool")

func (m *Manager) WithMarkDirty(
	txnID common.TxnID,
	pageIdent common.PageIdentity,
	page *page.SlottedPage,
	fn func(lockedPage *page.SlottedPage) (loc common.LogRecordLocInfo, err error),
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.diskManager.Lock()
	defer m.diskManager.Unlock()

	page.Lock()
	defer page.Unlock()

	loc, err := fn(page)
	if err != nil {
		return err
	}

	if _, ok := m.DPT[pageIdent]; !ok {
		m.DPT[pageIdent] = loc
	}
	if txnID != common.NilTxnID {
		m.ATT[txnID] = loc
	}
	return nil
}

func (m *Manager) WithMarkDirtyLogPage(
	fn func() (common.LogRecordLocInfo, error),
) (common.LogRecordLocInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.diskManager.Lock()
	defer m.diskManager.Unlock()

	loc, err := fn()
	if err != nil {
		return common.LogRecordLocInfo{}, err
	}

	return loc, nil
}

func (m *Manager) reserveFrame() uint64 {
	if len(m.emptyFrames) > 0 {
		id := m.emptyFrames[len(m.emptyFrames)-1]
		m.emptyFrames = m.emptyFrames[:len(m.emptyFrames)-1]
		return id
	}

	return noFrame
}

// WARN: expects **BOTH** buffer pool and diskManager to be locked
func (m *Manager) FlushLogs() error {
	logFileID, startPageID, endPageID, lastLSN := m.logger.GetFlushInfo()
	logPageID := startPageID

	var flush = func(pageID common.PageID) error {
		logPageIdent := common.PageIdentity{
			FileID: logFileID,
			PageID: common.PageID(logPageID),
		}
		if _, ok := m.DPT[logPageIdent]; !ok {
			return nil
		}

		logPageInfo, ok := m.pageTable[logPageIdent]
		assert.Assert(ok, "dirty log page %+v not found", logPageIdent)

		logPage := &m.frames[logPageInfo.frameID]
		err := func() error {
			logPage.Lock()
			defer logPage.Unlock()

			err := m.diskManager.WritePageAssumeLocked(logPage, logPageIdent)
			if err != nil {
				return err
			}

			delete(m.DPT, logPageIdent)
			return nil
		}()
		return err
	}

	for ; logPageID <= endPageID; logPageID++ {
		if err := flush(logPageID); err != nil {
			m.logger.UpdateFirstUnflushedPage(logPageID)
			return err
		}
	}

	m.logger.UpdateFirstUnflushedPage(logPageID)
	m.logger.UpdateFlushLSN(lastLSN)

	if err := flush(common.CheckpointInfoPageID); err != nil {
		return err
	}

	return nil
}

func (m *Manager) flushPageAssumeDiskLocked(
	lockedPg *page.SlottedPage,
	pIdent common.PageIdentity,
) error {
	if _, ok := m.DPT[pIdent]; !ok {
		return nil
	}

	flushLSN := m.logger.GetFlushLSN()
	if lockedPg.PageLSN() > flushLSN {
		if err := m.FlushLogs(); err != nil {
			return err
		}
	}

	err := m.diskManager.WritePageAssumeLocked(lockedPg, pIdent)
	if err != nil {
		return err
	}
	delete(m.DPT, pIdent)
	return nil
}

func (m *Manager) flushPage(lockedPg *page.SlottedPage, pIdent common.PageIdentity) error {
	if _, ok := m.DPT[pIdent]; !ok {
		return nil
	}

	m.diskManager.Lock()
	defer m.diskManager.Unlock()

	flushLSN := m.logger.GetFlushLSN()
	if lockedPg.PageLSN() > flushLSN {
		if err := m.FlushLogs(); err != nil {
			return err
		}
	}

	err := m.diskManager.WritePageAssumeLocked(lockedPg, pIdent)
	if err != nil {
		return err
	}
	delete(m.DPT, pIdent)
	return nil
}

func (m *Manager) FlushAllPages() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.diskManager.Lock()
	defer m.diskManager.Unlock()

	if err := m.FlushLogs(); err != nil {
		return err
	}

	flushLSN := m.logger.GetFlushLSN()

	var err error
	for pgIdent := range m.DPT {
		frameInfo, ok := m.pageTable[pgIdent]
		assert.Assert(ok, "dirty page %+v not found", pgIdent)

		frame := &m.frames[frameInfo.frameID]
		if !frame.TryLock() {
			continue
		}
		assert.Assert(frame.PageLSN() <= flushLSN, "didn't flush logs for page %+v", pgIdent)

		err = errors.Join(err, m.diskManager.WritePageAssumeLocked(frame, pgIdent))
		frame.Unlock()
	}

	clear(m.DPT)
	return err
}

func (m *Manager) GetDPTandATT() (map[common.PageIdentity]common.LogRecordLocInfo, map[common.TxnID]common.LogRecordLocInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return maps.Clone(m.DPT), maps.Clone(m.ATT)
}

type DebugBufferPool struct {
	m           *Manager
	leakedPages map[common.PageIdentity]struct{}
}

func (d *DebugBufferPool) SetLogger(logger common.ITxnLogger) {
	d.m.SetLogger(logger)
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

func (d *DebugBufferPool) FlushLogs() error {
	return d.m.FlushLogs()
}

func (d *DebugBufferPool) GetPageAssumeLocked(
	pIdent common.PageIdentity,
) (*page.SlottedPage, error) {
	return d.m.GetPageAssumeLocked(pIdent)
}

func (d *DebugBufferPool) GetPageNoCreateAssumeLocked(
	pIdent common.PageIdentity,
) (*page.SlottedPage, error) {
	return d.m.GetPageNoCreateAssumeLocked(pIdent)
}

func (d *DebugBufferPool) MarkDirtyNoLogsAssumeLocked(pIdent common.PageIdentity) {
	d.m.MarkDirtyNoLogsAssumeLocked(pIdent)
}

func (d *DebugBufferPool) UnpinAssumeLocked(pIdent common.PageIdentity) {
	d.m.UnpinAssumeLocked(pIdent)
}

func (d *DebugBufferPool) WithMarkDirtyLogPage(
	fn func() (common.LogRecordLocInfo, error),
) (common.LogRecordLocInfo, error) {
	return d.m.WithMarkDirtyLogPage(fn)
}

func (d *DebugBufferPool) FlushAllPages() error {
	return d.m.FlushAllPages()
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
	txnID common.TxnID,
	pIdent common.PageIdentity,
	page *page.SlottedPage,
	fn func(lockedPage *page.SlottedPage) (loc common.LogRecordLocInfo, err error),
) error {
	return d.m.WithMarkDirty(txnID, pIdent, page, fn)
}

func (d *DebugBufferPool) GetDPTandATT() (map[common.PageIdentity]common.LogRecordLocInfo, map[common.TxnID]common.LogRecordLocInfo) {
	return d.m.GetDPTandATT()
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
