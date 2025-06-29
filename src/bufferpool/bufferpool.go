package bufferpool

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
)

const noFrame = ^uint64(0)

var ErrNoSuchPage = errors.New("no such page")

type LSN uint64

const NilLSN = LSN(0)

type Page interface {
	GetData() []byte
	SetData(d []byte)

	SetDirtiness(val bool)
	IsDirty() bool

	GetFileID() uint64
	GetPageID() uint64

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
	Pin(frameID uint64)
	Unpin(frameID uint64)
	ChooseVictim() (uint64, error)
	GetSize() uint64
}

type DiskManager[T Page] interface {
	ReadPage(fileID, pageID uint64) (T, error)
	WritePage(page T) error
}

type frame[T Page] struct {
	Page     T
	Idx      uint64
	PinCount int
	FileID   uint64
	PageID   uint64
}

type PageIdentity struct {
	FileID uint64
	PageID uint64
}

func (p PageIdentity) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.BigEndian, p.FileID)
	_ = binary.Write(buf, binary.BigEndian, p.PageID)

	return buf.Bytes(), nil
}

func (p *PageIdentity) UnmarshalBinary(data []byte) error {
	rd := bytes.NewReader(data)
	if err := binary.Read(rd, binary.BigEndian, &p.FileID); err != nil {
		return err
	}

	return binary.Read(rd, binary.BigEndian, &p.PageID)
}

type BufferPool[T Page] interface {
	Unpin(PageIdentity) error
	GetPage(PageIdentity) (T, error)
	GetPageNoCreate(PageIdentity) (T, error)
	FlushPage(PageIdentity) error
}

type Manager[T Page] struct {
	poolSize    uint64
	pageToFrame map[PageIdentity]uint64
	frames      []frame[T]
	emptyFrames []uint64

	replacer Replacer

	diskManager    DiskManager[T]
	DirtyPageTable map[PageIdentity]LSN

	fastPath sync.Mutex
	slowPath sync.Mutex
}

var (
	_ BufferPool[Page] = &Manager[Page]{}
)

func New[T Page](
	poolSize uint64,
	replacer Replacer,
	diskManager DiskManager[T],
) (*Manager[T], error) {
	assert.Assert(poolSize > 0, "pool size must be greater than zero")

	emptyFrames := make([]uint64, poolSize)
	for i := range poolSize {
		emptyFrames[i] = uint64(i)
	}

	frames := make([]frame[T], poolSize)

	return &Manager[T]{
		poolSize:       poolSize,
		pageToFrame:    make(map[PageIdentity]uint64),
		frames:         frames,
		emptyFrames:    emptyFrames,
		replacer:       replacer,
		diskManager:    diskManager,
		DirtyPageTable: make(map[PageIdentity]LSN),
	}, nil
}

func (m *Manager[T]) Unpin(pIdent PageIdentity) error {
	m.fastPath.Lock()
	defer m.fastPath.Unlock()

	frameID, ok := m.pageToFrame[pIdent]
	if !ok {
		return ErrNoSuchPage
	}

	m.unpinFrame(frameID)

	return nil
}

func (m *Manager[T]) unpinFrame(frameID uint64) {
	frame := &m.frames[frameID]

	assert.Assert(frame.PinCount > 0, "invalid pin count")

	frame.PinCount--
	if frame.PinCount == 0 {
		m.replacer.Unpin(frameID)
	}
}

func (m *Manager[T]) pin(pIdent PageIdentity) {
	frameID, ok := m.pageToFrame[pIdent]

	assert.Assert(ok, "no frame for page: %v", pIdent)

	m.frames[frameID].PinCount++
	m.replacer.Pin(frameID)
}

func (m *Manager[T]) GetPageNoCreate(pageID PageIdentity) (T, error) {
	panic("NOT IMPLEMENTED")
}

func (m *Manager[T]) GetPage(pIdent PageIdentity) (T, error) {
	m.fastPath.Lock()

	if frameID, ok := m.pageToFrame[pIdent]; ok {
		m.pin(pIdent)
		m.fastPath.Unlock()

		return m.frames[frameID].Page, nil
	}

	m.fastPath.Unlock()

	m.slowPath.Lock()
	defer m.slowPath.Unlock()

	m.fastPath.Lock()
	if frameID, ok := m.pageToFrame[pIdent]; ok {
		m.pin(pIdent)
		m.fastPath.Unlock()

		return m.frames[frameID].Page, nil
	}
	m.fastPath.Unlock()

	frameID := m.reserveFrame()
	if frameID != noFrame {
		page, err := m.diskManager.ReadPage(pIdent.FileID, pIdent.PageID)
		if err != nil {
			var zero T
			return zero, err
		}

		m.frames[frameID] = frame[T]{
			Page:     page,
			PinCount: 1,
			FileID:   pIdent.FileID,
			PageID:   pIdent.PageID,
		}
		m.pageToFrame[pIdent] = frameID

		return page, nil
	}

	victimFrameID, err := m.replacer.ChooseVictim()
	if err != nil {
		var zero T
		return zero, err
	}

	victimFrame := m.frames[victimFrameID]
	if victimFrame.Page.IsDirty() {
		err = m.diskManager.WritePage(victimFrame.Page)
		if err != nil {
			var zero T
			return zero, err
		}

		delete(m.DirtyPageTable, pIdent)
	}

	oldIdent := PageIdentity{
		FileID: victimFrame.FileID,
		PageID: victimFrame.PageID,
	}

	delete(m.pageToFrame, oldIdent)

	page, err := m.diskManager.ReadPage(pIdent.FileID, pIdent.PageID)
	if err != nil {
		var zero T
		return zero, err
	}

	m.frames[victimFrameID] = frame[T]{
		Page:     page,
		PinCount: 1,
		FileID:   pIdent.FileID,
		PageID:   pIdent.PageID,
	}

	m.pageToFrame[pIdent] = victimFrameID

	m.pin(pIdent)

	return page, nil
}

func (m *Manager[T]) reserveFrame() uint64 {
	m.fastPath.Lock()
	defer m.fastPath.Unlock()

	if len(m.emptyFrames) > 0 {
		id := m.emptyFrames[0]
		m.emptyFrames = m.emptyFrames[1:]
		m.frames[id].PinCount = 1
		m.replacer.Pin(id)

		return id
	}

	return noFrame
}

func (m *Manager[T]) FlushPage(pIdent PageIdentity) error {
	m.fastPath.Lock()
	defer m.fastPath.Unlock()

	frameID, ok := m.pageToFrame[pIdent]
	if !ok {
		return fmt.Errorf("no frame for such page: %v", pIdent)
	}

	frame := &m.frames[frameID]
	if !frame.Page.IsDirty() {
		return nil
	}

	err := m.diskManager.WritePage(frame.Page)
	if err != nil {
		return fmt.Errorf("failed to write page to disk: %w", err)
	}

	delete(m.DirtyPageTable, pIdent)

	frame.Page.SetDirtiness(false)

	return nil
}

func (m *Manager[T]) FlushAllPages() error {
	m.fastPath.Lock()
	defer m.fastPath.Unlock()

	for i := range m.frames {
		frame := &m.frames[i]
		if frame.Page.IsDirty() {
			_ = m.diskManager.WritePage(frame.Page)

			frame.Page.SetDirtiness(false)

			delete(m.DirtyPageTable, PageIdentity{
				FileID: frame.FileID,
				PageID: frame.PageID,
			})
		}
	}

	return nil
}
