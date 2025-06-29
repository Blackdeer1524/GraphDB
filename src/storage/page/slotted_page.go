package page

import (
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"

	assert "github.com/Blackdeer1524/GraphDB/src/pkg/assert"
)

var (
	ErrNoEnoughSpace = errors.New("not enough space")
	ErrInvalidSlotID = errors.New("invalid slot ID")
)

// TODO добавить рисунок - иллюстрацию

const (
	Size       = 4096
	HeaderSize = uint16(
		unsafe.Sizeof(uint16(0)),
	) * 3 // numSlots (4) + freeStart (4) + freeEnd (4)
	SlotSize = uint16(unsafe.Sizeof(uint16(0))) * 2 // offset (2) + length (2)
)

type SlottedPage struct {
	data []byte

	locked atomic.Bool
	latch  sync.RWMutex

	dirty atomic.Bool

	fileID uint64
	pageID uint64
}

func NewSlottedPage(fileID, pageID uint64) *SlottedPage {
	p := &SlottedPage{
		data:   make([]byte, Size),
		fileID: fileID,
		pageID: pageID,
	}

	p.setNumSlots(0)
	p.setFreeStart(HeaderSize)
	p.setFreeEnd(Size)

	return p
}

func (p *SlottedPage) NumSlots() uint16 {
	return binary.LittleEndian.Uint16(p.data[0:2])
}

func (p *SlottedPage) setNumSlots(n uint16) {
	binary.LittleEndian.PutUint16(p.data[0:2], n)
}

func (p *SlottedPage) freeStart() uint16 {
	return binary.LittleEndian.Uint16(p.data[2:4])
}

func (p *SlottedPage) setFreeStart(n uint16) {
	binary.LittleEndian.PutUint16(p.data[2:4], n)
}

func (p *SlottedPage) freeEnd() uint16 {
	return binary.LittleEndian.Uint16(p.data[4:6])
}

func (p *SlottedPage) setFreeEnd(n uint16) {
	binary.LittleEndian.PutUint16(p.data[4:6], n)
}

func (p *SlottedPage) getSlot(i uint16) (offset, length uint16) {
	base := HeaderSize + i*SlotSize

	offset = binary.LittleEndian.Uint16(p.data[base : base+2])
	length = binary.LittleEndian.Uint16(p.data[base+2 : base+4])

	return
}

func (p *SlottedPage) setSlot(i, offset, length uint16) {
	base := HeaderSize + i*SlotSize
	binary.LittleEndian.PutUint16(p.data[base:base+2], offset)
	binary.LittleEndian.PutUint16(p.data[base+2:base+4], length)
}

// returns an error ONLY IF there is no free space left
func (p *SlottedPage) Insert(record []byte) (slotID uint16, err error) {
	defer func() { assert.Assert(err == nil || errors.Is(err, ErrNoEnoughSpace), "contract violation") }()

	//nolint:gosec
	recLen := uint16(len(record))
	freeSpace := p.freeEnd() - p.freeStart()

	if freeSpace < recLen+SlotSize {
		return 0, ErrNoEnoughSpace
	}

	// Allocate space for the record
	newOffset := p.freeEnd() - recLen
	copy(p.data[newOffset:], record)

	// Create slot
	slotID = p.NumSlots()
	p.setSlot(slotID, newOffset, recLen)

	// Update header
	p.setNumSlots(slotID + 1)
	p.setFreeEnd(newOffset)
	p.setFreeStart(HeaderSize + (slotID+1)*SlotSize)

	return slotID, nil
}

func (p *SlottedPage) Get(slotID uint16) ([]byte, error) {
	if slotID >= p.NumSlots() {
		return nil, ErrInvalidSlotID
	}

	offset, length := p.getSlot(slotID)

	return p.data[offset : offset+length], nil
}

func (p *SlottedPage) GetData() []byte {
	assert.Assert(!p.locked.Load(), "GetData contract is violated")

	return p.data
}

func (p *SlottedPage) Lock() {
	p.latch.Lock()

	p.locked.Store(true)
}

func (p *SlottedPage) Unlock() {
	p.locked.Store(false)

	p.latch.Unlock()
}

func (p *SlottedPage) RLock() {
	p.latch.RLock()

	p.locked.Store(true)
}

func (p *SlottedPage) RUnlock() {
	p.locked.Store(false)

	p.latch.RUnlock()
}

func (p *SlottedPage) SetDirtiness(val bool) {
	p.dirty.Store(val)
}

func (p *SlottedPage) GetFileID() uint64 {
	return p.fileID
}

func (p *SlottedPage) GetPageID() uint64 {
	return p.pageID
}

func (p *SlottedPage) IsDirty() bool {
	return p.dirty.Load()
}

func (p *SlottedPage) SetData(d []byte) {
	assert.Assert(p.locked.Load(), "SetData contract is violated")

	clear(p.data)
	copy(p.data, d)
}
