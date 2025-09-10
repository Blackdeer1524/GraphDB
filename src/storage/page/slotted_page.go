package page

import (
	"encoding"
	"errors"
	"sync"
	"unsafe"

	assert "github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/optional"
)

// TODO добавить рисунок - иллюстрацию

const (
	slotOffsetSize        = 12
	PageSize              = (1 << slotOffsetSize)
	slotOffsetMask uint16 = PageSize - 1
	slotPtrSize    uint16 = uint16(unsafe.Sizeof(slotPointer(1)))
)

func PageCapacity(recordLen int) int {
	requiredSpace := unsafe.Sizeof(uint16(0)) + uintptr(recordLen)

	slotsOffset := unsafe.Offsetof(header{}.slots)
	test := (PageSize - slotsOffset) / (uintptr(slotPtrSize) + requiredSpace)
	return int(test)
}

type SlottedPage struct {
	data [PageSize]byte
}

var (
	_ common.Page = &SlottedPage{}
)

func (p *SlottedPage) UnsafeInitLatch() {
	p.getHeader().latch = sync.RWMutex{}
}

type slotPointer uint16
type slotStatus byte

const (
	SlotStatusPrepareInsert slotStatus = iota
	SlotStatusInserted
	SlotStatusDeleted
)

func (st slotStatus) String() string {
	return []string{
		"PrepareInsert",
		"Inserted",
		"Deleted",
	}[st]
}

func newSlotPtr(status slotStatus, recordOffset uint16) slotPointer {
	assert.Assert(recordOffset <= slotOffsetMask, "the offset is too big")
	return slotPointer((uint16(status) << slotOffsetSize) | recordOffset)
}

func (s slotPointer) RecordOffset() uint16 {
	return uint16(s) & slotOffsetMask
}

func (s slotPointer) slotInfo() slotStatus {
	res := (uint16(s) & (^slotOffsetMask)) >> slotOffsetSize
	return slotStatus(res)
}

type header struct {
	latch sync.RWMutex

	pageLSN common.LSN

	freeStart uint16
	freeEnd   uint16

	slotsCount uint16
	slots      slotPointer
}

func (h *header) getSlots() []slotPointer {
	return unsafe.Slice(&h.slots, h.slotsCount)
}

func (p *SlottedPage) getHeader() *header {
	return (*header)(unsafe.Pointer(&p.data[0]))
}

func (p *SlottedPage) NumSlots() uint16 {
	header := p.getHeader()
	return header.slotsCount
}

func NewSlottedPage() *SlottedPage {
	p := &SlottedPage{
		data: [PageSize]byte{},
	}
	p.setupHeader()
	return p
}

func (p *SlottedPage) setupHeader() {
	head := p.getHeader()
	head.freeStart = uint16(unsafe.Offsetof(head.slots))
	head.freeEnd = PageSize
}

func (p *SlottedPage) UnsafeClear() {
	for i := range PageSize {
		p.data[i] = 0
	}
	p.setupHeader()
}

func (p *SlottedPage) Clear() {
	h := p.getHeader()

	h.pageLSN = common.NilLSN

	h.freeStart = uint16(unsafe.Offsetof(h.slots))
	h.freeEnd = PageSize

	h.slotsCount = 0
	h.slots = slotPointer(0)

	for i := h.freeStart; i < h.freeEnd; i++ {
		p.data[i] = 0
	}
}

func (p *SlottedPage) PageLSN() common.LSN {
	header := p.getHeader()
	return header.pageLSN
}

func (p *SlottedPage) SetPageLSN(lsn common.LSN) {
	header := p.getHeader()
	header.pageLSN = lsn
}

func (p *SlottedPage) insertPrepare(data []byte) optional.Optional[uint16] {
	assert.Assert(len(data) > 0, "data is empty")

	header := p.getHeader()
	// space required to store both the array and it's length
	requiredLength := int(unsafe.Sizeof(uint16(1))) + len(data)
	if int(header.freeEnd) < requiredLength {
		// uint16 overflow check
		return optional.None[uint16]()
	}

	pos := header.freeEnd - uint16(requiredLength)
	if pos < header.freeStart+slotPtrSize {
		return optional.None[uint16]()
	}

	defer func() {
		header.freeStart += slotPtrSize
		header.freeEnd = pos
	}()

	ptrToLen := (*uint16)(unsafe.Pointer(&p.data[pos]))
	*ptrToLen = uint16(len(data))
	ptr := newSlotPtr(SlotStatusPrepareInsert, pos)

	dst := p.getBytesBySlotPtr(ptr)
	n := copy(dst, data)
	assert.Assert(
		n == len(data),
		"couldn't copy data. copied only %d bytes",
		len(data),
	)

	curSlot := header.slotsCount
	header.slotsCount++
	slots := header.getSlots()
	slots[curSlot] = ptr

	return optional.Some(curSlot)
}

func (p *SlottedPage) insertCommit(slotHandle uint16) {
	header := p.getHeader()
	assert.Assert(
		uint16(slotHandle) < header.slotsCount,
		"slot number is too large. actual: %d. slots count: %d",
		slotHandle,
		header.slotsCount,
	)

	slots := header.getSlots()
	ptr := slots[slotHandle]
	assert.Assert(
		ptr.slotInfo() == SlotStatusPrepareInsert,
		"tried to commit an insert to a wrong slot",
	)
	slots[slotHandle] = newSlotPtr(SlotStatusInserted, ptr.RecordOffset())
}

func (p *SlottedPage) UnsafeInsertNoLogs(data []byte) optional.Optional[uint16] {
	slotOpt := p.insertPrepare(data)
	if slotOpt.IsNone() {
		return optional.None[uint16]()
	}
	p.insertCommit(slotOpt.Unwrap())
	return slotOpt
}

var ErrNoSpaceLeft error = errors.New("the page is full")

func (p *SlottedPage) InsertWithLogs(
	data []byte,
	pageIdent common.PageIdentity,
	lockedLogger common.ITxnLoggerWithContext,
) (uint16, common.LogRecordLocInfo, error) {
	slotOpt := p.insertPrepare(data)
	if slotOpt.IsNone() {
		return 0, common.NewNilLogRecordLocation(), ErrNoSpaceLeft
	}

	slot := slotOpt.Unwrap()
	recordID := common.RecordID{
		FileID:  pageIdent.FileID,
		PageID:  pageIdent.PageID,
		SlotNum: slot,
	}
	logRecordLoc, err := lockedLogger.AppendInsert(recordID, data)
	if err != nil {
		return 0, common.NewNilLogRecordLocation(), err
	}

	p.insertCommit(slot)
	p.SetPageLSN(logRecordLoc.Lsn)
	return slot, logRecordLoc, nil
}

func (p *SlottedPage) UndoInsert(slotID uint16) {
	header := p.getHeader()
	assert.Assert(slotID < header.slotsCount, "slotID is too large")
	ptr := header.getSlots()[slotID]
	slotInfo := ptr.slotInfo()

	assert.Assert(
		slotInfo == SlotStatusPrepareInsert || slotInfo == SlotStatusInserted,
		"tried to call `UndoInsert` on a slot with status %s", ptr.slotInfo().String(),
	)

	p.UnsafeOverrideSlotStatus(slotID, SlotStatusDeleted)
}

func (p *SlottedPage) getBytesBySlotPtr(ptr slotPointer) []byte {
	offset := ptr.RecordOffset()
	sliceLen := *(*uint16)(unsafe.Pointer(&p.data[offset]))
	data := unsafe.Slice(
		&p.data[offset+uint16(unsafe.Sizeof(uint16(0)))],
		sliceLen,
	)
	return data
}

func (p *SlottedPage) assertSlotInserted(slotID uint16) slotPointer {
	header := p.getHeader()
	assert.Assert(slotID < header.slotsCount, "slotID is too large")
	ptr := header.getSlots()[slotID]
	assert.Assert(
		ptr.slotInfo() == SlotStatusInserted,
		"tried to read from a slot with status %s", ptr.slotInfo().String(),
	)
	return ptr
}

func (p *SlottedPage) LockedRead(slotID uint16) []byte {
	p.RLock()
	defer p.RUnlock()
	return p.UnsafeRead(slotID)
}

func (p *SlottedPage) UnsafeRead(slotID uint16) []byte {
	ptr := p.assertSlotInserted(slotID)
	slotData := p.getBytesBySlotPtr(ptr)
	res := make([]byte, len(slotData))
	copy(res, slotData)
	return res
}

func (p *SlottedPage) slotDataView(slotID uint16) []byte {
	ptr := p.assertSlotInserted(slotID)
	return p.getBytesBySlotPtr(ptr)
}

func (p *SlottedPage) UnsafeDeleteNoLogs(slotID uint16) {
	ptr := p.assertSlotInserted(slotID)
	p.getHeader().getSlots()[slotID] = newSlotPtr(
		SlotStatusDeleted,
		ptr.RecordOffset(),
	)
}

func (p *SlottedPage) DeleteWithLogs(
	recordID common.RecordID,
	lockedLogger common.ITxnLoggerWithContext,
) (common.LogRecordLocInfo, error) {
	ptr := p.assertSlotInserted(recordID.SlotNum)

	logRecordLoc, err := lockedLogger.AppendDelete(recordID)
	if err != nil {
		return common.NewNilLogRecordLocation(), err
	}

	p.getHeader().getSlots()[recordID.SlotNum] = newSlotPtr(
		SlotStatusDeleted,
		ptr.RecordOffset(),
	)
	p.SetPageLSN(logRecordLoc.Lsn)

	return logRecordLoc, nil
}

func (p *SlottedPage) UndoDelete(slotID uint16) {
	header := p.getHeader()
	assert.Assert(slotID < header.slotsCount, "slotID is too large")
	ptr := header.getSlots()[slotID]
	assert.Assert(
		ptr.slotInfo() == SlotStatusDeleted,
		"tried to UndoDelete from a slot with status %s", ptr.slotInfo().String(),
	)

	p.UnsafeOverrideSlotStatus(slotID, SlotStatusInserted)
}

func (p *SlottedPage) UnsafeUpdateNoLogs(slotID uint16, newData []byte) {
	data := p.slotDataView(slotID)
	assert.Assert(len(data) == len(newData))

	clear(data)
	copy(data, newData)
}

func (p *SlottedPage) UpdateWithLogs(
	newData []byte,
	recordID common.RecordID,
	logger common.ITxnLoggerWithContext,
) (common.LogRecordLocInfo, error) {
	data := p.slotDataView(recordID.SlotNum)
	assert.Assert(
		len(data) == len(newData),
		"data and newData have different lengths. data: %d, newData: %d",
		len(data),
		len(newData),
	)

	before := make([]byte, len(data))
	copy(before, data)
	logRecordLoc, err := logger.AppendUpdate(
		recordID,
		before,
		newData,
	)
	if err != nil {
		return common.NewNilLogRecordLocation(), err
	}
	clear(data)
	copy(data, newData)
	p.SetPageLSN(logRecordLoc.Lsn)

	return logRecordLoc, nil
}

func (p *SlottedPage) TryLock() bool {
	return p.getHeader().latch.TryLock()
}

func (p *SlottedPage) Lock() {
	p.getHeader().latch.Lock()
}

func (p *SlottedPage) Unlock() {
	p.getHeader().latch.Unlock()
}

func (p *SlottedPage) RLock() {
	p.getHeader().latch.RLock()
}

func (p *SlottedPage) RUnlock() {
	p.getHeader().latch.RUnlock()
}

func (p *SlottedPage) GetData() []byte {
	return p.data[:]
}

func (p *SlottedPage) SetData(data []byte) {
	copy(p.data[:], data)
}

func (p *SlottedPage) UnsafeOverrideSlotStatus(
	slotNumber uint16,
	newStatus slotStatus,
) {
	assert.Assert(slotNumber < p.NumSlots(), "slotNumber is too large")

	header := p.getHeader()
	slot := header.getSlots()[slotNumber]

	header.getSlots()[slotNumber] = newSlotPtr(newStatus, slot.RecordOffset())
}

func InsertSerializable[T encoding.BinaryMarshaler](
	p *SlottedPage,
	obj T,
) optional.Optional[uint16] {
	bytes, err := obj.MarshalBinary()
	assert.NoError(err)
	return p.UnsafeInsertNoLogs(bytes)
}

func (p *SlottedPage) SlotInfo(slotID uint16) slotStatus {
	header := p.getHeader()
	assert.Assert(slotID < header.slotsCount, "slotID is too large")
	ptr := header.getSlots()[slotID]
	return ptr.slotInfo()
}
