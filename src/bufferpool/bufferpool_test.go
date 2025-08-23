package bufferpool

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage/disk"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
)

func TestGetPage_Cached(t *testing.T) {
	mockDisk := new(MockDiskManager)
	mockReplacer := new(MockReplacer)

	manager := New(1, mockReplacer, mockDisk)

	fileID, pageID := uint64(1), uint64(0)
	pageIdent := common.PageIdentity{
		FileID: common.FileID(fileID),
		PageID: common.PageID(pageID),
	}

	p := page.NewSlottedPage()
	slotOpt := p.Insert([]byte("cached data"))
	require.True(t, slotOpt.IsSome())

	frameID := uint64(0)
	manager.frames[frameID] = *p
	manager.pageTable[pageIdent] = frameInfo{
		frameID:  frameID,
		pinCount: 0,
	}

	mockReplacer.On("Pin", pageIdent).Return()

	result, err := manager.GetPage(pageIdent)

	assert.NoError(t, err)
	assert.Equal(t, p, result)

	// не должно быть считывания с диска
	mockDisk.AssertNotCalled(t, "ReadPage", pageIdent)

	mockReplacer.AssertExpectations(t)
	mockReplacer.AssertNotCalled(t, "Unpin", mock.Anything)
}

func TestGetPage_LoadFromDisk(t *testing.T) {
	mockDisk := new(MockDiskManager)
	mockReplacer := new(MockReplacer)

	manager := New(1, mockReplacer, mockDisk)

	fileID, pageID := uint64(1), uint64(0)
	pageIdent := common.PageIdentity{
		FileID: common.FileID(fileID),
		PageID: common.PageID(pageID),
	}

	expectedPage := page.NewSlottedPage()
	slotOpt := expectedPage.Insert([]byte("disk data"))
	require.True(t, slotOpt.IsSome())

	mockDisk.On("ReadPage", mock.AnythingOfType("*page.SlottedPage"), pageIdent).
		Run(func(args mock.Arguments) {
			pg := args.Get(0).(*page.SlottedPage)
			pg.SetData(expectedPage.GetData())
			pg.UnsafeInitLatch()
		}).
		Return(nil)

	mockReplacer.On("Pin", pageIdent).Return()

	result, err := manager.GetPage(pageIdent)

	assert.NoError(t, err)
	assert.Equal(t, expectedPage, result)

	assert.Equal(t, frameInfo{
		frameID:  manager.poolSize - 1,
		pinCount: 1,
	}, manager.pageTable[pageIdent])

	_, ok := manager.DPT[pageIdent]
	assert.False(t, ok)

	assert.Equal(t, *expectedPage, manager.frames[manager.poolSize-1])

	mockDisk.AssertExpectations(t)
	mockReplacer.AssertExpectations(t)
}

func TestGetPage_LoadFromDisk_WithExistingPage(t *testing.T) {
	mockDisk := new(MockDiskManager)
	mockReplacer := new(MockReplacer)

	// Создаем пул из 2 фреймов
	manager := New(2, mockReplacer, mockDisk)

	existingFileID, existingPageID := uint64(1), uint64(0)

	existingPage := page.NewSlottedPage()
	slotOpt := existingPage.Insert([]byte("existing data"))
	require.True(t, slotOpt.IsSome())

	existingPageData := common.PageIdentity{
		FileID: common.FileID(existingFileID),
		PageID: common.PageID(existingPageID),
	}

	frameID := uint64(0)
	manager.pageTable[existingPageData] = frameInfo{
		frameID:  frameID,
		pinCount: 1,
	}
	manager.frames[frameID] = *existingPage
	manager.emptyFrames = []uint64{1}

	newFileID := uint64(2)
	newPageID := uint64(1)

	newPage := page.NewSlottedPage()
	newSlotOpt := newPage.Insert([]byte("new data"))
	require.True(t, newSlotOpt.IsSome())

	pIdent := common.PageIdentity{
		FileID: common.FileID(newFileID),
		PageID: common.PageID(newPageID),
	}
	mockDisk.On("ReadPage", mock.AnythingOfType("*page.SlottedPage"), pIdent).
		Run(func(args mock.Arguments) {
			pg := args.Get(0).(*page.SlottedPage)
			pg.SetData(newPage.GetData())
			pg.UnsafeInitLatch()
		}).
		Return(nil)
	mockReplacer.On("Pin", pIdent).Return()

	result, err := manager.GetPage(pIdent)
	assert.NoError(t, err)
	assert.Equal(t, newPage, result)

	assert.Equal(t, frameInfo{
		frameID:  1,
		pinCount: 1,
	}, manager.pageTable[pIdent])

	_, ok := manager.DPT[pIdent]
	assert.False(t, ok)

	assert.Equal(t, *newPage, manager.frames[1])
	assert.Equal(t, *existingPage, manager.frames[0])

	mockDisk.AssertExpectations(t)
	mockReplacer.AssertExpectations(t)
}

func TestGetPage_LoadFromDisk_WithVictimReplacement(t *testing.T) {
	mockDisk := new(MockDiskManager)
	mockReplacer := new(MockReplacer)

	manager := New(1, mockReplacer, mockDisk)

	existingFileID, existingPageID := uint64(1), uint64(0)

	existingPage := page.NewSlottedPage()
	slotOpt := existingPage.Insert([]byte("old data"))
	require.True(t, slotOpt.IsSome())

	existingPageIdent := common.PageIdentity{
		FileID: common.FileID(existingFileID),
		PageID: common.PageID(existingPageID),
	}

	frameID := uint64(0)
	manager.pageTable[existingPageIdent] = frameInfo{
		frameID:  frameID,
		pinCount: 0,
	}
	manager.DPT[existingPageIdent] = common.LogRecordLocInfo{
		Lsn: 1,
		Location: common.FileLocation{
			PageID:  1,
			SlotNum: 0,
		},
	}
	manager.frames[frameID] = *existingPage
	manager.emptyFrames = []uint64{}

	newPage := page.NewSlottedPage()
	newSlotOpt := newPage.Insert([]byte("new data"))
	require.True(t, newSlotOpt.IsSome())

	mockReplacer.On("ChooseVictim").Return(existingPageIdent, nil)
	mockDisk.On("WritePage", mock.AnythingOfType("*page.SlottedPage"), existingPageIdent).
		Return(nil)

	newPageIdent := common.PageIdentity{
		FileID: common.FileID(uint64(2)),
		PageID: common.PageID(uint64(1)),
	}
	mockDisk.On("ReadPage", mock.AnythingOfType("*page.SlottedPage"), newPageIdent).
		Run(func(args mock.Arguments) {
			pg := args.Get(0).(*page.SlottedPage)
			pg.SetData(newPage.GetData())
			pg.UnsafeInitLatch()
		}).
		Return(nil)
	mockReplacer.On("Pin", newPageIdent).Return()

	result, err := manager.GetPage(newPageIdent)

	assert.NoError(t, err)
	assert.Equal(t, newPage, result)

	_, exists := manager.pageTable[existingPageIdent]
	assert.False(t, exists, "Старая страница не удалена из pageTable")

	assert.Equal(t, *newPage, manager.frames[frameID])
	assert.Equal(t, frameInfo{
		frameID:  frameID,
		pinCount: 1,
	}, manager.pageTable[newPageIdent])

	_, ok := manager.DPT[existingPageIdent]
	assert.False(t, ok)

	mockReplacer.AssertExpectations(t)
	mockDisk.AssertExpectations(t)
}

type concurrentFakeDisk struct {
	t       *testing.T
	mu      sync.Mutex
	storage map[common.PageIdentity]*page.SlottedPage
}

func newConcurrentFakeDisk(t *testing.T) *concurrentFakeDisk {
	return &concurrentFakeDisk{
		t:       t,
		storage: make(map[common.PageIdentity]*page.SlottedPage),
	}
}

var _ common.DiskManager[*page.SlottedPage] = &concurrentFakeDisk{}

func pageIdentString(pIdent common.PageIdentity) string {
	return fmt.Sprintf("PAGE:%d:%d", pIdent.FileID, pIdent.PageID)
}

func ensurePage(t *testing.T, pg *page.SlottedPage, pIdent common.PageIdentity) {
	pg.RLock()
	defer pg.RUnlock()

	expected := []byte(pageIdentString(pIdent))
	if !assert.Equal(t, uint16(1), pg.NumSlots()) {
		return
	}

	data := pg.Read(0)
	assert.Equal(t, expected, data)
}

func (d *concurrentFakeDisk) GetPageNoNew(pg *page.SlottedPage, pIdent common.PageIdentity) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	storedPage, ok := d.storage[pIdent]
	if ok {
		pg.SetData(storedPage.GetData())
		pg.UnsafeInitLatch()
		return nil
	}

	return disk.ErrNoSuchPage
}

func (d *concurrentFakeDisk) ReadPage(pg *page.SlottedPage, pIdent common.PageIdentity) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	storedPage, ok := d.storage[pIdent]
	if ok {
		require.Equal(d.t, uint16(1), storedPage.NumSlots())
		pg.SetData(storedPage.GetData())
		pg.UnsafeInitLatch()
		return nil
	}

	newPage := page.NewSlottedPage()
	require.NotNil(d.t, newPage)
	require.Equal(d.t, uint16(0), newPage.NumSlots())
	slotOpt := newPage.Insert([]byte(pageIdentString(pIdent)))
	if slotOpt.IsNone() {
		return fmt.Errorf("failed to insert page: %+v", pIdent)
	}
	require.Equal(d.t, uint16(1), newPage.NumSlots())
	d.storage[pIdent] = newPage
	pg.SetData(newPage.GetData())
	return nil
}

func (d *concurrentFakeDisk) WritePage(page *page.SlottedPage, pIdent common.PageIdentity) error {
	return nil
}

func TestManager_ConcurrentReplacement(t *testing.T) {
	disk := newConcurrentFakeDisk(t)
	replacer := NewLRUReplacer()

	const poolSize uint64 = 4
	const numPages = 32
	const numWorkers = 4
	const opsPerWorker = 500

	pool := NewDebugBufferPool(New(poolSize, replacer, disk), map[common.PageIdentity]struct{}{})
	defer func() { assert.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	fileID := common.FileID(1)

	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for w := 0; w < numWorkers; w++ {
		workerID := w
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				pageIdx := (i*7 + workerID*3) % numPages
				pid := common.PageIdentity{
					FileID: fileID,
					PageID: common.PageID(pageIdx),
				}

				func() {
					pg, err := pool.GetPage(pid)
					if !assert.NoError(t, err) {
						return
					}
					defer pool.Unpin(pid)

					ensurePage(t, pg, pid)
					time.Sleep(100 * time.Microsecond)
				}()
			}
		}()
	}
	wg.Wait()
}
