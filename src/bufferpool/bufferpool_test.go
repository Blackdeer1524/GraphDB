package bufferpool

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
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
	slotOpt := p.UnsafeInsertNoLogs([]byte("cached data"))
	require.True(t, slotOpt.IsSome())

	frameID := uint64(0)
	manager.frames[frameID] = p
	manager.pageTable[pageIdent] = frameInfo{
		frameID:  frameID,
		pinCount: 0,
	}
	manager.DPT[pageIdent] = common.NewNilLogRecordLocation()

	mockReplacer.On("Pin", pageIdent).Return()

	result, err := manager.GetPage(pageIdent)

	assert.NoError(t, err)
	assert.Equal(t, p, *result)

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
	slotOpt := expectedPage.UnsafeInsertNoLogs([]byte("disk data"))
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
	assert.Equal(t, expectedPage, *result)

	assert.Equal(t, frameInfo{
		frameID:  manager.poolSize - 1,
		pinCount: 1,
	}, manager.pageTable[pageIdent])

	_, ok := manager.DPT[pageIdent]
	assert.False(t, ok)

	assert.Equal(t, expectedPage, manager.frames[manager.poolSize-1])

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
	slotOpt := existingPage.UnsafeInsertNoLogs([]byte("existing data"))
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
	manager.frames[frameID] = existingPage
	manager.emptyFrames = []uint64{1}

	newFileID := uint64(2)
	newPageID := uint64(1)

	newPage := page.NewSlottedPage()
	newSlotOpt := newPage.UnsafeInsertNoLogs([]byte("new data"))
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
	assert.Equal(t, newPage, *result)

	assert.Equal(t, frameInfo{
		frameID:  1,
		pinCount: 1,
	}, manager.pageTable[pIdent])

	_, ok := manager.DPT[pIdent]
	assert.False(t, ok)

	assert.Equal(t, newPage, manager.frames[1])
	assert.Equal(t, existingPage, manager.frames[0])

	mockDisk.AssertExpectations(t)
	mockReplacer.AssertExpectations(t)
}

func TestGetPage_LoadFromDisk_WithVictimReplacement(t *testing.T) {
	mockDisk := new(MockDiskManager)
	mockReplacer := new(MockReplacer)

	manager := New(1, mockReplacer, mockDisk)

	existingFileID, existingPageID := uint64(1), uint64(0)

	existingPage := page.NewSlottedPage()
	slotOpt := existingPage.UnsafeInsertNoLogs([]byte("old data"))
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
	manager.frames[frameID] = existingPage
	manager.emptyFrames = []uint64{}

	newPage := page.NewSlottedPage()
	newSlotOpt := newPage.UnsafeInsertNoLogs([]byte("new data"))
	require.True(t, newSlotOpt.IsSome())

	mockReplacer.On("ChooseVictim").Return(existingPageIdent, nil)

	mockDisk.On("Lock").Return()
	mockDisk.On("Unlock").Return()

	mockDisk.On("WritePageAssumeLocked", mock.AnythingOfType("*page.SlottedPage"), existingPageIdent).
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
	assert.Equal(t, newPage, *result)

	_, exists := manager.pageTable[existingPageIdent]
	assert.False(t, exists, "old page not removed from pageTable")

	assert.Equal(t, newPage, manager.frames[frameID])
	assert.Equal(t, frameInfo{
		frameID:  frameID,
		pinCount: 1,
	}, manager.pageTable[newPageIdent])

	_, ok := manager.DPT[existingPageIdent]
	assert.False(t, ok)

	mockReplacer.AssertExpectations(t)
	mockDisk.AssertExpectations(t)
}

func TestManager_Replacement(t *testing.T) {
	dk := disk.NewInMemoryManager()
	replacer := NewLRUReplacer()

	N := uint64(100)
	failedCh := make(chan uint64, N)

	pool := NewDebugBufferPool(New(4, replacer, dk))
	defer func() { assert.NoError(t, pool.EnsureAllPagesUnpinnedAndUnlocked()) }()

	fileID := common.FileID(1)
	wg := sync.WaitGroup{}
	for i := range N {
		wg.Add(1)
		go func(i uint64) {
			defer wg.Done()

			pid := common.PageIdentity{
				FileID: fileID,
				PageID: common.PageID(i),
			}

			pg, err := pool.GetPage(pid)
			if err != nil {
				assert.ErrorIs(t, err, ErrNoSpaceLeft)
				failedCh <- i
				return
			}

			defer pool.Unpin(pid)
			assert.NoError(t, pool.WithMarkDirty(
				common.TxnID(i),
				pid,
				pg,
				func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
					slotID, logRecordLoc, err := lockedPage.InsertWithLogs(
						utils.ToBytes[uint64](i),
						pid,
						common.NoLogs(common.TxnID(i)),
					)
					if err != nil {
						failedCh <- i
						return logRecordLoc, err
					}
					assert.Equal(t, slotID, uint16(0))
					return logRecordLoc, nil
				},
			))
		}(i)
	}
	wg.Wait()
	close(failedCh)

	failedIDs := map[uint64]struct{}{}
	for i := range failedCh {
		failedIDs[i] = struct{}{}
	}

	for i := range N {
		func(i uint64) {
			pid := common.PageIdentity{
				FileID: fileID,
				PageID: common.PageID(i),
			}

			if _, ok := failedIDs[i]; ok {
				_, err := pool.GetPageNoCreate(pid)
				assert.ErrorIs(t, err, disk.ErrNoSuchPage)
				return
			}

			pg, err := pool.GetPageNoCreate(pid)
			require.NoError(t, err)
			defer pool.Unpin(pid)

			pg.Lock()
			defer pg.Unlock()

			assert.Equal(t, uint16(1), pg.NumSlots())

			data := pg.UnsafeRead(0)
			assert.Equal(t, utils.ToBytes[uint64](i), data)
		}(i)
	}
}
