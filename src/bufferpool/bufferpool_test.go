package bufferpool

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
)

func TestGetPage_Cached(t *testing.T) {
	mockDisk := new(MockDiskManager)
	mockReplacer := new(MockReplacer)

	manager, err := New(1, mockReplacer, mockDisk)
	require.NoError(t, err)

	fileID, pageID := uint64(1), uint64(0)
	pageIdent := common.PageIdentity{
		FileID: common.FileID(fileID),
		PageID: common.PageID(pageID),
	}

	p := page.NewSlottedPage()
	insertOpt := p.InsertPrepare([]byte("cached data"))
	p.InsertCommit(insertOpt.Unwrap())

	frameID := uint64(0)
	manager.frames[frameID] = *p
	manager.pageTable[pageIdent] = frameInfo{
		frameID:  frameID,
		pinCount: 0,
		isDirty:  false,
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

	manager, err := New(1, mockReplacer, mockDisk)
	require.NoError(t, err)

	fileID, pageID := uint64(1), uint64(0)
	pageIdent := common.PageIdentity{
		FileID: common.FileID(fileID),
		PageID: common.PageID(pageID),
	}

	expectedPage := page.NewSlottedPage()
	slotOpt := expectedPage.InsertPrepare([]byte("disk data"))
	expectedPage.InsertCommit(slotOpt.Unwrap())

	mockDisk.On("ReadPage", pageIdent).Return(expectedPage, nil)
	mockReplacer.On("Pin", pageIdent).Return()

	result, err := manager.GetPage(pageIdent)

	assert.NoError(t, err)
	assert.Equal(t, expectedPage, result)

	assert.Equal(t, frameInfo{
		frameID:  manager.poolSize - 1,
		pinCount: 1,
		isDirty:  false,
	}, manager.pageTable[pageIdent])

	assert.Equal(t, *expectedPage, manager.frames[manager.poolSize-1])

	mockDisk.AssertExpectations(t)
	mockReplacer.AssertExpectations(t)
}

func TestGetPage_LoadFromDisk_WithExistingPage(t *testing.T) {
	mockDisk := new(MockDiskManager)
	mockReplacer := new(MockReplacer)

	// Создаем пул из 2 фреймов
	manager, err := New(2, mockReplacer, mockDisk)
	require.NoError(t, err)

	existingFileID, existingPageID := uint64(1), uint64(0)

	existingPage := page.NewSlottedPage()
	slotOpt := existingPage.InsertPrepare([]byte("existing data"))
	existingPage.InsertCommit(slotOpt.Unwrap())

	existingPageData := common.PageIdentity{
		FileID: common.FileID(existingFileID),
		PageID: common.PageID(existingPageID),
	}

	frameID := uint64(0)
	manager.pageTable[existingPageData] = frameInfo{
		frameID:  frameID,
		pinCount: 1,
		isDirty:  false,
	}
	manager.frames[frameID] = *existingPage
	manager.emptyFrames = []uint64{1}

	newFileID := uint64(2)
	newPageID := uint64(1)

	newPage := page.NewSlottedPage()
	newInsertSlotOpt := newPage.InsertPrepare([]byte("new data"))
	newPage.InsertCommit(newInsertSlotOpt.Unwrap())

	pIdent := common.PageIdentity{
		FileID: common.FileID(newFileID),
		PageID: common.PageID(newPageID),
	}
	mockDisk.On("ReadPage", pIdent).Return(newPage, nil)
	mockReplacer.On("Pin", pIdent).Return()

	result, err := manager.GetPage(pIdent)
	assert.NoError(t, err)
	assert.Equal(t, newPage, result)

	assert.Equal(t, frameInfo{
		frameID:  1,
		pinCount: 1,
		isDirty:  false,
	}, manager.pageTable[pIdent])
	assert.Equal(t, *newPage, manager.frames[1])
	assert.Equal(t, *existingPage, manager.frames[0])

	mockDisk.AssertExpectations(t)
	mockReplacer.AssertExpectations(t)
}

func TestGetPage_LoadFromDisk_WithVictimReplacement(t *testing.T) {
	mockDisk := new(MockDiskManager)
	mockReplacer := new(MockReplacer)

	manager, err := New(1, mockReplacer, mockDisk)
	require.NoError(t, err)

	existingFileID, existingPageID := uint64(1), uint64(0)

	existingPage := page.NewSlottedPage()
	slotOpt := existingPage.InsertPrepare([]byte("old data"))
	existingPage.InsertCommit(slotOpt.Unwrap())

	existingPageIdent := common.PageIdentity{
		FileID: common.FileID(existingFileID),
		PageID: common.PageID(existingPageID),
	}

	frameID := uint64(0)
	manager.pageTable[existingPageIdent] = frameInfo{
		frameID:  frameID,
		pinCount: 1,
		isDirty:  true,
	}
	manager.frames[frameID] = *existingPage
	manager.emptyFrames = []uint64{}

	newPage := page.NewSlottedPage()
	newInsertSlotOpt := newPage.InsertPrepare([]byte("new data"))
	newPage.InsertCommit(newInsertSlotOpt.Unwrap())

	mockReplacer.On("ChooseVictim").Return(existingPageIdent, nil)
	mockDisk.On("WritePage", existingPage, existingPageIdent).Return(nil)

	newFileID, newPageID := uint64(2), uint64(1)
	newPageIdent := common.PageIdentity{
		FileID: common.FileID(newFileID),
		PageID: common.PageID(newPageID),
	}
	mockDisk.On("ReadPage", newPageIdent).Return(newPage, nil)
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
		isDirty:  false,
	}, manager.pageTable[newPageIdent])

	mockReplacer.AssertExpectations(t)
	mockDisk.AssertExpectations(t)
}
