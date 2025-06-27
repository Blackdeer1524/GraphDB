package bufferpool

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool/mocks"
)

func TestGetPage_Cached(t *testing.T) {
	mockDisk := new(mocks.MockDiskManager)
	mockReplacer := new(mocks.MockReplacer)

	manager, err := New(1, mockReplacer, mockDisk)
	require.NoError(t, err)

	fileID, pageID := uint64(1), uint64(0)

	p := mocks.NewSlottedPage(fileID, pageID)
	p.SetData([]byte("cached data"))

	frameID := uint64(0)
	manager.frames[frameID] = frame[*mocks.SlottedPage]{
		Page:     p,
		PinCount: 0,
		FileID:   fileID,
		PageID:   pageID,
	}
	manager.pageToFrame[PageIdentity{FileID: fileID, PageID: pageID}] = frameID

	mockReplacer.On("Pin", frameID).Return()

	pIdent := PageIdentity{FileID: fileID, PageID: pageID}

	result, err := manager.GetPage(pIdent)

	assert.NoError(t, err)
	assert.Equal(t, p, result)

	// не должно быть считывания с диска
	mockDisk.AssertNotCalled(t, "ReadPage", fileID, pageID)

	mockReplacer.AssertExpectations(t)
	mockReplacer.AssertNotCalled(t, "Unpin", mock.Anything)
}

func TestGetPage_LoadFromDisk(t *testing.T) {
	mockDisk := new(mocks.MockDiskManager)
	mockReplacer := new(mocks.MockReplacer)

	manager, err := New(1, mockReplacer, mockDisk)
	require.NoError(t, err)

	fileID, pageID := uint64(1), uint64(0)

	expectedPage := mocks.NewSlottedPage(fileID, pageID)
	expectedPage.SetData([]byte("disk data"))

	mockDisk.On("ReadPage", fileID, pageID).Return(expectedPage, nil)
	mockReplacer.On("Pin", uint64(0)).Return()

	pIdent := PageIdentity{FileID: fileID, PageID: pageID}
	result, err := manager.GetPage(pIdent)

	assert.NoError(t, err)
	assert.Equal(t, expectedPage, result)

	assert.Equal(t, uint64(0), manager.pageToFrame[pIdent])
	assert.Equal(t, expectedPage, manager.frames[0].Page)

	mockDisk.AssertExpectations(t)
	mockReplacer.AssertExpectations(t)
}

func TestGetPage_LoadFromDisk_WithExistingPage(t *testing.T) {
	mockDisk := new(mocks.MockDiskManager)
	mockReplacer := new(mocks.MockReplacer)

	// Создаем пул из 2 фреймов
	manager, err := New(2, mockReplacer, mockDisk)
	require.NoError(t, err)

	existingFileID, existingPageID := uint64(1), uint64(0)
	existingPage := mocks.NewSlottedPage(existingFileID, existingPageID)
	existingPage.SetData([]byte("existing data"))

	frameID := uint64(0)
	manager.frames[frameID] = frame[*mocks.SlottedPage]{
		Page:     existingPage,
		PinCount: 1,
		FileID:   existingFileID,
		PageID:   existingPageID,
	}
	manager.pageToFrame[PageIdentity{FileID: existingFileID, PageID: existingPageID}] = frameID
	manager.emptyFrames = []uint64{1}

	newFileID := uint64(2)
	newPageID := uint64(1)
	newPage := mocks.NewSlottedPage(newFileID, newPageID)
	newPage.SetData([]byte("new data"))

	mockDisk.On("ReadPage", newFileID, newPageID).Return(newPage, nil)
	mockReplacer.On("Pin", uint64(1)).Return()

	pIdent := PageIdentity{FileID: newFileID, PageID: newPageID}
	result, err := manager.GetPage(pIdent)

	assert.NoError(t, err)
	assert.Equal(t, newPage, result)

	assert.Equal(t, uint64(1), manager.pageToFrame[pIdent])
	assert.Equal(t, newPage, manager.frames[1].Page)

	assert.Equal(t, existingPage, manager.frames[0].Page)
	assert.Equal(
		t,
		uint64(0),
		manager.pageToFrame[PageIdentity{FileID: existingFileID, PageID: existingPageID}],
	)

	mockDisk.AssertExpectations(t)
	mockReplacer.AssertExpectations(t)
}

func TestGetPage_LoadFromDisk_WithVictimReplacement(t *testing.T) {
	mockDisk := new(mocks.MockDiskManager)
	mockReplacer := new(mocks.MockReplacer)

	manager, err := New(1, mockReplacer, mockDisk)
	require.NoError(t, err)

	existingFileID, existingPageID := uint64(1), uint64(0)
	existingPage := mocks.NewSlottedPage(existingFileID, existingPageID)
	existingPage.SetData([]byte("old data"))
	existingPage.SetDirtiness(true)

	frameID := uint64(0)
	manager.frames[frameID] = frame[*mocks.SlottedPage]{
		Page:     existingPage,
		PinCount: 0,
		FileID:   existingFileID,
		PageID:   existingPageID,
	}
	manager.pageToFrame[PageIdentity{FileID: existingFileID, PageID: existingPageID}] = frameID
	manager.emptyFrames = nil

	newFileID, newPageID := uint64(2), uint64(1)
	newPage := mocks.NewSlottedPage(newFileID, newPageID)
	newPage.SetData([]byte("new data"))

	mockReplacer.On("ChooseVictim").Return(frameID, nil)
	mockReplacer.On("Pin", frameID).Return()

	mockDisk.On("WritePage", existingPage).Return(nil)
	mockDisk.On("ReadPage", newFileID, newPageID).Return(newPage, nil)

	pIdent := PageIdentity{FileID: newFileID, PageID: newPageID}
	result, err := manager.GetPage(pIdent)

	assert.NoError(t, err)
	assert.Equal(t, newPage, result)

	oldIdent := PageIdentity{FileID: existingFileID, PageID: existingPageID}
	_, exists := manager.pageToFrame[oldIdent]
	assert.False(t, exists, "Старая страница не удалена из pageToFrame")

	assert.Equal(t, frameID, manager.pageToFrame[pIdent])
	assert.Equal(t, newPage, manager.frames[frameID].Page)

	mockReplacer.AssertExpectations(t)
	mockDisk.AssertExpectations(t)
}
