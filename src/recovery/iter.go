package recovery

import (
	"errors"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage/disk"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
)

type LogRecordsIter struct {
	logfileID common.FileID
	curLoc    common.FileLocation

	pool        bufferpool.BufferPool
	currentPage *page.SlottedPage
}

func newLogRecordIter(
	logfileID common.FileID,
	curLoc common.FileLocation,
	pool bufferpool.BufferPool,
	startPage *page.SlottedPage,
) *LogRecordsIter {
	return &LogRecordsIter{
		logfileID:   logfileID,
		curLoc:      curLoc,
		pool:        pool,
		currentPage: startPage,
	}
}

var ErrInvalidIterator = errors.New("iterator is invalid")

func (iter *LogRecordsIter) PageID() common.PageID {
	return iter.curLoc.PageID
}

// Returns an error only if couldn't read the next page
func (iter *LogRecordsIter) MoveForward() (res bool, err error) {
	assert.Assert(iter.currentPage != nil)

	iter.currentPage.RLock()
	if iter.curLoc.SlotNum+1 < iter.currentPage.NumSlots() {
		iter.curLoc.SlotNum++
		iter.currentPage.RUnlock()
		return true, nil
	}
	iter.currentPage.RUnlock()

	curPageID := common.PageIdentity{
		FileID: iter.logfileID,
		PageID: iter.curLoc.PageID,
	}
	iter.pool.Unpin(curPageID)
	iter.currentPage = nil

	nextPageIdent := common.PageIdentity{
		FileID: iter.logfileID,
		PageID: iter.curLoc.PageID + 1,
	}
	newPage, err := iter.pool.GetPageNoCreate(nextPageIdent)
	if errors.Is(err, disk.ErrNoSuchPage) {
		return false, nil
	} else if err != nil {
		return false, err
	}

	if newPage.NumSlots() == 0 {
		iter.pool.Unpin(nextPageIdent)
		return false, nil
	}

	iter.curLoc.PageID++
	iter.curLoc.SlotNum = 0

	iter.currentPage = newPage
	return true, nil
}

func (iter *LogRecordsIter) ReadRecord() (LogRecordTypeTag, any, error) {
	d := iter.currentPage.LockedRead(iter.curLoc.SlotNum)
	return parseLogRecord(d)
}

func (iter *LogRecordsIter) Location() common.FileLocation {
	return iter.curLoc
}
