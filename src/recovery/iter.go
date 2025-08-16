package recovery

import (
	"errors"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
)

type LogRecordsIter struct {
	logfileID common.FileID
	curLoc    common.FileLocation

	pool       bufferpool.BufferPool[*page.SlottedPage]
	lockedPage *page.SlottedPage
}

func newLogRecordIter(
	logfileID common.FileID,
	curLoc common.FileLocation,
	pool bufferpool.BufferPool[*page.SlottedPage],
	lockedPage *page.SlottedPage,
) *LogRecordsIter {
	return &LogRecordsIter{
		logfileID:  logfileID,
		curLoc:     curLoc,
		pool:       pool,
		lockedPage: lockedPage,
	}
}

var ErrInvalidIterator = errors.New("iterator is invalid")

// Returns an error only if couldn't read the next page
func (iter *LogRecordsIter) MoveForward() (res bool, err error) {
	if iter.curLoc.SlotNum+1 < iter.lockedPage.NumSlots() {
		iter.curLoc.SlotNum++
		return true, nil
	}

	curPageID := common.PageIdentity{
		FileID: iter.logfileID,
		PageID: iter.curLoc.PageID,
	}
	defer func(pageID common.PageIdentity) { err = iter.pool.Unpin(pageID) }(
		curPageID,
	)
	defer iter.lockedPage.RUnlock()

	newPage, err := iter.pool.GetPageNoCreate(
		common.PageIdentity{
			FileID: iter.logfileID,
			PageID: iter.curLoc.PageID + 1,
		})

	if errors.Is(err, bufferpool.ErrNoSuchPage) {
		return false, nil
	} else if err != nil {
		return false, err
	}

	iter.curLoc.PageID++
	iter.curLoc.SlotNum = 0

	newPage.RLock()
	iter.lockedPage = newPage

	return true, nil
}

func (iter *LogRecordsIter) ReadRecord() (LogRecordTypeTag, any, error) {
	d := iter.lockedPage.Read(iter.curLoc.SlotNum)
	return readLogRecord(d)
}

func (iter *LogRecordsIter) Location() common.FileLocation {
	return iter.curLoc
}
