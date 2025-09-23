package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

const hashmapLoadFactor = 0.6

type LinearProbingIndex struct {
	hasher DeterministicHasher64

	indexFileToken *txns.FileLockToken
	masterPage     *page.SlottedPage
	keySize        int
	pool           bufferpool.BufferPool
	locker         txns.ILockManager
	logger         common.ITxnLoggerWithContext

	debugAssertsEnabled     bool
	debugCheckAlreadyClosed bool
}

type bucketItemStatus byte

const (
	bucketItemStatusFree bucketItemStatus = iota
	bucketItemStatusInserted
	bucketItemStatusDeleted
)

const masterPageID = common.PageID(0)

const bucketItemSizeWithoutKey = unsafe.Sizeof(
	bucketItemStatusInserted,
) + uintptr(
	common.SerializedRecordIDSize,
)

func marshalBucketItem(
	status bucketItemStatus,
	key string,
	rid common.RecordID,
) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, status)
	if err != nil {
		return nil, err
	}

	// Write key data
	_, err = buf.WriteString(key)
	if err != nil {
		return nil, err
	}

	// Write record ID
	ridBytes, err := rid.MarshalBinary()
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(ridBytes)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func unmarshalBucketItem(
	data []byte,
	keySize int,
) (bucketItemStatus, string, common.RecordID, error) {
	rd := bytes.NewReader(data)
	var status bucketItemStatus
	var key string
	var rid common.RecordID

	// Read status
	err := binary.Read(rd, binary.BigEndian, &status)
	if err != nil {
		return 0, "", common.RecordID{}, err
	}

	keyBytes := make([]byte, keySize)
	_, err = rd.Read(keyBytes)
	if err != nil {
		return 0, "", common.RecordID{}, err
	}
	key = string(keyBytes)

	// Read record ID
	ridBytes := make([]byte, common.SerializedRecordIDSize)
	_, err = rd.Read(ridBytes)
	if err != nil {
		return 0, "", common.RecordID{}, err
	}

	err = rid.UnmarshalBinary(ridBytes)
	if err != nil {
		return 0, "", common.RecordID{}, err
	}

	return status, key, rid, nil
}

const (
	bucketsCountSlot = iota
	bucketItemSizeSlot
	bucketCapacitySlot
	recordsCountSlot
	hashmapTotalCapacitySlot
	startPageIDSlot
	masterPageSlotsCount
)

func NewLinearProbingIndex(
	meta storage.IndexMeta,
	pool bufferpool.BufferPool,
	locker txns.ILockManager,
	logger common.ITxnLoggerWithContext,
	enableDebugAsserts bool,
	seed uint64,
) (*LinearProbingIndex, error) {
	cToken := txns.NewNilCatalogLockToken(logger.GetTxnID())

	masterPage, err := pool.GetPage(getMasterPageIdent(meta.FileID))
	if err != nil {
		return nil, fmt.Errorf("failed to get master page: %w", err)
	}

	index := &LinearProbingIndex{
		indexFileToken: txns.NewNilFileLockToken(cToken, meta.FileID),
		keySize:        int(meta.KeyBytesCnt),
		locker:         locker,
		logger:         logger,
		hasher:         NewDeterministicHasher64(seed),
		masterPage:     masterPage,
		pool:           pool,

		debugCheckAlreadyClosed: false,
		debugAssertsEnabled:     enableDebugAsserts,
	}

	if err := index.setupMasterPage(meta); err != nil {
		pool.Unpin(getMasterPageIdent(meta.FileID))
		return nil, fmt.Errorf("failed to setup master page: %w", err)
	}

	return index, nil
}

func getMasterPageIdent(fileID common.FileID) common.PageIdentity {
	return common.PageIdentity{
		FileID: fileID,
		PageID: masterPageID,
	}
}

func (i *LinearProbingIndex) Get(key []byte) (common.RecordID, error) {
	assert.Assert(len(key) == i.keySize, "key size mismatch")

	pToken := i.locker.LockPage(i.indexFileToken, masterPageID, txns.SimpleLockShared)
	if pToken == nil {
		err := fmt.Errorf("failed to lock page %v: %w", masterPageID, txns.ErrDeadlockPrevention)
		return common.RecordID{}, err
	}

	bucketCapacity := utils.FromBytes[uint64](i.masterPage.LockedRead(bucketCapacitySlot))
	recordsLimit := utils.FromBytes[uint64](i.masterPage.LockedRead(hashmapTotalCapacitySlot))
	startPageID := utils.FromBytes[common.PageID](i.masterPage.LockedRead(startPageIDSlot))

	i.hasher.Reset()
	i.hasher.Write(key)
	startArrayIndex := i.hasher.Sum64() % recordsLimit
	k := startArrayIndex

	nProbesCount := 0
	for {
		nProbesCount++

		bucketIndex := k / bucketCapacity
		slotNumber := uint16(k % bucketCapacity)

		bucketItemPageID := startPageID + common.PageID(bucketIndex)

		pToken := i.locker.LockPage(i.indexFileToken, bucketItemPageID, txns.SimpleLockShared)
		if pToken == nil {
			err := fmt.Errorf(
				"failed to lock page %v: %w",
				bucketItemPageID,
				txns.ErrDeadlockPrevention,
			)
			return common.RecordID{}, err
		}

		bucketPageIdent := common.PageIdentity{
			FileID: i.indexFileToken.GetFileID(),
			PageID: bucketItemPageID,
		}
		pg, err := i.pool.GetPage(bucketPageIdent)
		if err != nil {
			return common.RecordID{}, fmt.Errorf("failed to get page: %w", err)
		}
		bucketItemData := pg.LockedRead(slotNumber)
		i.pool.Unpin(bucketPageIdent)

		status, itemKey, rid, err := unmarshalBucketItem(bucketItemData, i.keySize)
		if err != nil {
			return common.RecordID{}, fmt.Errorf("failed to unmarshal bucket item: %w", err)
		}

		switch status {
		case bucketItemStatusInserted:
			if itemKey == string(key) {
				return rid, nil
			}

		case bucketItemStatusDeleted:
		case bucketItemStatusFree:
			return common.RecordID{}, storage.ErrKeyNotFound
		}

		k = (k + 1) % recordsLimit
		assert.Assert(k != startArrayIndex, "k == startArrayIndex. Should have grown the index")
	}
}

func (i *LinearProbingIndex) Delete(key []byte) error {
	assert.Assert(len(key) == i.keySize, "key size mismatch")

	pToken := i.locker.LockPage(i.indexFileToken, masterPageID, txns.SimpleLockShared)
	if pToken == nil {
		err := fmt.Errorf("failed to lock page %v: %w", masterPageID, txns.ErrDeadlockPrevention)
		return err
	}

	bucketCapacity := utils.FromBytes[uint64](i.masterPage.LockedRead(bucketCapacitySlot))
	recordsLimit := utils.FromBytes[uint64](i.masterPage.LockedRead(hashmapTotalCapacitySlot))
	startPageID := utils.FromBytes[common.PageID](i.masterPage.LockedRead(startPageIDSlot))

	i.hasher.Reset()
	i.hasher.Write(key)
	startArrayIndex := i.hasher.Sum64() % recordsLimit
	k := startArrayIndex
	for {
		bucketIndex := k / bucketCapacity
		slotNumber := uint16(k % bucketCapacity)

		bucketItemPageID := startPageID + common.PageID(bucketIndex)

		pToken := i.locker.LockPage(i.indexFileToken, bucketItemPageID, txns.SimpleLockShared)
		if pToken == nil {
			err := fmt.Errorf(
				"failed to lock page %v: %w",
				bucketItemPageID,
				txns.ErrDeadlockPrevention,
			)
			return err
		}

		bucketPageIdent := common.PageIdentity{
			FileID: i.indexFileToken.GetFileID(),
			PageID: bucketItemPageID,
		}
		found, err := func() (bool, error) {
			pg, err := i.pool.GetPage(bucketPageIdent)
			if err != nil {
				return false, fmt.Errorf("failed to get page: %w", err)
			}
			defer i.pool.Unpin(bucketPageIdent)

			bucketItemData := pg.LockedRead(slotNumber)
			status, itemKey, rid, err := unmarshalBucketItem(bucketItemData, i.keySize)
			if err != nil {
				return false, fmt.Errorf("failed to unmarshal bucket item: %w", err)
			}

			switch status {
			case bucketItemStatusInserted:
				if itemKey != string(key) {
					return false, nil
				}
				deletedItemData, err := marshalBucketItem(bucketItemStatusDeleted, itemKey, rid)
				if err != nil {
					return false, fmt.Errorf("failed to marshal deleted bucket item: %w", err)
				}

				if !i.locker.UpgradePageLock(pToken, txns.SimpleLockExclusive) {
					err := fmt.Errorf(
						"failed to upgrade page lock %v: %w",
						bucketPageIdent,
						txns.ErrDeadlockPrevention,
					)
					return true, err
				}

				err = i.pool.WithMarkDirty(
					i.logger.GetTxnID(),
					bucketPageIdent,
					pg,
					func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
						return lockedPage.UpdateWithLogs(
							deletedItemData,
							common.RecordID{
								SlotNum: slotNumber,
								FileID:  i.indexFileToken.GetFileID(),
								PageID:  bucketItemPageID,
							},
							i.logger,
						)
					},
				)
				return true, err
			case bucketItemStatusDeleted:
				return false, nil
			case bucketItemStatusFree:
				return false, storage.ErrKeyNotFound
			}
			return false, nil
		}()
		if err != nil {
			return err
		}
		if found {
			return nil
		}
		k = (k + 1) % recordsLimit
		assert.Assert(k != startArrayIndex, "k == startArrayIndex. Should have grown the index")
	}
}

func (i *LinearProbingIndex) Insert(key []byte, rid common.RecordID) error {
	assert.Assert(len(key) == i.keySize, "key size mismatch")

	masterPageToken := i.locker.LockPage(i.indexFileToken, masterPageID, txns.SimpleLockShared)
	if masterPageToken == nil {
		err := fmt.Errorf("failed to lock page %v: %w", masterPageID, txns.ErrDeadlockPrevention)
		return err
	}

	i.masterPage.RLock()
	bucketItemSize := utils.FromBytes[uint64](i.masterPage.UnsafeRead(bucketItemSizeSlot))
	bucketCapacity := utils.FromBytes[uint64](i.masterPage.UnsafeRead(bucketCapacitySlot))
	recordsCount := utils.FromBytes[uint64](i.masterPage.UnsafeRead(recordsCountSlot))
	recordsLimit := utils.FromBytes[uint64](i.masterPage.UnsafeRead(hashmapTotalCapacitySlot))
	i.masterPage.RUnlock()

	if float64(recordsCount)/float64(recordsLimit) > hashmapLoadFactor {
		if err := i.grow(); err != nil {
			return err
		}
		recordsLimit = utils.FromBytes[uint64](i.masterPage.LockedRead(hashmapTotalCapacitySlot))
	}
	startPageID := utils.FromBytes[common.PageID](i.masterPage.LockedRead(startPageIDSlot))

	i.hasher.Reset()
	i.hasher.Write(key)
	startArrayIndex := i.hasher.Sum64() % recordsLimit
	k := startArrayIndex

	nProbesCount := 0
	for {
		nProbesCount++

		bucketIndex := k / bucketCapacity
		slotNumber := uint16(k % bucketCapacity)

		bucketItemPageID := startPageID + common.PageID(bucketIndex)

		bucketToken := i.locker.LockPage(i.indexFileToken, bucketItemPageID, txns.SimpleLockShared)
		if bucketToken == nil {
			err := fmt.Errorf(
				"failed to lock page %v: %w",
				bucketItemPageID,
				txns.ErrDeadlockPrevention,
			)
			return err
		}

		bucketPageIdent := common.PageIdentity{
			FileID: i.indexFileToken.GetFileID(),
			PageID: bucketItemPageID,
		}
		found, err := func() (bool, error) {
			bucketPage, err := i.pool.GetPage(bucketPageIdent)
			if err != nil {
				return false, fmt.Errorf("failed to get page: %w", err)
			}
			defer i.pool.Unpin(bucketPageIdent)

			bucketItemData := bucketPage.LockedRead(slotNumber)
			status, itemKey, _, err := unmarshalBucketItem(bucketItemData, i.keySize)
			if err != nil {
				return false, fmt.Errorf("failed to unmarshal bucket item: %w", err)
			}

			switch status {
			case bucketItemStatusInserted:
				// enforcing a unique constraint
				assert.Assert(itemKey != string(key), "unique constraint violation")
				return false, nil
			case bucketItemStatusDeleted:
				return false, nil
			case bucketItemStatusFree:
				insertedItemData, err := marshalBucketItem(
					bucketItemStatusInserted,
					string(key),
					rid,
				)
				assert.Assert(bucketItemSize == uint64(len(insertedItemData)))
				if err != nil {
					return false, fmt.Errorf("failed to marshal inserted bucket item: %w", err)
				}

				recordsCountData := utils.ToBytes[uint64](recordsCount + 1)
				if !i.locker.UpgradePageLock(masterPageToken, txns.SimpleLockExclusive) {
					err := fmt.Errorf(
						"failed to upgrade page lock %v: %w",
						masterPageID,
						txns.ErrDeadlockPrevention,
					)
					return true, err
				}
				err = i.pool.WithMarkDirty(
					i.logger.GetTxnID(),
					getMasterPageIdent(i.indexFileToken.GetFileID()),
					i.masterPage,
					func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
						return lockedPage.UpdateWithLogs(
							recordsCountData,
							common.RecordID{
								FileID:  i.indexFileToken.GetFileID(),
								PageID:  masterPageID,
								SlotNum: recordsCountSlot,
							},
							i.logger,
						)
					},
				)
				if err != nil {
					return true, err
				}

				if !i.locker.UpgradePageLock(bucketToken, txns.SimpleLockExclusive) {
					err := fmt.Errorf(
						"failed to upgrade page lock %v: %w",
						bucketPageIdent,
						txns.ErrDeadlockPrevention,
					)
					return true, err
				}

				err = i.pool.WithMarkDirty(
					i.logger.GetTxnID(),
					bucketPageIdent,
					bucketPage,
					func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
						return lockedPage.UpdateWithLogs(
							insertedItemData,
							common.RecordID{
								FileID:  i.indexFileToken.GetFileID(),
								PageID:  bucketItemPageID,
								SlotNum: slotNumber,
							},
							i.logger,
						)
					},
				)
				if err != nil {
					return false, err
				}
				return true, err
			}
			return false, nil
		}()
		if err != nil {
			return err
		}
		if found {
			return nil
		}
		k = (k + 1) % recordsLimit
		assert.Assert(k != startArrayIndex, "k == startArrayIndex. Should have grown the index")
	}
}

func (i *LinearProbingIndex) grow() error {
	if !i.locker.UpgradeFileLock(i.indexFileToken, txns.GranularLockExclusive) {
		err := fmt.Errorf(
			"failed to upgrade file lock %v: %w",
			i.indexFileToken.GetFileID(),
			txns.ErrDeadlockPrevention,
		)
		return err
	}
	if i.debugAssertsEnabled {
		masterPageToken := i.locker.LockPage(i.indexFileToken, masterPageID, txns.SimpleLockExclusive)
		assert.Assert(masterPageToken != nil)
	}

	i.masterPage.RLock()
	bucketsCount := utils.FromBytes[uint64](i.masterPage.UnsafeRead(bucketsCountSlot))
	startPageID := utils.FromBytes[common.PageID](i.masterPage.UnsafeRead(startPageIDSlot))
	bucketCapacity := utils.FromBytes[uint64](i.masterPage.UnsafeRead(bucketCapacitySlot))
	bucketItemSize := utils.FromBytes[uint64](i.masterPage.UnsafeRead(bucketItemSizeSlot))
	i.masterPage.RUnlock()

	dummyRecord := make([]byte, bucketItemSize)
	for k := range bucketsCount * 2 {
		newPageID := startPageID + common.PageID(bucketsCount) + common.PageID(k)

		if i.debugAssertsEnabled {
			pToken := i.locker.LockPage(i.indexFileToken, newPageID, txns.SimpleLockExclusive)
			assert.Assert(pToken != nil, "already aquired a file lock")
		}

		newBucketPageIdent := common.PageIdentity{
			FileID: i.indexFileToken.GetFileID(),
			PageID: newPageID,
		}
		err := func() error {
			pg, err := i.pool.GetPage(newBucketPageIdent)
			if err != nil {
				return fmt.Errorf("failed to get page: %w", err)
			}
			defer i.pool.Unpin(newBucketPageIdent)

			err = i.pool.WithMarkDirty(
				i.logger.GetTxnID(),
				newBucketPageIdent,
				pg,
				func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
					lockedPage.Clear()
					var loc common.LogRecordLocInfo
					var err error
					for range bucketCapacity {
						_, loc, err = lockedPage.InsertWithLogs(
							dummyRecord,
							newBucketPageIdent,
							i.logger,
						)
						assert.NoError(err)
					}
					return loc, nil
				},
			)
			return err
		}()

		if err != nil {
			return err
		}
	}

	err := i.pool.WithMarkDirty(
		i.logger.GetTxnID(),
		getMasterPageIdent(i.indexFileToken.GetFileID()),
		i.masterPage,
		func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
			_, err := lockedPage.UpdateWithLogs(
				utils.ToBytes[uint64](bucketsCount*2),
				common.RecordID{
					FileID:  i.indexFileToken.GetFileID(),
					PageID:  masterPageID,
					SlotNum: bucketsCountSlot,
				},
				i.logger,
			)
			if err != nil {
				return common.NewNilLogRecordLocation(), err
			}

			_, err = lockedPage.UpdateWithLogs(
				utils.ToBytes[uint64](uint64(startPageID)+bucketsCount),
				common.RecordID{
					FileID:  i.indexFileToken.GetFileID(),
					PageID:  masterPageID,
					SlotNum: startPageIDSlot,
				},
				i.logger,
			)
			if err != nil {
				return common.NewNilLogRecordLocation(), err
			}

			return lockedPage.UpdateWithLogs(
				utils.ToBytes[uint64](bucketCapacity*2*bucketsCount),
				common.RecordID{
					SlotNum: hashmapTotalCapacitySlot,
					FileID:  i.indexFileToken.GetFileID(),
					PageID:  masterPageID,
				},
				i.logger,
			)
		},
	)
	if err != nil {
		return err
	}

	for k := startPageID; k < startPageID+common.PageID(bucketsCount); k++ {
		if i.debugAssertsEnabled {
			prevGenBucketPageToken := i.locker.LockPage(i.indexFileToken, k, txns.SimpleLockShared)
			assert.Assert(prevGenBucketPageToken != nil, "already aquired a file lock")
		}

		prevGenBucketPageIdent := common.PageIdentity{
			FileID: i.indexFileToken.GetFileID(),
			PageID: k,
		}

		prevGenBucket, err := i.pool.GetPageNoCreate(prevGenBucketPageIdent)
		if err != nil {
			return fmt.Errorf("failed to get page: %w", err)
		}
		err = func() error {
			defer i.pool.Unpin(prevGenBucketPageIdent)

			prevGenBucket.RLock()
			defer prevGenBucket.RUnlock()

			for slotIdx := range prevGenBucket.NumSlots() {
				bucketItemData := prevGenBucket.UnsafeRead(slotIdx)
				status, itemKey, rid, err := unmarshalBucketItem(bucketItemData, i.keySize)

				if err != nil {
					return fmt.Errorf("failed to unmarshal bucket item: %w", err)
				}
				if status != bucketItemStatusInserted {
					continue
				}

				err = i.Insert([]byte(itemKey), rid)
				if err != nil {
					return fmt.Errorf("failed to insert bucket item: %w", err)
				}
			}
			return nil
		}()

		if err != nil {
			return err
		}
	}
	return nil
}

func (i *LinearProbingIndex) Close() error {
	assert.Assert(!i.debugCheckAlreadyClosed, "index already closed")
	i.debugCheckAlreadyClosed = true

	masterPageIdent := getMasterPageIdent(i.indexFileToken.GetFileID())
	i.pool.Unpin(masterPageIdent)
	return nil
}

func (i *LinearProbingIndex) setupMasterPage(indexMeta storage.IndexMeta) error {
	bucketItemSize := bucketItemSizeWithoutKey + uintptr(indexMeta.KeyBytesCnt)
	bucketCapacity := page.PageCapacity(int(bucketItemSize))

	err := func() error {
		masterPageIdent := getMasterPageIdent(indexMeta.FileID)
		masterPageToken := i.locker.LockPage(i.indexFileToken, masterPageID, txns.SimpleLockShared)
		if masterPageToken == nil {
			return fmt.Errorf(
				"failed to lock page %v: %w",
				masterPageID,
				txns.ErrDeadlockPrevention,
			)
		}

		masterPage, err := i.pool.GetPage(masterPageIdent)
		if err != nil {
			return fmt.Errorf("failed to get page: %w", err)
		}
		defer i.pool.Unpin(masterPageIdent)

		masterPage.RLock()
		if masterPage.NumSlots() == masterPageSlotsCount {
			foundAbnormal := false
			for slotIdx := range masterPage.NumSlots() {
				if masterPage.SlotInfo(slotIdx) != page.SlotStatusInserted {
					foundAbnormal = true
				}
			}
			if !foundAbnormal {
				masterPage.RUnlock()
				return nil
			}
		}
		masterPage.RUnlock()

		if !i.locker.UpgradePageLock(masterPageToken, txns.SimpleLockExclusive) {
			return fmt.Errorf(
				"failed to upgrade page lock %v: %w",
				masterPageID,
				txns.ErrDeadlockPrevention,
			)
		}

		inserts := []struct {
			expectedSlotNum uint16
			data            uint64
		}{
			{bucketsCountSlot, 1},
			{bucketItemSizeSlot, uint64(bucketItemSize)},
			{bucketCapacitySlot, uint64(bucketCapacity)},
			{recordsCountSlot, 0},
			{hashmapTotalCapacitySlot, uint64(bucketCapacity)},
			{startPageIDSlot, 1},
		}

		assert.Assert(
			masterPageSlotsCount == len(inserts),
			"master page slots count mismatch: %d != %d",
			masterPageSlotsCount,
			len(inserts),
		)
		err = i.pool.WithMarkDirty(
			i.logger.GetTxnID(),
			masterPageIdent,
			masterPage,
			func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
				var loc common.LogRecordLocInfo
				lockedPage.Clear()
				for _, insert := range inserts {
					var slot uint16
					var err error
					slot, loc, err = lockedPage.InsertWithLogs(
						utils.ToBytes[uint64](insert.data),
						masterPageIdent,
						i.logger,
					)
					if err != nil {
						return common.NewNilLogRecordLocation(), err
					}

					assert.Assert(
						insert.expectedSlotNum == slot,
						"slot number mismatch: %d != %d",
						insert.expectedSlotNum,
						slot,
					)
				}
				return loc, nil
			},
		)
		return err
	}()

	if err != nil {
		return err
	}

	err = func() error {
		const bucketPageID = 1
		bucketPageIdent := common.PageIdentity{
			FileID: indexMeta.FileID,
			PageID: bucketPageID,
		}

		bucketPageToken := i.locker.LockPage(i.indexFileToken, bucketPageID, txns.SimpleLockShared)
		if bucketPageToken == nil {
			return fmt.Errorf(
				"failed to lock page %v: %w",
				bucketPageID,
				txns.ErrDeadlockPrevention,
			)
		}

		bucketPage, err := i.pool.GetPage(bucketPageIdent)
		if err != nil {
			return fmt.Errorf("failed to get page: %w", err)
		}
		defer i.pool.Unpin(bucketPageIdent)

		bucketPage.RLock()
		if bucketPage.NumSlots() == uint16(bucketCapacity) {
			if !i.debugAssertsEnabled {
				bucketPage.RUnlock()
				return nil
			}
			foundAbnormal := false
			for slotIdx := range bucketPage.NumSlots() {
				if bucketPage.SlotInfo(slotIdx) != page.SlotStatusInserted {
					foundAbnormal = true
				}
			}
			if !foundAbnormal {
				bucketPage.RUnlock()
				return nil
			}
		}
		bucketPage.RUnlock()

		if !i.locker.UpgradePageLock(bucketPageToken, txns.SimpleLockExclusive) {
			return fmt.Errorf(
				"failed to upgrade page lock %v: %w",
				bucketPageID,
				txns.ErrDeadlockPrevention,
			)
		}

		dummyRecord := make([]byte, bucketItemSize)
		err = i.pool.WithMarkDirty(
			i.logger.GetTxnID(),
			bucketPageIdent,
			bucketPage,
			func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
				var loc common.LogRecordLocInfo
				lockedPage.Clear()
				for range bucketCapacity {
					var err error
					_, loc, err = lockedPage.InsertWithLogs(dummyRecord, bucketPageIdent, i.logger)
					if err != nil {
						return common.NewNilLogRecordLocation(), err
					}
					assert.NoErrorWithMessage(err, "impossible")
				}
				slotOpt := lockedPage.UnsafeInsertNoLogs(dummyRecord)
				if slotOpt.IsNone() {
					return loc, nil
				}

				for slotOpt.IsSome() {
					slotOpt = lockedPage.UnsafeInsertNoLogs(dummyRecord)
				}
				assert.Assert(
					false,
					"calculated records limit per bucket didn't match the actual limit. "+
						"precalculated limit: %d, actual limit: %d, record size: %d",
					bucketCapacity,
					lockedPage.NumSlots(),
					bucketItemSize,
				)
				panic("unreachable")
			},
		)
		return err
	}()
	return err
}
