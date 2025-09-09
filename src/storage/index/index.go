package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/maphash"
	"log"
	"unsafe"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

const hashmapLoadFactor = 0.7

type LinearProbingIndex struct {
	hasher maphash.Hash

	indexFileToken *txns.FileLockToken
	masterPage     *page.SlottedPage
	keySize        int
	pool           bufferpool.BufferPool
	locker         txns.ILockManager
	logger         common.ITxnLoggerWithContext
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
		hasher:         maphash.Hash{},
		masterPage:     masterPage,
		pool:           pool,
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

	log.Printf("key=%x Get: starting lookup for key", key)

	pToken := i.locker.LockPage(i.indexFileToken, masterPageID, txns.PageLockShared)
	if pToken == nil {
		return common.RecordID{}, fmt.Errorf("failed to lock page: %v", masterPageID)
	}

	bucketCapacity := utils.FromBytes[uint64](i.masterPage.LockedRead(bucketCapacitySlot))
	recordsLimit := utils.FromBytes[uint64](i.masterPage.LockedRead(hashmapTotalCapacitySlot))
	startPageID := utils.FromBytes[common.PageID](i.masterPage.LockedRead(startPageIDSlot))

	log.Printf(
		"key=%x Get: bucketCapacity=%d, recordsLimit=%d, startPageID=%d",
		key,
		bucketCapacity,
		recordsLimit,
		startPageID,
	)

	i.hasher.Reset()
	i.hasher.Write(key)
	startArrayIndex := i.hasher.Sum64() % recordsLimit
	k := startArrayIndex

	log.Printf("key=%x Get: startArrayIndex=%d, starting probe from k=%d", key, startArrayIndex, k)

	for {
		bucketIndex := k / bucketCapacity
		slotNumber := uint16(k % bucketCapacity)

		bucketItemPageID := startPageID + common.PageID(bucketIndex)

		log.Printf(
			"key=%x Get: probing k=%d, bucketIndex=%d, slotNumber=%d, bucketItemPageID=%d",
			key,
			k,
			bucketIndex,
			slotNumber,
			bucketItemPageID,
		)

		pToken := i.locker.LockPage(i.indexFileToken, bucketItemPageID, txns.PageLockShared)
		if pToken == nil {
			return common.RecordID{}, fmt.Errorf(
				"failed to lock page: %v",
				bucketItemPageID,
			)
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

		log.Printf("key=%x Get: slot status=%d, itemKey=%q at k=%d", key, status, itemKey, k)

		switch status {
		case bucketItemStatusInserted:
			if itemKey == string(key) {
				log.Printf("key=%x Get: found matching key at k=%d, returning rid=%+v", key, k, rid)
				return rid, nil
			}
			log.Printf(
				"key=%x Get: slot occupied by different key %q, continuing probe",
				key,
				itemKey,
			)
		case bucketItemStatusDeleted:
			log.Printf("key=%x Get: slot was deleted, continuing probe", key)
		case bucketItemStatusFree:
			log.Printf("key=%x Get: found free slot at k=%d, key not found", key, k)
			return common.RecordID{}, storage.ErrKeyNotFound
		}

		k = (k + 1) % recordsLimit
		log.Printf("key=%x Get: incrementing k to %d (recordsLimit=%d)", key, k, recordsLimit)
		assert.Assert(k != startArrayIndex, "k == startArrayIndex. Should have grown the index")
	}
}

func (i *LinearProbingIndex) Delete(key []byte) error {
	assert.Assert(len(key) == i.keySize, "key size mismatch")

	pToken := i.locker.LockPage(i.indexFileToken, masterPageID, txns.PageLockShared)
	if pToken == nil {
		return fmt.Errorf("failed to lock page: %v", masterPageID)
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

		pToken := i.locker.LockPage(i.indexFileToken, bucketItemPageID, txns.PageLockShared)
		if pToken == nil {
			err := fmt.Errorf("failed to lock page: %v", bucketItemPageID)
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

				if !i.locker.UpgradePageLock(pToken, txns.PageLockExclusive) {
					return true, fmt.Errorf("failed to upgrade page lock: %v", bucketPageIdent)
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

	masterPageToken := i.locker.LockPage(i.indexFileToken, masterPageID, txns.PageLockShared)
	if masterPageToken == nil {
		return fmt.Errorf("failed to lock page: %v", masterPageID)
	}

	bucketItemSize := utils.FromBytes[uint64](i.masterPage.LockedRead(bucketItemSizeSlot))
	bucketCapacity := utils.FromBytes[uint64](i.masterPage.LockedRead(bucketCapacitySlot))
	recordsCount := utils.FromBytes[uint64](i.masterPage.LockedRead(recordsCountSlot))
	recordsLimit := utils.FromBytes[uint64](i.masterPage.LockedRead(hashmapTotalCapacitySlot))

	log.Printf(
		"key=%x Insert: key=%x, rid=%+v, recordsCount=%d, recordsLimit=%d",
		key,
		key,
		rid,
		recordsCount,
		recordsLimit,
	)

	if float64(recordsCount)/float64(recordsLimit) > hashmapLoadFactor {
		log.Printf(
			"key=%x Insert: load factor exceeded, growing index (recordsCount=%d, recordsLimit=%d)",
			key,
			recordsCount,
			recordsLimit,
		)
		if err := i.grow(); err != nil {
			return err
		}
		recordsLimit = utils.FromBytes[uint64](i.masterPage.LockedRead(hashmapTotalCapacitySlot))
		log.Printf("key=%x Insert: after grow, new recordsLimit=%d", key, recordsLimit)
	}
	startPageID := utils.FromBytes[common.PageID](i.masterPage.LockedRead(startPageIDSlot))

	i.hasher.Reset()
	i.hasher.Write(key)
	startArrayIndex := i.hasher.Sum64() % recordsLimit
	k := startArrayIndex

	log.Printf(
		"key=%x Insert: startArrayIndex=%d, startPageID=%d",
		key,
		startArrayIndex,
		startPageID,
	)

	for {
		bucketIndex := k / bucketCapacity
		slotNumber := uint16(k % bucketCapacity)

		bucketItemPageID := startPageID + common.PageID(bucketIndex)

		log.Printf(
			"key=%x Insert: probing k=%d, bucketIndex=%d, slotNumber=%d, bucketItemPageID=%d",
			key,
			k,
			bucketIndex,
			slotNumber,
			bucketItemPageID,
		)

		bucketToken := i.locker.LockPage(i.indexFileToken, bucketItemPageID, txns.PageLockShared)
		if bucketToken == nil {
			err := fmt.Errorf("failed to lock page: %v", bucketItemPageID)
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

			log.Printf("key=%x Insert: slot status=%d, itemKey=%q at k=%d", key, status, itemKey, k)

			switch status {
			case bucketItemStatusInserted:
				// enforcing a unique constraint
				assert.Assert(itemKey != string(key))
				log.Printf(
					"key=%x Insert: slot occupied by different key %q, continuing probe",
					key,
					itemKey,
				)
				return false, nil
			case bucketItemStatusDeleted:
				log.Printf("key=%x Insert: slot was deleted, continuing probe", key)
				return false, nil
			case bucketItemStatusFree:
				log.Printf("key=%x Insert: found free slot at k=%d, inserting key=%x", key, k, key)
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
				if !i.locker.UpgradePageLock(masterPageToken, txns.PageLockExclusive) {
					return true, fmt.Errorf("failed to upgrade page lock: %v", masterPageID)
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

				if !i.locker.UpgradePageLock(bucketToken, txns.PageLockExclusive) {
					return true, fmt.Errorf("failed to upgrade page lock: %v", bucketPageIdent)
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
				log.Printf(
					"key=%x Insert: successfully inserted key=%x at k=%d, pageID=%d, slotNum=%d",
					key,
					key,
					k,
					bucketItemPageID,
					slotNumber,
				)
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
		return fmt.Errorf("failed to upgrade file lock: %v", i.indexFileToken.GetFileID())
	}
	masterPageToken := i.locker.LockPage(i.indexFileToken, masterPageID, txns.PageLockExclusive)
	assert.Assert(masterPageToken != nil)

	bucketsCount := utils.FromBytes[uint64](i.masterPage.LockedRead(bucketsCountSlot))
	startPageID := utils.FromBytes[common.PageID](i.masterPage.LockedRead(startPageIDSlot))
	bucketCapacity := utils.FromBytes[uint64](i.masterPage.LockedRead(bucketCapacitySlot))
	bucketItemSize := utils.FromBytes[uint64](i.masterPage.LockedRead(bucketItemSizeSlot))

	dummyRecord := make([]byte, bucketItemSize)
	for k := range bucketsCount * 2 {
		newPageID := startPageID + common.PageID(bucketsCount) + common.PageID(k)

		pToken := i.locker.LockPage(i.indexFileToken, newPageID, txns.PageLockExclusive)
		assert.Assert(pToken != nil, "already aquired a file lock")

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
				common.NilTxnID,
				newBucketPageIdent,
				pg,
				func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
					lockedPage.Clear()
					for range bucketCapacity {
						slotOpt := lockedPage.UnsafeInsertNoLogs(dummyRecord)
						assert.Assert(slotOpt.IsSome(), "impossible, because the page is empty")
					}
					return common.NewNilLogRecordLocation(), nil
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
		prevGenBucketPageToken := i.locker.LockPage(i.indexFileToken, k, txns.PageLockShared)
		assert.Assert(prevGenBucketPageToken != nil, "already aquired a file lock")

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
	masterPageIdent := getMasterPageIdent(i.indexFileToken.GetFileID())
	i.pool.Unpin(masterPageIdent)
	return nil
}
