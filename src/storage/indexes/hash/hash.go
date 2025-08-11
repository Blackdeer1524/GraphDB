package hash

import (
	"errors"
	"fmt"
	"github.com/Blackdeer1524/GraphDB/src/txns"
	"hash/fnv"
)

const (
	idxKind = 2

	indexRootPageID uint64 = 0

	directoryPageCapacity = 512
)

var ErrNotFound = fmt.Errorf("not found")

type Page interface {
	GetData() []byte
	SetData(d []byte)

	SetDirtiness(val bool)
	IsDirty() bool

	// latch methods
	Lock()
	Unlock()
	RLock()
	RUnlock()
}

type StorageEngine[T Page] interface {
	GetPage(pageID uint64, fileID uint64) (T, error)
	UnpinPage(pageID uint64, fileID uint64) error
	WritePage(pageID uint64, fileID uint64, page T) error

	//GetPageWithLock(fileID uint64, pageID uint64, kind uint64, req txns.PageLockRequest) (T, error)
}

type Locker interface {
	GetPageLock(req txns.PageLockRequest) bool
	UpgradePageLock(req txns.PageLockRequest) bool
}

type Index[T Page, U comparable] struct {
	se StorageEngine[T]

	indexFileID uint64

	lock Locker
}

func New[T Page, U comparable](fileID uint64) (*Index[T, U], error) {
	// мне нужны локи уровня страниц
	return &Index[T, U]{
		se: nil,
		//root:   getRootPageMetadata(fst.GetData()),
		indexFileID: fileID,
	}, nil
}

func hashKey[U comparable](value U) uint64 {
	s := fmt.Sprint(value)

	h := fnv.New32a()
	_, _ = h.Write([]byte(s))

	return uint64(h.Sum32())
}

func (h *Index[T, U]) Search(txnID txns.TxnID, key U) (rid RID, err error) {
	rootPageLock := txns.PageLockRequest{
		TxnID:    txnID,
		LockMode: txns.IndexShared,
		PageID:   indexRootPageID,
	}

	if !h.lock.GetPageLock(rootPageLock) {
		return RID{}, errors.New("failed to get lock on index root page")
	}

	rootPage, err := h.se.GetPage(indexRootPageID, h.indexFileID)
	if err != nil {
		return RID{}, fmt.Errorf("failed to get root page: %w", err)
	}
	defer func() {
		err1 := h.se.UnpinPage(indexRootPageID, h.indexFileID)
		if err1 != nil {
			err = errors.Join(err, err1)
		}
	}()

	root := getRootPageMetadata(rootPage.GetData())

	// index of directoryPages that we need. But now we
	// need to find a directoryPages page with this index
	dIdx := hashKey(key) & ((uint64(1) << root.globalDepth) - 1)

	directoryPageID := root.directoryPages[dIdx/directoryPageCapacity]

	directoryPageLock := txns.PageLockRequest{
		TxnID:    txnID,
		LockMode: txns.IndexShared,
		PageID:   directoryPageID,
	}

	if !h.lock.GetPageLock(directoryPageLock) {
		return RID{}, errors.New("failed to get lock on directoryPages page")
	}

	directoryPageBytes, err := h.se.GetPage(directoryPageID, h.indexFileID)
	if err != nil {
		return RID{}, fmt.Errorf("failed to get directoryPages page: %w", err)
	}
	defer func() {
		err1 := h.se.UnpinPage(directoryPageID, h.indexFileID)
		if err1 != nil {
			err = errors.Join(err, err1)
		}
	}()

	directoryPage := getDirectoryPage(directoryPageBytes.GetData())

	bucketPageID := directoryPage.bucketPages[dIdx%directoryPageCapacity]

	bucketPageLock := txns.PageLockRequest{
		TxnID:    txnID,
		LockMode: txns.IndexShared,
		PageID:   bucketPageID,
	}

	if !h.lock.GetPageLock(bucketPageLock) {
		return RID{}, fmt.Errorf("failed to get lock on bucket page: %w", err)
	}

	bucketPageBytes, err := h.se.GetPage(bucketPageID, h.indexFileID)
	if err != nil {
		return RID{}, fmt.Errorf("failed to get bucket page: %w", err)
	}
	defer func() {
		err1 := h.se.UnpinPage(bucketPageID, h.indexFileID)
		if err1 != nil {
			err = errors.Join(err, err1)
		}
	}()

	bucketPage := getBucketPage[U](bucketPageBytes.GetData())

	for i := uint32(0); i < bucketPage.entriesCnt; i++ {
		if bucketPage.entries[i].key == key {
			return bucketPage.entries[i].rid, nil
		}
	}

	return RID{}, ErrNotFound
}

// insertSlow inserts key-value pair but under exclusive lock
// on directoryPages of index. All locks must be occupied by caller
// of this function.
func (h *Index[T, U]) insertSlow(txnID txns.TxnID, key U, rid RID) error {
	panic("not implemented!")

	return nil
}

func (h *Index[T, U]) Insert(txnID txns.TxnID, key U, rid RID) (err error) {
	panic("not implemented!")

	return h.insertSlow(txnID, key, rid)
}

func (h *Index[T, U]) Delete(txnID txns.TxnID, key U) (err error) {
	rootPageLock := txns.PageLockRequest{
		TxnID:    txnID,
		LockMode: txns.IndexShared,
		PageID:   indexRootPageID,
	}

	if !h.lock.GetPageLock(rootPageLock) {
		return errors.New("failed to get lock on index root page")
	}

	rootPage, err := h.se.GetPage(indexRootPageID, h.indexFileID)
	if err != nil {
		return fmt.Errorf("failed to get root page: %w", err)
	}
	defer func() {
		err1 := h.se.UnpinPage(indexRootPageID, h.indexFileID)
		if err1 != nil {
			err = errors.Join(err, err1)
		}
	}()

	root := getRootPageMetadata(rootPage.GetData())

	// index of directoryPages that we need. But now we
	// need to find a directoryPages page with this index
	dIdx := hashKey(key) & ((uint64(1) << root.globalDepth) - 1)

	directoryPageID := root.directoryPages[dIdx/directoryPageCapacity]

	directoryPageLock := txns.PageLockRequest{
		TxnID:    txnID,
		LockMode: txns.IndexShared,
		PageID:   directoryPageID,
	}

	if !h.lock.GetPageLock(directoryPageLock) {
		return errors.New("failed to get lock on directoryPages page")
	}

	directoryPageBytes, err := h.se.GetPage(directoryPageID, h.indexFileID)
	if err != nil {
		return fmt.Errorf("failed to get directoryPages page: %w", err)
	}
	defer func() {
		err1 := h.se.UnpinPage(directoryPageID, h.indexFileID)
		if err1 != nil {
			err = errors.Join(err, err1)
		}
	}()

	directoryPage := getDirectoryPage(directoryPageBytes.GetData())

	bucketPageID := directoryPage.bucketPages[dIdx%directoryPageCapacity]

	bucketPageLock := txns.PageLockRequest{
		TxnID:    txnID,
		LockMode: txns.IndexExclusive,
		PageID:   bucketPageID,
	}

	if !h.lock.GetPageLock(bucketPageLock) {
		return fmt.Errorf("failed to get lock on bucket page: %w", err)
	}

	bucketPageBytes, err := h.se.GetPage(bucketPageID, h.indexFileID)
	if err != nil {
		return fmt.Errorf("failed to get bucket page: %w", err)
	}
	defer func() {
		err1 := h.se.UnpinPage(bucketPageID, h.indexFileID)
		if err1 != nil {
			err = errors.Join(err, err1)
		}
	}()

	bucketPage := getBucketPage[U](bucketPageBytes.GetData())

	var found bool

	newEntries := make([]KeyWithRID[U], 0, bucketPage.entriesCnt)

	for i := uint32(0); i < bucketPage.entriesCnt; i++ {
		if bucketPage.entries[i].key == key {
			found = true

			continue
		}

		newEntries = append(newEntries, bucketPage.entries[i])
	}

	if !found {
		return ErrNotFound
	}

	bucketPage.entries = newEntries
	bucketPage.entriesCnt = uint32(len(newEntries))

	newData, err := bucketPageToBytes(bucketPage)
	if err != nil {
		return fmt.Errorf("failed to serialize bucket: %w", err)
	}

	bucketPageBytes.SetData(newData)
	bucketPageBytes.SetDirtiness(true)

	return nil
}
