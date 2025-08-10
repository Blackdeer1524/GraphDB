package hash

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/Blackdeer1524/GraphDB/src/txns"
	"hash/fnv"

	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
)

const (
	idxKind = 2

	indexRootPageID = 0
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
	GetPage(kind uint64, id uint64, pageID uint64) (T, error)
	UnpinPage(kind uint64, id uint64, pageID uint64) error
	WritePage(kind uint64, id uint64, pageID uint64, page T) error
}

type Locker interface {
	GetPageLock(req txns.IndexLockRequest, kind uint64) bool
	GetPageUnlock(req txns.IndexLockRequest, kind uint64) bool
}

type rootMetaData struct {
	magic         [4]byte
	globalDepth   uint32
	maxBucketSize uint32
	directory     []uint64
	checksum      uint64
}

type Index[T Page, U comparable] struct {
	se StorageEngine[T]

	root    rootMetaData
	indexID uint64

	lock Locker
}

type RID struct {
	PageID uint64
	SlotID uint16
}

type KeyWithRID[U comparable] struct {
	rid RID
	key U
}

type BucketPage[U comparable] struct {
	localDepth uint32
	entriesCnt uint32
	entries    []KeyWithRID[U]
}

func rootPageMetadata(m rootMetaData) []byte {
	assert.Assert(len(m.directory) == (1 << m.globalDepth))

	data := make([]byte, page.PageSize)

	data[0] = m.magic[0]
	data[1] = m.magic[1]
	data[2] = m.magic[2]
	data[3] = m.magic[3]

	binary.LittleEndian.PutUint32(data[4:], m.globalDepth)

	binary.LittleEndian.PutUint32(data[8:], m.maxBucketSize)

	binary.LittleEndian.PutUint64(data[page.PageSize-8:], m.checksum)

	l := 8 + 4

	for _, v := range m.directory {
		binary.LittleEndian.PutUint64(data[l:], v)
		l += 8
	}

	return data
}

func getRootPageMetadata(data []byte) rootMetaData {
	assert.Assert(len(data) == page.PageSize)

	var res rootMetaData

	res.magic[0] = data[0]
	res.magic[1] = data[1]
	res.magic[2] = data[2]
	res.magic[3] = data[3]

	res.globalDepth = binary.LittleEndian.Uint32(data[4:])
	res.maxBucketSize = binary.LittleEndian.Uint32(data[8:])
	res.checksum = binary.LittleEndian.Uint64(data[page.PageSize-8:])

	res.directory = make([]uint64, 0, res.globalDepth)

	l := 8 + 4
	for i := 0; i < (1 << res.globalDepth); i++ {
		res.directory = append(res.directory, binary.LittleEndian.Uint64(data[l:]))
		l += 8
	}

	return res
}

func New[T Page, U comparable](fst Page, fileID uint64) (*Index[T, U], error) {
	// мне нужны локи уровня страниц
	return &Index[T, U]{
		se: nil,
		//root:   getRootPageMetadata(fst.GetData()),
		indexID: fileID,
	}, nil
}

func hashKey[U comparable](value U) uint64 {
	s := fmt.Sprint(value)

	h := fnv.New32a()
	h.Write([]byte(s))

	return uint64(h.Sum32())
}

func getBucketPage[U comparable](data []byte) BucketPage[U] {
	assert.Assert(len(data) == page.PageSize)

	localDepth := binary.LittleEndian.Uint32(data[0:])
	entriesCnt := binary.LittleEndian.Uint32(data[4:])

	entries := make([]KeyWithRID[U], 0, entriesCnt)

	l := 8

	for i := 0; i < int(entriesCnt); i++ {
		if l+4 > len(data) {
			break
		}
		keyLength := binary.LittleEndian.Uint32(data[l : l+4])
		l += 4

		if l+int(keyLength)+10 > len(data) {
			break
		}

		var key U

		buf := bytes.NewBuffer(data[l : l+int(keyLength)])

		decoder := gob.NewDecoder(buf)
		if err := decoder.Decode(&key); err != nil {
			continue
		}

		l += int(keyLength)

		pageID := binary.LittleEndian.Uint64(data[l : l+8])
		slotID := binary.LittleEndian.Uint16(data[l+8 : l+10])
		l += 10

		entries = append(entries, KeyWithRID[U]{
			key: key,
			rid: RID{
				PageID: pageID,
				SlotID: slotID,
			},
		})
	}

	return BucketPage[U]{
		localDepth: localDepth,
		entriesCnt: entriesCnt,
		entries:    entries,
	}
}

func bucketPageToBytes[U comparable](bucket BucketPage[U]) ([]byte, error) {
	data := make([]byte, page.PageSize)

	binary.LittleEndian.PutUint32(data[0:4], bucket.localDepth)
	binary.LittleEndian.PutUint32(data[4:8], bucket.entriesCnt)

	l := 8

	for _, entry := range bucket.entries {
		var buf bytes.Buffer
		encoder := gob.NewEncoder(&buf)
		if err := encoder.Encode(entry.key); err != nil {
			return nil, fmt.Errorf("failed to encode key: %w", err)
		}
		keyBytes := buf.Bytes()
		keyLength := uint32(len(keyBytes))

		if l+4+int(keyLength)+10 > page.PageSize {
			return nil, fmt.Errorf("bucket data exceeds page size")
		}

		binary.LittleEndian.PutUint32(data[l:l+4], keyLength)
		l += 4

		copy(data[l:l+int(keyLength)], keyBytes)
		l += int(keyLength)

		binary.LittleEndian.PutUint64(data[l:l+8], entry.rid.PageID)
		binary.LittleEndian.PutUint16(data[l+8:l+10], entry.rid.SlotID)

		l += 10
	}

	return data, nil
}

func (h *Index[T, U]) Search(txnID txns.TxnID, key U) (RID, error) {
	rootLockReq := txns.IndexLockRequest{
		TxnID:    txnID,
		LockMode: txns.IndexShared,
		PageID:   indexRootPageID,
	}

	if !h.lock.GetPageLock(rootLockReq, idxKind) {
		return RID{}, fmt.Errorf("failed to get page lock for index root")
	}
	defer h.lock.GetPageUnlock(rootLockReq, idxKind)

	rootPage, err := h.se.GetPage(idxKind, h.indexID, indexRootPageID)
	if err != nil {
		return RID{}, fmt.Errorf("failed to get root page: %w", err)
	}
	defer h.se.UnpinPage(idxKind, h.indexID, indexRootPageID)

	h.root = getRootPageMetadata(rootPage.GetData())

	dIdx := hashKey(key) & ((1 << h.root.globalDepth) - 1)

	bucketPage := h.root.directory[dIdx]

	bucketLockReq := txns.IndexLockRequest{
		TxnID:    txnID,
		LockMode: txns.IndexShared,
		PageID:   bucketPage,
	}

	if !h.lock.GetPageLock(bucketLockReq, idxKind) {
		return RID{}, fmt.Errorf("failed to get page lock for bucket %d", dIdx)
	}
	defer h.lock.GetPageUnlock(bucketLockReq, idxKind)

	pageWithRIDs, err := h.se.GetPage(idxKind, h.indexID, bucketPage)
	if err != nil {
		return RID{}, fmt.Errorf("failed to get page: %w", err)
	}
	defer h.se.UnpinPage(idxKind, h.indexID, bucketPage)

	bucketPageSt := getBucketPage[U](pageWithRIDs.GetData())

	for _, v := range bucketPageSt.entries {
		if v.key == key {
			return v.rid, nil
		}
	}

	return RID{}, ErrNotFound
}

func (h *Index[T, U]) Insert() {

}

func (h *Index[T, U]) Delete(txnID txns.TxnID, key U) error {
	rootLockReq := txns.IndexLockRequest{
		TxnID:    txnID,
		LockMode: txns.IndexShared,
		PageID:   indexRootPageID,
	}
	if !h.lock.GetPageLock(rootLockReq, idxKind) {
		return fmt.Errorf("failed to get page lock for index root")
	}
	defer h.lock.GetPageUnlock(rootLockReq, idxKind)

	rootPage, err := h.se.GetPage(idxKind, h.indexID, indexRootPageID)
	if err != nil {
		return fmt.Errorf("failed to get root page: %w", err)
	}
	defer h.se.UnpinPage(idxKind, h.indexID, indexRootPageID)

	h.root = getRootPageMetadata(rootPage.GetData())

	dIdx := hashKey(key) & ((1 << h.root.globalDepth) - 1)
	bucketPage := h.root.directory[dIdx]

	bucketLockReq := txns.IndexLockRequest{
		TxnID:    txnID,
		LockMode: txns.IndexExclusive,
		PageID:   bucketPage,
	}
	if !h.lock.GetPageLock(bucketLockReq, idxKind) {
		return fmt.Errorf("failed to get page lock for bucket %d", bucketPage)
	}
	defer h.lock.GetPageUnlock(bucketLockReq, idxKind)

	pageWithRIDs, err := h.se.GetPage(idxKind, h.indexID, bucketPage)
	if err != nil {
		return fmt.Errorf("failed to get page: %w", err)
	}
	defer h.se.UnpinPage(idxKind, h.indexID, bucketPage)

	bucketPageSt := getBucketPage[U](pageWithRIDs.GetData())

	newEntries := make([]KeyWithRID[U], 0, len(bucketPageSt.entries))
	found := false

	for _, v := range bucketPageSt.entries {
		if v.key == key {
			found = true

			continue
		}

		newEntries = append(newEntries, v)
	}

	if !found {
		return ErrNotFound
	}

	bucketPageSt.entries = newEntries
	bucketPageSt.entriesCnt = uint32(len(newEntries))

	newData, err := bucketPageToBytes(bucketPageSt)
	if err != nil {
		return fmt.Errorf("failed to serialize bucket: %w", err)
	}

	pageWithRIDs.SetData(newData)
	pageWithRIDs.SetDirtiness(true)

	err = h.se.WritePage(idxKind, h.indexID, bucketPage, pageWithRIDs)
	if err != nil {
		return fmt.Errorf("failed to write page: %w", err)
	}

	return nil
}
