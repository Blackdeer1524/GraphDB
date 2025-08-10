package hash

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"

	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
)

const (
	idxKind = 2

	indexRootPageID = 0
)

var ErrNotFound = fmt.Errorf("not found")

type TableLockRequest struct {
}

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
	ReadPage() (T, error)
	GetPage(kind uint64, pageID uint64) (T, error)
}

type Locker interface {
	GetPageLock(req TableLockRequest, kind uint64, pageID uint64) bool
	GetPageUnlock(req TableLockRequest, kind uint64, pageID uint64) bool
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

	root   rootMetaData
	fileID uint64

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
		fileID: fileID,
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

	return BucketPage[U]{}
}

func (h *Index[T, U]) Search(key U) (RID, error) {
	rootLockReq := TableLockRequest{}

	ok := h.lock.GetPageLock(rootLockReq, idxKind, indexRootPageID)
	if !ok {
		return RID{}, fmt.Errorf("failed to get page lock for index root")
	}
	defer h.lock.GetPageUnlock(rootLockReq, idxKind, indexRootPageID)

	// let's use the least significant bits
	dIdx := hashKey(key) & ((1 << h.root.globalDepth) - 1)

	bucketLockReq := TableLockRequest{}

	bucketPage := h.root.directory[dIdx]

	ok = h.lock.GetPageLock(bucketLockReq, idxKind, bucketPage)
	if !ok {
		return RID{}, fmt.Errorf("failed to get page lock for bucket %d", dIdx)
	}
	defer h.lock.GetPageUnlock(bucketLockReq, idxKind, bucketPage)

	// bucket page is page with row identifiers
	pageWithRIDs, err := h.se.GetPage(idxKind, bucketPage)
	if err != nil {
		return RID{}, fmt.Errorf("failed to get page: %w", err)
	}

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

func (h *Index[T, U]) Delete() {

}
