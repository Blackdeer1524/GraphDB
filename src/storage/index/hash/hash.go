package hash

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"hash/fnv"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
)

const (
	indexRootPageID         common.PageID = 0
	overflowPageNotExist                  = ^uint64(0)
	metaPageDirPagesCount                 = 100
	dirPageBucketsPtrsCount               = 100
	ridBytes                              = 8 + 8 + 2 + 2
)

var (
	ErrKeyNotFound         = errors.New("key not found")
	errKeyNotInsertedRetry = errors.New("key not found")
)

type StorageEngine interface {
	GetPage(pageID uint64, fileID uint64) (*page.SlottedPage, error)
	UnpinPage(pageID uint64, fileID uint64) error
	WritePage(pageID uint64, fileID uint64, p *page.SlottedPage) error
	Unpin(pageID common.PageIdentity)
}

type Index struct {
	se StorageEngine

	indexFileID common.FileID

	keySize int

	recordsCountInBucket uint16
}

func New(indexFileID common.FileID, se StorageEngine, keySize int) (*Index, error) {
	return &Index{
		se:                   se,
		indexFileID:          indexFileID,
		keySize:              keySize,
		recordsCountInBucket: uint16(3 * 1024 / keySize),
	}, nil
}

func hashKeyToUint64(key []byte) uint64 {
	hasher := fnv.New64a()
	_, _ = hasher.Write(key)

	return hasher.Sum64()
}

type rootPage struct {
	p              *page.SlottedPage
	d              uint16
	overflowPageID uint64
	pageID         uint64
}

type dirPage struct {
	p      *page.SlottedPage
	pageID uint64
}

type bucketPage struct {
	p      *page.SlottedPage
	l      uint16
	pageID uint64
}

type entrySuffix struct {
	FileID  common.FileID
	PageID  common.PageID
	SlotNum uint16
	Exist   uint16
}

func (bp *bucketPage) find(key []byte) (*common.RecordID, error) {
	ns := bp.p.NumSlots()

	keyLen := uint16(len(key))

	// skip slot with local depth
	for i := uint16(1); i < ns; i++ {
		slot := bp.p.Read(i)
		if uint16(len(slot)) != keyLen+ridBytes {
			continue
		}

		if !bytes.Equal(slot[:keyLen], key) {
			continue
		}

		var suffix entrySuffix
		if err := binary.Read(bytes.NewReader(slot[keyLen:]), binary.BigEndian, &suffix); err != nil {
			continue
		}

		if suffix.Exist == 0 {
			continue
		}

		rid := &common.RecordID{
			FileID:  suffix.FileID,
			PageID:  suffix.PageID,
			SlotNum: suffix.SlotNum,
		}

		return rid, nil
	}

	return nil, ErrKeyNotFound
}

func (bp *bucketPage) delete(key []byte) (common.RecordID, error) {
	ns := bp.p.NumSlots()

	keyLen := uint16(len(key))

	for i := uint16(1); i < ns; i++ {
		slot := bp.p.Read(i)
		if uint16(len(slot)) != keyLen+ridBytes {
			continue
		}

		if !bytes.Equal(slot[:keyLen], key) {
			continue
		}

		var suffix entrySuffix
		if err := binary.Read(bytes.NewReader(slot[keyLen:]), binary.BigEndian, &suffix); err != nil {
			continue
		}

		if suffix.Exist == 0 {
			continue
		}

		suffix.Exist = 0

		buf := new(bytes.Buffer)
		if err := binary.Write(buf, binary.BigEndian, suffix); err != nil {
			return common.RecordID{}, err
		}

		data := make([]byte, 0, int(keyLen)+ridBytes)

		data = append(data, key...)
		data = append(data, buf.Bytes()...)

		bp.p.Update(i, data)

		rid := common.RecordID{
			FileID:  suffix.FileID,
			PageID:  suffix.PageID,
			SlotNum: suffix.SlotNum,
		}

		return rid, nil
	}

	return common.RecordID{}, ErrKeyNotFound
}

func (bp *bucketPage) insert(key []byte, rid common.RecordID, recordsCountInBucket uint16) (bool, error) {
	ns := bp.p.NumSlots()

	keyLen := uint16(len(key))

	// try to reuse a previously deleted slot (exist == 0)
	for i := uint16(1); i < ns; i++ {
		slot := bp.p.Read(i)
		if uint16(len(slot)) != keyLen+ridBytes {
			continue
		}

		var suffix entrySuffix
		if err := binary.Read(bytes.NewReader(slot[keyLen:]), binary.BigEndian, &suffix); err != nil {
			continue
		}

		if suffix.Exist == 0 {
			data := make([]byte, 0, int(keyLen)+ridBytes)

			data = append(data, key...)

			suffix = entrySuffix{
				FileID:  rid.FileID,
				PageID:  rid.PageID,
				SlotNum: rid.SlotNum,
				Exist:   1,
			}

			buf := new(bytes.Buffer)

			err := binary.Write(buf, binary.BigEndian, suffix)
			if err != nil {
				return false, err
			}

			data = append(data, buf.Bytes()...)

			bp.p.Update(i, data)

			return true, nil
		}
	}

	// cannot reuse, lets allocate new slot

	if ns == recordsCountInBucket+1 {
		return false, errors.New("bucket page is full")
	}

	suffix := entrySuffix{
		FileID:  rid.FileID,
		PageID:  rid.PageID,
		SlotNum: rid.SlotNum,
		Exist:   1,
	}

	buf := new(bytes.Buffer)

	err := binary.Write(buf, binary.BigEndian, suffix)
	if err != nil {
		return false, err
	}

	data := make([]byte, 0, int(keyLen)+ridBytes)
	data = append(data, key...)
	data = append(data, buf.Bytes()...)

	sl := bp.p.Insert(data)
	if sl.IsNone() {
		return false, errors.New("failed to insert to bucket")
	}

	return true, nil
}

func (h *Index) getRootPage() (*rootPage, error) {
	rp, err := h.se.GetPage(uint64(indexRootPageID), uint64(h.indexFileID))
	if err != nil {
		return nil, fmt.Errorf("failed to get root page: %w", err)
	}

	if rp.NumSlots() < 2 {
		return nil, errors.New("corrupted root page: not enough header slots")
	}

	overflowPageID := utils.FromBytes[uint64](rp.Read(0))
	d := utils.FromBytes[uint16](rp.Read(1))

	return &rootPage{
		p:              rp,
		d:              d,
		overflowPageID: overflowPageID,
		pageID:         uint64(indexRootPageID),
	}, nil
}

func (h *Index) getMetaPage(rp *rootPage, num uint64) (*rootPage, error) {
	if num == 0 {
		return rp, nil
	}

	currentPageID := rp.overflowPageID
	if currentPageID == overflowPageNotExist {
		return nil, errors.New("meta page does not exist")
	}

	var (
		cur      *page.SlottedPage
		err      error
		overflow uint64
	)

	for i := uint64(1); i <= num; i++ {
		cur, err = h.se.GetPage(currentPageID, uint64(h.indexFileID))
		if err != nil {
			return nil, fmt.Errorf("failed to get meta page %d: %w", i, err)
		}

		overflow = utils.FromBytes[uint64](cur.Read(0))

		if i < num {
			if overflow == overflowPageNotExist {
				h.se.Unpin(common.PageIdentity{
					FileID: h.indexFileID,
					PageID: common.PageID(currentPageID),
				})

				return nil, errors.New("chain of overflow pages ended early")
			}

			h.se.Unpin(common.PageIdentity{
				FileID: h.indexFileID,
				PageID: common.PageID(currentPageID),
			})

			currentPageID = overflow
		}
	}

	assert.Assert(cur != nil, "cur page is nil")

	return &rootPage{
		p:              cur,
		overflowPageID: overflow,
		pageID:         currentPageID,
	}, nil
}

func (h *Index) getDirPage(rp *rootPage, num uint64) (*dirPage, error) {
	ns := rp.p.NumSlots()

	// because one slot is for overflow page and second is for global depth
	if uint64(ns-2) <= num {
		return nil, errors.New("dir page not exist")
	}

	dirPageID := utils.FromBytes[uint64](rp.p.Read(uint16(num + 2)))

	dp, err := h.se.GetPage(dirPageID, uint64(h.indexFileID))
	if err != nil {
		return nil, fmt.Errorf("failed to get dir page: %w", err)
	}

	return &dirPage{
		p:      dp,
		pageID: dirPageID,
	}, nil
}

func (h *Index) getBucketPage(dp *dirPage, num uint16) (*bucketPage, error) {
	ns := dp.p.NumSlots()

	if ns <= num {
		return nil, errors.New("bucket page not exist")
	}

	slot := dp.p.Read(num)
	if len(slot) < 8 {
		return nil, errors.New("corrupted dir page slot: too small for pageID")
	}

	bucketPageID := utils.FromBytes[uint64](slot)

	bp, err := h.se.GetPage(bucketPageID, uint64(h.indexFileID))
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket page: %w", err)
	}

	return &bucketPage{
		p:      bp,
		l:      utils.FromBytes[uint16](bp.Read(0)),
		pageID: bucketPageID,
	}, nil
}

func (h *Index) Get(key []byte) (*common.RecordID, error) {
	if len(key) != h.keySize {
		return nil, errors.New("key size is not equal to index key size")
	}

	rp, err := h.getRootPage()
	if err != nil {
		return nil, fmt.Errorf("failed to get root page: %w", err)
	}
	defer h.se.Unpin(common.PageIdentity{
		FileID: h.indexFileID,
		PageID: common.PageID(rp.pageID),
	})

	keyHash := hashKeyToUint64(key)

	index := keyHash & ((1 << rp.d) - 1)

	// calc an index of meta page
	mpIndex := index / (metaPageDirPagesCount * dirPageBucketsPtrsCount)

	mp, err := h.getMetaPage(rp, mpIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to get meta page: %w", err)
	}
	if mp.pageID != rp.pageID {
		defer h.se.Unpin(common.PageIdentity{
			FileID: h.indexFileID,
			PageID: common.PageID(mp.pageID),
		})
	}

	mpOffset := index % (metaPageDirPagesCount * dirPageBucketsPtrsCount)

	dirSlot := mpOffset / dirPageBucketsPtrsCount
	bucketSlot := uint16(mpOffset % dirPageBucketsPtrsCount)

	dp, err := h.getDirPage(mp, dirSlot)
	if err != nil {
		return nil, fmt.Errorf("failed to get dir page: %w", err)
	}
	defer h.se.Unpin(common.PageIdentity{
		FileID: h.indexFileID,
		PageID: common.PageID(dp.pageID),
	})

	bucketPtr, err := h.getBucketPage(dp, bucketSlot)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket ptr: %w", err)
	}
	defer h.se.Unpin(common.PageIdentity{
		FileID: h.indexFileID,
		PageID: common.PageID(bucketPtr.pageID),
	})

	rid, err := bucketPtr.find(key)
	if err != nil {
		return nil, fmt.Errorf("failed to find key: %w", err)
	}

	return rid, nil
}

func (h *Index) Delete(key []byte) (common.RecordID, error) {
	if len(key) != h.keySize {
		return common.RecordID{}, errors.New("key size is not equal to index key size")
	}

	rp, err := h.getRootPage()
	if err != nil {
		return common.RecordID{}, fmt.Errorf("failed to get root page: %w", err)
	}
	defer h.se.Unpin(common.PageIdentity{
		FileID: h.indexFileID,
		PageID: common.PageID(rp.pageID),
	})

	keyHash := hashKeyToUint64(key)

	index := keyHash & ((1 << rp.d) - 1)

	// calc an index of meta page
	mpIndex := index / (metaPageDirPagesCount * dirPageBucketsPtrsCount)

	mp, err := h.getMetaPage(rp, mpIndex)
	if err != nil {
		return common.RecordID{}, fmt.Errorf("failed to get meta page: %w", err)
	}
	if mp.pageID != rp.pageID {
		defer h.se.Unpin(common.PageIdentity{
			FileID: h.indexFileID,
			PageID: common.PageID(mp.pageID),
		})
	}

	mpOffset := index % (metaPageDirPagesCount * dirPageBucketsPtrsCount)

	dirSlot := mpOffset / dirPageBucketsPtrsCount
	bucketSlot := uint16(mpOffset % dirPageBucketsPtrsCount)

	dp, err := h.getDirPage(mp, dirSlot)
	if err != nil {
		return common.RecordID{}, fmt.Errorf("failed to get dir page: %w", err)
	}
	defer h.se.Unpin(common.PageIdentity{
		FileID: h.indexFileID,
		PageID: common.PageID(dp.pageID),
	})

	bucketPtr, err := h.getBucketPage(dp, bucketSlot)
	if err != nil {
		return common.RecordID{}, fmt.Errorf("failed to get bucket ptr: %w", err)
	}
	defer h.se.Unpin(common.PageIdentity{
		FileID: h.indexFileID,
		PageID: common.PageID(bucketPtr.pageID),
	})

	rid, err := bucketPtr.delete(key)
	if err != nil {
		return common.RecordID{}, fmt.Errorf("failed to delete key: %w", err)
	}

	err = h.se.WritePage(bucketPtr.pageID, uint64(h.indexFileID), bucketPtr.p)
	if err != nil {
		return common.RecordID{}, fmt.Errorf("failed to write bucket page: %w", err)
	}

	return rid, nil
}

func (h *Index) tryInsert(rp *rootPage, key []byte, rid common.RecordID) error {
	keyHash := hashKeyToUint64(key)

	index := keyHash & ((1 << rp.d) - 1)

	// calc an index of meta page
	mpIndex := index / (metaPageDirPagesCount * dirPageBucketsPtrsCount)

	mp, err := h.getMetaPage(rp, mpIndex)
	if err != nil {
		return fmt.Errorf("failed to get meta page: %w", err)
	}
	if mp.pageID != rp.pageID {
		defer h.se.Unpin(common.PageIdentity{
			FileID: h.indexFileID,
			PageID: common.PageID(mp.pageID),
		})
	}

	mpOffset := index % (metaPageDirPagesCount * dirPageBucketsPtrsCount)

	dirSlot := mpOffset / dirPageBucketsPtrsCount
	bucketSlot := uint16(mpOffset % dirPageBucketsPtrsCount)

	dp, err := h.getDirPage(mp, dirSlot)
	if err != nil {
		return fmt.Errorf("failed to get dir page: %w", err)
	}
	defer h.se.Unpin(common.PageIdentity{
		FileID: h.indexFileID,
		PageID: common.PageID(dp.pageID),
	})

	bucketPtr, err := h.getBucketPage(dp, bucketSlot)
	if err != nil {
		return fmt.Errorf("failed to get bucket ptr: %w", err)
	}
	defer h.se.Unpin(common.PageIdentity{
		FileID: h.indexFileID,
		PageID: common.PageID(bucketPtr.pageID),
	})

	// only unique keys are allowed
	_, err = bucketPtr.find(key)
	if err == nil {
		return errors.New("key already exists")
	}

	if !errors.Is(err, ErrKeyNotFound) {
		return fmt.Errorf("failed to find key: %w", err)
	}

	inserted, err := bucketPtr.insert(key, rid, h.recordsCountInBucket)
	if err != nil {
		return fmt.Errorf("failed to delete key: %w", err)
	}

	// if inserted to bucket page, we are done
	// else we need to rebuild
	if inserted {
		return nil
	}

	return errKeyNotInsertedRetry
}

func (h *Index) Insert(key []byte, rid common.RecordID) error {
	if len(key) != h.keySize {
		return errors.New("key size is not equal to index key size")
	}

	rp, err := h.getRootPage()
	if err != nil {
		return fmt.Errorf("failed to get root page: %w", err)
	}
	defer h.se.Unpin(common.PageIdentity{
		FileID: h.indexFileID,
		PageID: common.PageID(rp.pageID),
	})

	for {
		err = h.tryInsert(rp, key, rid)
		if err != nil {
			if errors.Is(err, errKeyNotInsertedRetry) {
				continue
			}

			return fmt.Errorf("failed to insert key: %w", err)
		}

		return nil
	}
}
