package hash

import (
	"bytes"
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

	// FileID + PageID + SlotNum + Existence
	ridBytes = 8 + 8 + 2 + 2
)

var (
	ErrKeyNotFound = errors.New("key not found")
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
}

func New(indexFileID common.FileID, se StorageEngine, keySize int) (*Index, error) {
	return &Index{
		se:          se,
		indexFileID: indexFileID,
		keySize:     keySize,
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

func (bp *bucketPage) find(key []byte) (*common.RecordID, error) {
	ns := bp.p.NumSlots()

	keyLen := uint16(len(key))

	for i := uint16(1); i < ns; i++ {
		sl := bp.p.Read(i)
		if uint16(len(sl)) != keyLen+ridBytes {
			continue
		}

		if !bytes.Equal(sl[:keyLen], key) {
			continue
		}

		base := keyLen
		fid := utils.FromBytes[uint64](sl[base : base+8])
		pid := utils.FromBytes[uint64](sl[base+8 : base+16])
		sn := utils.FromBytes[uint16](sl[base+16 : base+18])
		ext := utils.FromBytes[uint16](sl[base+18 : base+20])

		if ext == 0 {
			continue
		}

		rid := &common.RecordID{
			FileID:  common.FileID(fid),
			PageID:  common.PageID(pid),
			SlotNum: sn,
		}

		return rid, nil
	}

	return nil, ErrKeyNotFound
}

func (bp *bucketPage) delete(key []byte) (common.RecordID, error) {
	ns := bp.p.NumSlots()

	keyLen := uint16(len(key))

	// FileID + PageID + SlotNum + Existence
	const ridBytes = 8 + 8 + 2 + 2

	for i := uint16(1); i < ns; i++ {
		slot := bp.p.Read(i)
		if uint16(len(slot)) != keyLen+ridBytes {
			continue
		}

		if !bytes.Equal(slot[:keyLen], key) {
			continue
		}

		base := keyLen
		fid := utils.FromBytes[uint64](slot[base : base+8])
		pid := utils.FromBytes[uint64](slot[base+8 : base+16])
		sn := utils.FromBytes[uint16](slot[base+16 : base+18])
		ext := utils.FromBytes[uint16](slot[base+18 : base+20])

		if ext == 0 {
			continue
		}

		newExt := utils.ToBytes[uint16](0)
		copy(slot[base+18:base+20], newExt)

		//bp.p.Write(i, slot)

		rid := common.RecordID{
			FileID:  common.FileID(fid),
			PageID:  common.PageID(pid),
			SlotNum: sn,
		}

		return rid, nil
	}

	return common.RecordID{}, ErrKeyNotFound
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

func (h *Index) Insert(key []byte, rid common.RecordID) (common.RecordID, error) {
	panic("not implemented")
}
