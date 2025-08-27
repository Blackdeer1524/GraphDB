package hash

import (
	"errors"
	"fmt"
	"hash/fnv"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
)

const (
	indexRootPageID         uint64 = 0
	overflowPageNotExist           = ^uint64(0)
	metaPageDirPagesCount          = 100
	dirPageBucketsPtrsCount        = 100
)

type StorageEngine interface {
	GetPage(pageID uint64, fileID uint64) (*page.SlottedPage, error)
	UnpinPage(pageID uint64, fileID uint64) error
	WritePage(pageID uint64, fileID uint64, page page.SlottedPage) error
}

type Index struct {
	se StorageEngine

	indexFileID uint64

	keySize int
}

func New(indexFileID uint64, se StorageEngine, index int) (*Index, error) {
	return &Index{
		se:      se,
		keySize: index,
	}, nil
}

func hashKeyToUint64(key []byte) uint64 {
	hasher := fnv.New64a()
	_, _ = hasher.Write(key)

	return hasher.Sum64()
}

type rootPage struct {
	p              *page.SlottedPage
	d              uint64
	overflowPageID uint64
}

type metaPage struct {
	p *page.SlottedPage
}

type dirPage struct {
	p *page.SlottedPage
}

type bucketPage struct {
	p *page.SlottedPage
	l uint64
}

func (bp *bucketPage) find(key []byte) (common.RecordID, error) {
	return common.RecordID{}, nil
}

func (h *Index) getRootPage() (*rootPage, error) {
	panic("not implemented")
}

func (h *Index) getMetaPage(rp *rootPage, num uint64) (*metaPage, error) {
	panic("not implemented")
}

func (h *Index) getDirPage(mp *metaPage, num uint64) (*dirPage, error) {
	panic("not implemented")
}

func (h *Index) getBucketPage(dp *dirPage, num uint64) (*bucketPage, error) {
	panic("not implemented")
}

func (h *Index) Get(key []byte) (common.RecordID, error) {
	if len(key) != h.keySize {
		return common.RecordID{}, errors.New("key size is not equal to index key size")
	}

	rp, err := h.getRootPage()
	if err != nil {
		return common.RecordID{}, fmt.Errorf("failed to get root page: %w", err)
	}

	keyHash := hashKeyToUint64(key)

	index := keyHash & ((1 << rp.d) - 1)

	// calc an index of meta page
	mpIndex := index / (metaPageDirPagesCount * dirPageBucketsPtrsCount)

	mp, err := h.getMetaPage(rp, mpIndex)
	if err != nil {
		return common.RecordID{}, fmt.Errorf("failed to get meta page: %w", err)
	}

	mpOffset := index % (metaPageDirPagesCount * dirPageBucketsPtrsCount)

	dirSlot := mpOffset / dirPageBucketsPtrsCount
	bucketSlot := mpOffset % dirPageBucketsPtrsCount

	dp, err := h.getDirPage(mp, dirSlot)
	if err != nil {
		return common.RecordID{}, fmt.Errorf("failed to get dir page: %w", err)
	}

	bucketPtr, err := h.getBucketPage(dp, bucketSlot)
	if err != nil {
		return common.RecordID{}, fmt.Errorf("failed to get bucket ptr: %w", err)
	}

	rid, err := bucketPtr.find(key)
	if err != nil {
		return common.RecordID{}, fmt.Errorf("failed to find key: %w", err)
	}

	return rid, nil
}

func (h *Index) Delete(key []byte) (common.RecordID, error) {
	panic("not implemented")
}

func (h *Index) Insert(key []byte, rid common.RecordID) (common.RecordID, error) {
	panic("not implemented")
}
