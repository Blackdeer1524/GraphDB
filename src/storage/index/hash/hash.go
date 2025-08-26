package hash

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
)

const (
	indexRootPageID uint64 = 0
)

type HashIndex struct {
}

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

func (h *Index) Get(key []byte) (common.RecordID, error) {
	if len(key) != h.keySize {
		return common.RecordID{}, errors.New("key size is not equal to index key size")
	}

	// 1. Получаем root page
	rootPage, err := h.se.GetPage(indexRootPageID, h.indexFileID)
	if err != nil {
		return common.RecordID{}, fmt.Errorf("failed to get root page: %w", err)
	}
	defer func() {
		_ = h.se.UnpinPage(indexRootPageID, h.indexFileID)
	}()

	root := decodeRootPage(rootPage.GetData())
	hashVal := h.hashKey(key)

	// 3. Берём d младших бит
	dirIdx := hashVal & ((1 << root.GlobalDepth) - 1)

	dirPageID := root.DirectoryPointers[dirIdx/root.DirPageFanout]
	offset := dirIdx % root.DirPageFanout

	dirPage, err := h.se.GetPage(dirPageID, h.indexFileID)
	if err != nil {
		return common.RecordID{}, fmt.Errorf("failed to get directory page: %w", err)
	}
	defer func() {
		_ = h.se.UnpinPage(dirPageID, h.indexFileID)
	}()

	directory := decodeDirectoryPage(dirPage.Data())
	bucketPageID := directory.BucketPointers[offset]

	bucketPage, err := h.se.GetPage(bucketPageID, h.indexFileID)
	if err != nil {
		return common.RecordID{}, fmt.Errorf("failed to get bucket page: %w", err)
	}
	defer func() {
		_ = h.se.UnpinPage(bucketPageID, h.indexFileID)
	}()

	bucket := decodeBucketPage(bucketPage.Data())
	for _, entry := range bucket.Entries {
		if bytes.Equal(entry.Key, key) {
			return entry.RID, nil
		}
	}

	return common.RecordID{}, errors.New("key not found")
}
