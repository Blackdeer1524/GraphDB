package hash

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
)

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

type rootMetaData struct {
	magic          [4]byte
	globalDepth    uint32
	dirPagesCount  uint32
	directoryPages []uint64

	checksum uint64
}

type directoryPageData struct {
	count       uint32
	bucketPages []uint64
}

func getRootPageMetadata(data []byte) rootMetaData {
	assert.Assert(len(data) == page.PageSize)

	var res rootMetaData

	copy(res.magic[:], data[0:4])
	res.globalDepth = binary.LittleEndian.Uint32(data[4:])
	res.dirPagesCount = binary.LittleEndian.Uint32(data[8:])
	res.checksum = binary.LittleEndian.Uint64(data[page.PageSize-8:])

	res.directoryPages = make([]uint64, 0, res.dirPagesCount)

	l := 12
	for i := 0; i < int(res.dirPagesCount); i++ {
		res.directoryPages = append(res.directoryPages, binary.LittleEndian.Uint64(data[l:]))
		l += 8
	}

	return res
}

func rootPageToBytes(m rootMetaData) []byte {
	assert.Assert(len(m.directoryPages) == int(m.dirPagesCount))

	maxDirPages := (page.PageSize - 16) / 8
	assert.Assert(len(m.directoryPages) <= maxDirPages)

	data := make([]byte, page.PageSize)

	copy(data[0:4], m.magic[:])
	binary.LittleEndian.PutUint32(data[4:], m.globalDepth)
	binary.LittleEndian.PutUint32(data[8:], m.dirPagesCount)
	binary.LittleEndian.PutUint64(data[page.PageSize-8:], m.checksum)

	l := 12
	for _, v := range m.directoryPages {
		binary.LittleEndian.PutUint64(data[l:], v)
		l += 8
	}

	return data
}

func getDirectoryPage(data []byte) directoryPageData {
	assert.Assert(len(data) == page.PageSize)

	var res directoryPageData

	res.count = binary.LittleEndian.Uint32(data[0:])

	res.bucketPages = make([]uint64, 0, res.count)

	l := 4

	for i := 0; i < int(res.count); i++ {
		res.bucketPages = append(res.bucketPages, binary.LittleEndian.Uint64(data[l:]))
		l += 8
	}

	return res
}

func directoryPageToBytes(data directoryPageData) []byte {
	assert.Assert(len(data.bucketPages) == int(data.count))

	buf := make([]byte, page.PageSize)

	// Записываем количество
	binary.LittleEndian.PutUint32(buf[0:], data.count)

	// Записываем ссылки на бакеты
	l := 4
	for _, v := range data.bucketPages {
		binary.LittleEndian.PutUint64(buf[l:], v)
		l += 8
	}

	// Остаток страницы остаётся нулями
	return buf
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
