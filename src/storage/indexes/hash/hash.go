package hash

import (
	"encoding/binary"
	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
)

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
}

type rootMetaData struct {
	magic         [4]byte
	globalDepth   uint32
	maxBucketSize uint32
	directory     []uint64
	checksum      uint64
}

type Index[T Page] struct {
	se StorageEngine[T]

	root rootMetaData
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

func New[T Page](fst Page) (*Index[T], error) {
	return &Index[T]{}, nil
}

func (h *Index[T]) Search() {

}

func (h *Index[T]) Insert() {

}

func (h *Index[T]) Delete() {

}
