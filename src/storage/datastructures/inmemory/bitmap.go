package inmemory

import "github.com/Blackdeer1524/GraphDB/src/storage"

type InMemoryBitMap struct {
	data map[storage.VertexID]bool
}

func (i *InMemoryBitMap) Close() error {
	i.data = nil
	return nil
}

func (i *InMemoryBitMap) Get(v storage.VertexID) (bool, error) {
	value, exists := i.data[v]
	return value && exists, nil
}

func (i *InMemoryBitMap) Set(v storage.VertexID, b bool) error {
	// Set the value in the map
	i.data[v] = b
	return nil
}

var _ storage.BitMap = &InMemoryBitMap{}

func NewInMemoryBitMap() *InMemoryBitMap {
	return &InMemoryBitMap{
		data: make(map[storage.VertexID]bool),
	}
}
