package storage

import (
	"fmt"
	"sync"

	common "github.com/Blackdeer1524/GraphDB/src/pkg/common"
	txns "github.com/Blackdeer1524/GraphDB/src/txns"
)

type InMemoryIndex struct {
	mu   sync.Mutex
	data map[string]common.RecordID
}

var _ Index = &InMemoryIndex{}

func NewInMemoryIndex() *InMemoryIndex {
	return &InMemoryIndex{
		data: make(map[string]common.RecordID),
		mu:   sync.Mutex{},
	}
}

func (i *InMemoryIndex) Get(key []byte) (common.RecordID, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	res, ok := i.data[string(key)]
	if !ok {
		return common.RecordID{}, fmt.Errorf("key not found")
	}

	return res, nil
}

func (i *InMemoryIndex) Delete(key []byte) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if _, ok := i.data[string(key)]; !ok {
		return fmt.Errorf("key not found")
	}

	delete(i.data, string(key))
	return nil
}

func (i *InMemoryIndex) Insert(key []byte, rid common.RecordID) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if _, ok := i.data[string(key)]; ok {
		return fmt.Errorf("key already exists")
	}

	i.data[string(key)] = rid
	return nil
}

type InMemoryIndexLoader struct {
	mu      sync.Mutex
	indices map[string]Index
}

func NewInMemoryIndexLoader() *InMemoryIndexLoader {
	return &InMemoryIndexLoader{
		indices: make(map[string]Index),
		mu:      sync.Mutex{},
	}
}

func (i *InMemoryIndexLoader) Load(
	indexMeta IndexMeta,
	locker *txns.LockManager,
	logger common.ITxnLoggerWithContext,
) (Index, error) {
	if _, ok := i.indices[indexMeta.Name]; !ok {
		return nil, fmt.Errorf("index not found")
	}
	return i.indices[indexMeta.Name], nil
}
