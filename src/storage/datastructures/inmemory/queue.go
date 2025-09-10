package inmemory

import "github.com/Blackdeer1524/GraphDB/src/storage"

type InMemoryQueue struct {
	data []storage.VertexSystemIDWithDepthAndRID
}

func (i *InMemoryQueue) Close() error {
	i.data = nil
	return nil
}

func (i *InMemoryQueue) Dequeue() (storage.VertexSystemIDWithDepthAndRID, error) {
	if len(i.data) == 0 {
		return storage.VertexSystemIDWithDepthAndRID{}, storage.ErrQueueEmpty
	}

	element := i.data[0]
	i.data = i.data[1:]
	return element, nil
}

func (i *InMemoryQueue) Enqueue(v storage.VertexSystemIDWithDepthAndRID) error {
	i.data = append(i.data, v)
	return nil
}

var _ storage.Queue = &InMemoryQueue{}

func NewInMemoryQueue() *InMemoryQueue {
	return &InMemoryQueue{
		data: make([]storage.VertexSystemIDWithDepthAndRID, 0),
	}
}
