package mocks

import (
	"fmt"
	"iter"

	"github.com/stretchr/testify/mock"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/storage/datastructures/inmemory"
)

// Mock implementations

// mockQueue is a simple in-memory queue for testing.
type mockQueue struct {
	items []storage.VertexWithDepthAndRID
}

func (m *mockQueue) Enqueue(v storage.VertexWithDepthAndRID) error {
	m.items = append(m.items, v)
	return nil
}

func (m *mockQueue) Dequeue() (storage.VertexWithDepthAndRID, error) {
	if len(m.items) == 0 {
		return storage.VertexWithDepthAndRID{}, storage.ErrQueueEmpty
	}
	v := m.items[0]
	m.items = m.items[1:]
	return v, nil
}

func (m *mockQueue) Close() error {
	return nil
}

// mockVisited is a simple map-based visited set.
type mockVisited struct {
	visited map[storage.VertexID]bool
}

func newMockVisited() *mockVisited {
	return &mockVisited{visited: make(map[storage.VertexID]bool)}
}

func (m *mockVisited) Get(v storage.VertexID) (bool, error) {
	val, ok := m.visited[v]
	return ok && val, nil
}

func (m *mockVisited) Set(v storage.VertexID, val bool) error {
	m.visited[v] = val
	return nil
}

func (m *mockVisited) Close() error {
	return nil
}

// mockIterator for neighbors.
type mockIterator struct {
	vertices []storage.VertexIDWithRID
	index    int
}

func newMockIterator(vertices []storage.VertexIDWithRID) *mockIterator {
	return &mockIterator{vertices: vertices, index: -1}
}

func (m *mockIterator) Next() (bool, error) {
	m.index++
	return m.index < len(m.vertices), nil
}

func (m *mockIterator) Vertex() storage.VertexIDWithRID {
	if m.index >= 0 && m.index < len(m.vertices) {
		return m.vertices[m.index]
	}
	return storage.VertexIDWithRID{}
}

func (m *mockIterator) Close() error {
	return nil
}

type MockAllVerticesIter struct {
	SeqF   func(yield func(*storage.Vertex) bool)
	CloseF error
}

func (m *MockAllVerticesIter) Seq() iter.Seq[*storage.Vertex] {
	return m.SeqF
}

func (m *MockAllVerticesIter) Close() error {
	return m.CloseF
}

type mockNeighborsIterator struct {
	items []*storage.VertexIDWithRID
	idx   int
}

func newNeighborsMockIterator(items []storage.VertexIDWithRID) storage.NeighborIter {
	ptrs := make([]*storage.VertexIDWithRID, len(items))
	for i := range items {
		ptrs[i] = &items[i]
	}

	return &mockNeighborsIterator{items: ptrs}
}

func (m *mockNeighborsIterator) Seq() iter.Seq[*storage.VertexIDWithRID] {
	return func(yield func(*storage.VertexIDWithRID) bool) {
		for _, it := range m.items {
			if !yield(it) {
				return
			}
		}
	}
}

func (m *mockNeighborsIterator) Close() error {
	return nil
}

// MockTransactionManager
type MockTransactionManager struct {
	BeginErr    error
	CommitErr   error
	rollbackErr error
	NextTxnID   common.TxnID
}

func (m *MockTransactionManager) Begin() (common.TxnID, error) {
	if m.BeginErr != nil {
		return 0, m.BeginErr
	}
	m.NextTxnID++
	return m.NextTxnID, nil
}

func (m *MockTransactionManager) CommitTx(_ common.TxnID) error {
	return m.CommitErr
}

func (m *MockTransactionManager) RollbackTx(_ common.TxnID) error {
	return m.rollbackErr
}

type MockTxnManager struct {
	mock.Mock
}

func (m *MockTxnManager) Begin() (common.TxnID, error) {
	args := m.Called()

	return args.Get(0).(common.TxnID), args.Error(1)
}
func (m *MockTxnManager) CommitTx(tx common.TxnID) error {
	args := m.Called(tx)

	return args.Error(0)
}
func (m *MockTxnManager) RollbackTx(tx common.TxnID) error {
	args := m.Called(tx)
	return args.Error(0)
}

type DataMockStorageEngine struct {
	vertices     map[storage.VertexID]common.RecordID
	neighbors    map[storage.VertexID][]storage.VertexIDWithRID
	queueErr     error
	bitMapErr    error
	getRIDErr    error
	neighborsErr error
}

// NewDataMockStorageEngine создаёт DataMockStorageEngine с заданным графом.
func NewDataMockStorageEngine(
	vertices []storage.VertexID,
	edges [][]storage.VertexID,
	neighborsErr, queueErr, bitMapErr, getRIDErr error,
) *DataMockStorageEngine {
	se := &DataMockStorageEngine{
		vertices:     make(map[storage.VertexID]common.RecordID),
		neighbors:    make(map[storage.VertexID][]storage.VertexIDWithRID),
		neighborsErr: neighborsErr,
		queueErr:     queueErr,
		bitMapErr:    bitMapErr,
		getRIDErr:    getRIDErr,
	}
	for _, v := range vertices {
		se.vertices[v] = common.RecordID{PageID: common.PageID(v * 100)}
	}
	for _, edge := range edges {
		if len(edge) != 2 {
			panic("edge must be a pair [u, v]")
		}
		u, v := edge[0], edge[1]
		se.neighbors[u] = append(
			se.neighbors[u],
			storage.VertexIDWithRID{V: v, R: common.RecordID{PageID: common.PageID(v * 100)}},
		)
		se.neighbors[v] = append(
			se.neighbors[v],
			storage.VertexIDWithRID{V: u, R: common.RecordID{PageID: common.PageID(u * 100)}},
		)
	}
	return se
}

func (m *DataMockStorageEngine) NewAggregationAssociativeArray(
	common.TxnID,
) (storage.AssociativeArray[storage.VertexID, float64], error) {
	return inmemory.NewInMemoryAssociativeArray[storage.VertexID, float64](), nil
}

func (m *DataMockStorageEngine) GetNeighborsWithEdgeFilter(
	t common.TxnID,
	v storage.VertexID,
	filter storage.EdgeFilter,
) (storage.VerticesIter, error) {
	return &MockAllVerticesIter{}, nil
}

func (m *DataMockStorageEngine) GetAllVertices(t common.TxnID) (storage.VerticesIter, error) {
	vertices := make([]*storage.Vertex, 0, len(m.vertices))
	for v := range m.vertices {
		vertices = append(vertices, &storage.Vertex{ID: v})
	}

	n := &MockAllVerticesIter{}

	n.SeqF = func(yield func(*storage.Vertex) bool) {
		for _, v := range vertices {
			if !yield(v) {
				return
			}
		}
	}

	return n, nil
}

func (m *DataMockStorageEngine) CountOfFilteredEdges(
	t common.TxnID,
	v storage.VertexID,
	f storage.EdgeFilter,
) (uint64, error) {
	return 0, nil
}

func (m *DataMockStorageEngine) AllVerticesWithValue(
	t common.TxnID,
	field string,
	value []byte,
) (storage.VerticesIter, error) {
	return &MockAllVerticesIter{}, nil
}

func (m *DataMockStorageEngine) NewQueue(_ common.TxnID) (storage.Queue, error) {
	if m.queueErr != nil {
		return nil, m.queueErr
	}
	return &mockQueue{}, nil
}

func (m *DataMockStorageEngine) NewBitMap(_ common.TxnID) (storage.BitMap, error) {
	if m.bitMapErr != nil {
		return nil, m.bitMapErr
	}
	return newMockVisited(), nil
}

func (m *DataMockStorageEngine) GetVertexRID(
	_ common.TxnID,
	v storage.VertexID,
) (storage.VertexIDWithRID, error) {
	if m.getRIDErr != nil {
		return storage.VertexIDWithRID{}, m.getRIDErr
	}
	rid, ok := m.vertices[v]
	if !ok {
		return storage.VertexIDWithRID{}, fmt.Errorf("vertex not found")
	}
	return storage.VertexIDWithRID{V: v, R: rid}, nil
}

func (m *DataMockStorageEngine) Neighbours(
	_ common.TxnID,
	v storage.VertexID,
) (storage.NeighborIter, error) {
	if m.neighborsErr != nil {
		return nil, m.neighborsErr
	}
	neighs, ok := m.neighbors[v]
	if !ok {
		return newNeighborsMockIterator(nil), nil
	}
	return newNeighborsMockIterator(neighs), nil
}

// MockStorageEngine использует mock.Mock для всех методов.
type MockStorageEngine struct {
	mock.Mock
}

func (m *MockStorageEngine) NewAggregationAssociativeArray(
	t common.TxnID,
) (storage.AssociativeArray[storage.VertexID, float64], error) {
	args := m.Called(t)
	return args.Get(0).(storage.AssociativeArray[storage.VertexID, float64]), args.Error(1)
}

func (m *MockStorageEngine) GetNeighborsWithEdgeFilter(
	t common.TxnID,
	v storage.VertexID,
	filter storage.EdgeFilter,
) (storage.VerticesIter, error) {
	args := m.Called(t, v, filter)
	return args.Get(0).(storage.VerticesIter), args.Error(1)
}

func (m *MockStorageEngine) GetAllVertices(t common.TxnID) (storage.VerticesIter, error) {
	args := m.Called(t)
	return args.Get(0).(storage.VerticesIter), args.Error(1)
}

func (m *MockStorageEngine) CountOfFilteredEdges(
	t common.TxnID,
	v storage.VertexID,
	f storage.EdgeFilter,
) (uint64, error) {
	args := m.Called(t, v, f)
	return args.Get(0).(uint64), args.Error(1)
}

func (m *MockStorageEngine) AllVerticesWithValue(
	t common.TxnID,
	field string,
	value []byte,
) (storage.VerticesIter, error) {
	args := m.Called(t, field, value)
	return args.Get(0).(storage.VerticesIter), args.Error(1)
}

func (m *MockStorageEngine) NewQueue(t common.TxnID) (storage.Queue, error) {
	args := m.Called(t)
	return args.Get(0).(storage.Queue), args.Error(1)
}

func (m *MockStorageEngine) NewBitMap(t common.TxnID) (storage.BitMap, error) {
	args := m.Called(t)
	return args.Get(0).(storage.BitMap), args.Error(1)
}

func (m *MockStorageEngine) GetVertexRID(
	t common.TxnID,
	v storage.VertexID,
) (storage.VertexIDWithRID, error) {
	args := m.Called(t, v)
	return args.Get(0).(storage.VertexIDWithRID), args.Error(1)
}

func (m *MockStorageEngine) Neighbours(
	t common.TxnID,
	v storage.VertexID,
) (storage.NeighborIter, error) {
	args := m.Called(t, v)
	return args.Get(0).(storage.NeighborIter), args.Error(1)
}
