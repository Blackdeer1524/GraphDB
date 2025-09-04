package storage

import (
	"encoding"
	"errors"
	"fmt"
	"iter"

	"github.com/google/uuid"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
)

type VertexID uuid.UUID
type EdgeID uuid.UUID
type DirItemID uuid.UUID

var NilVertexID = VertexID(uuid.Nil)
var NilEdgeID = EdgeID(uuid.Nil)
var NilDirItemID = DirItemID(uuid.Nil)

func (v VertexID) IsNil() bool {
	return v == NilVertexID
}

func (v VertexID) MarshalBinary() ([]byte, error) {
	return uuid.UUID(v).MarshalBinary()
}

func (v *VertexID) UnmarshalBinary(data []byte) error {
	return (*uuid.UUID)(v).UnmarshalBinary(data)
}

func (e EdgeID) IsNil() bool {
	return e == NilEdgeID
}

func (v EdgeID) MarshalBinary() ([]byte, error) {
	return uuid.UUID(v).MarshalBinary()
}

func (v *EdgeID) UnmarshalBinary(data []byte) error {
	return (*uuid.UUID)(v).UnmarshalBinary(data)
}

func (d DirItemID) IsNil() bool {
	return d == NilDirItemID
}

func (v DirItemID) MarshalBinary() ([]byte, error) {
	return uuid.UUID(v).MarshalBinary()
}

func (v *DirItemID) UnmarshalBinary(data []byte) error {
	return (*uuid.UUID)(v).UnmarshalBinary(data)
}

type VertexWithDepthAndRID struct {
	V VertexID
	D uint32
	R common.RecordID
}

type VertexIDWithRID struct {
	V VertexID
	R common.RecordID
}

type EdgeIDWithRID struct {
	E EdgeID
	R common.RecordID
}

type DirectoryIDWithRID struct {
	D DirItemID
	R common.RecordID
}

func (v *VertexIDWithRID) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (v *VertexIDWithRID) UnmarshalBinary(data []byte) error {
	return nil
}

var ErrQueueEmpty = errors.New("queue is empty")

type NeighborIter interface {
	Seq() iter.Seq[VertexIDWithRID]
	Close() error
}

type Queue interface {
	Enqueue(v VertexWithDepthAndRID) error
	Dequeue() (VertexWithDepthAndRID, error)

	Close() error
}

type RawQueue interface {
	Enqueue(v []byte) error
	Dequeue() ([]byte, error)
	Close() error
}

type BitMap interface {
	Get(v VertexID) (bool, error)
	Set(v VertexID, b bool) error
	Close() error
}

type TypedQueue[T interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}] struct {
	q RawQueue
}

func (tq *TypedQueue[T]) Enqueue(v T) error {
	b, err := v.MarshalBinary()
	if err != nil {
		return err
	}

	return tq.q.Enqueue(b)
}

func (tq *TypedQueue[T]) Dequeue() (T, error) {
	var zero T

	b, err := tq.q.Dequeue()
	if err != nil {
		return zero, fmt.Errorf("failed to dequeue: %w", err)
	}

	var obj T

	err = obj.UnmarshalBinary(b)
	if err != nil {
		return zero, fmt.Errorf("failed to unmarshal: %w", err)
	}

	return obj, nil
}

func (tq *TypedQueue[T]) Close() error {
	return tq.q.Close()
}

var ErrNoVertexesInGraph = errors.New("no vertexes")

type Vertex struct {
	VertexInternalFields
	Data map[string]any
}

type VerticesIter interface {
	Seq() iter.Seq[Vertex]
	Close() error
}

type Edge struct {
	EdgeInternalFields
	Data map[string]any
}

type EdgeFilter func(*Edge) bool

type SumNeighborAttributesFilter func(float64) bool

type AssociativeArray[K comparable, V any] interface {
	Get(k K) (V, bool)
	Set(k K, v V) error
	Seq(yield func(K, V) bool)
}

type StorageEngine interface {
	NewQueue(common.TxnID) (Queue, error)
	NewAggregationAssociativeArray(common.TxnID) (AssociativeArray[VertexID, float64], error)
	NewBitMap(common.TxnID) (BitMap, error)
	Neighbours(t common.TxnID, v VertexID, vertTableID common.FileID) (NeighborIter, error)

	AllVerticesWithValue(t common.TxnID, field string, value []byte) (VerticesIter, error)
	CountOfFilteredEdges(t common.TxnID, v VertexID, f EdgeFilter) (uint64, error)
	GetAllVertices(t common.TxnID, vertTableID common.FileID) (VerticesIter, error)
	GetNeighborsWithEdgeFilter(
		t common.TxnID,
		vertTableID common.FileID,
		v VertexID,
		filter EdgeFilter,
	) (VerticesIter, error)

	GetVertexRID(txnID common.TxnID, vertexID VertexID, vertexIndex Index) (VertexIDWithRID, error)
	GetEdgeRID(txnID common.TxnID, edgeID EdgeID, edgeIndex Index) (EdgeIDWithRID, error)

	CreateVertexTable(
		txnID common.TxnID,
		name string,
		schema Schema,
		logger common.ITxnLoggerWithContext,
	) error
	GetVertexTableInternalIndex(
		txnID common.TxnID,
		vertexTableID common.FileID,
		logger common.ITxnLoggerWithContext,
	) (Index, error)
	DropVertexTable(txnID common.TxnID, name string, logger common.ITxnLoggerWithContext) error

	CreateEdgeTable(
		txnID common.TxnID,
		name string,
		schema Schema,
		logger common.ITxnLoggerWithContext,
	) error
	GetEdgeTableInternalIndex(
		txnID common.TxnID,
		edgeTableID common.FileID,
		logger common.ITxnLoggerWithContext,
	) (Index, error)
	DropEdgeTable(txnID common.TxnID, name string, logger common.ITxnLoggerWithContext) error

	GetDirectoryIndex(
		txnID common.TxnID,
		indexName string,
		logger common.ITxnLoggerWithContext,
	) (Index, error)

	CreateVertexTableIndex(
		txnID common.TxnID,
		indexName string,
		tableName string,
		columns []string,
		keyBytesCnt uint32,
		logger common.ITxnLoggerWithContext,
	) error
	DropVertexTableIndex(
		txnID common.TxnID,
		indexName string,
		logger common.ITxnLoggerWithContext,
	) error

	CreateEdgesTableIndex(
		txnID common.TxnID,
		indexName string,
		tableName string,
		columns []string,
		keyBytesCnt uint32,
		logger common.ITxnLoggerWithContext,
	) error
	DropEdgesTableIndex(
		txnID common.TxnID,
		indexName string,
		logger common.ITxnLoggerWithContext,
	) error
}

type SystemCatalog interface {
	GetBasePath() string
	GetNewFileID() common.FileID
	GetFileIDToPathMap() map[common.FileID]string

	AddDirIndex(index IndexMeta) error
	AddDirTable(req DirTableMeta) error
	AddEdgeIndex(index IndexMeta) error
	AddEdgeTable(req EdgeTableMeta) error
	AddVertexIndex(index IndexMeta) error
	AddVertexTable(req VertexTableMeta) error
	DirIndexExists(name string) (bool, error)
	DirTableExists(vertexTableID common.FileID) (bool, error)
	DropDirIndex(name string) error
	DropDirTable(vertexTableID common.FileID) error
	DropEdgeIndex(name string) error
	DropEdgeTable(name string) error
	DropVertexIndex(name string) error
	DropVertexTable(name string) error
	EdgeIndexExists(name string) (bool, error)
	EdgeTableExists(name string) (bool, error)
	GetDirIndexMeta(name string) (IndexMeta, error)
	GetDirTableMeta(vertexTableID common.FileID) (DirTableMeta, error)
	GetEdgeIndexMeta(name string) (IndexMeta, error)
	GetEdgeTableIndexes(name string) ([]IndexMeta, error)
	GetEdgeTableMeta(name string) (EdgeTableMeta, error)
	GetEdgeTableNameByFileID(fileID common.FileID) (string, error)
	GetVertexIndexMeta(name string) (IndexMeta, error)
	GetVertexTableIndexes(name string) ([]IndexMeta, error)
	GetVertexTableMeta(name string) (VertexTableMeta, error)
	GetVertexTableNameByFileID(fileID common.FileID) (string, error)
	VertexIndexExists(name string) (bool, error)
	VertexTableExists(name string) (bool, error)

	Save(logger common.ITxnLoggerWithContext) (err error)
}

type Index interface {
	Get(key []byte) (common.RecordID, error)
	Delete(key []byte) error
	Insert(key []byte, rid common.RecordID) error
}
