package storage

import (
	"encoding"
	"errors"
	"fmt"
	"iter"
	"unsafe"

	"github.com/google/uuid"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

type VertexSystemID uuid.UUID
type EdgeSystemID uuid.UUID
type DirItemSystemID uuid.UUID

var NilVertexID = VertexSystemID(uuid.Nil)
var NilEdgeID = EdgeSystemID(uuid.Nil)
var NilDirItemID = DirItemSystemID(uuid.Nil)

const (
	VertexSystemIDSize  = uint64(unsafe.Sizeof(VertexSystemID{}))
	EdgeSystemIDSize    = uint64(unsafe.Sizeof(EdgeSystemID{}))
	DirItemSystemIDSize = uint64(unsafe.Sizeof(DirItemSystemID{}))
)

func (v VertexSystemID) IsNil() bool {
	return v == NilVertexID
}

func (v VertexSystemID) MarshalBinary() ([]byte, error) {
	return uuid.UUID(v).MarshalBinary()
}

func (v *VertexSystemID) UnmarshalBinary(data []byte) error {
	return (*uuid.UUID)(v).UnmarshalBinary(data)
}

func (e EdgeSystemID) IsNil() bool {
	return e == NilEdgeID
}

func (v EdgeSystemID) MarshalBinary() ([]byte, error) {
	return uuid.UUID(v).MarshalBinary()
}

func (v *EdgeSystemID) UnmarshalBinary(data []byte) error {
	return (*uuid.UUID)(v).UnmarshalBinary(data)
}

func (d DirItemSystemID) IsNil() bool {
	return d == NilDirItemID
}

func (v DirItemSystemID) MarshalBinary() ([]byte, error) {
	return uuid.UUID(v).MarshalBinary()
}

func (v *DirItemSystemID) UnmarshalBinary(data []byte) error {
	return (*uuid.UUID)(v).UnmarshalBinary(data)
}

type VertexID struct {
	SystemID VertexSystemID
	TableID  common.FileID
}

type EdgeID struct {
	ID      EdgeSystemID
	TableID common.FileID
}

type VertexSystemIDWithDepthAndRID struct {
	V VertexSystemID
	D uint32
	R common.RecordID
}

type VertexSystemIDWithRID struct {
	V VertexSystemID
	R common.RecordID
}

type EdgeSystemIDWithRID struct {
	E EdgeSystemID
	R common.RecordID
}

type DirItemSystemIDWithRID struct {
	D DirItemSystemID
	R common.RecordID
}

func (v *VertexSystemIDWithRID) MarshalBinary() ([]byte, error) {
	vBytes, err := v.V.MarshalBinary()
	if err != nil {
		return nil, err
	}

	rBytes, err := v.R.MarshalBinary()
	if err != nil {
		return nil, err
	}

	result := make([]byte, len(vBytes)+len(rBytes))
	copy(result, vBytes)
	copy(result[len(vBytes):], rBytes)

	return result, nil
}

func (v *VertexSystemIDWithRID) UnmarshalBinary(data []byte) error {
	if err := v.V.UnmarshalBinary(data[:VertexSystemIDSize]); err != nil {
		return err
	}

	rBytes := data[VertexSystemIDSize:]
	if err := (&v.R).UnmarshalBinary(rBytes); err != nil {
		return err
	}

	return nil
}

var ErrQueueEmpty = errors.New("queue is empty")

type NeighborIDIter interface {
	Seq() iter.Seq[utils.Pair[VertexSystemIDWithRID, error]]
	Close() error
}

type NeighborEdgesIter interface {
	Seq() iter.Seq[utils.Triple[common.RecordID, Edge, error]]
	Close() error
}

type Queue interface {
	Enqueue(v VertexSystemIDWithDepthAndRID) error
	Dequeue() (VertexSystemIDWithDepthAndRID, error)

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
	VertexSystemFields
	Data map[string]any
}

type VerticesIter interface {
	Seq() iter.Seq[utils.Triple[common.RecordID, Vertex, error]]
	Close() error
}

type EdgesIter interface {
	Seq() iter.Seq[utils.Triple[common.RecordID, Edge, error]]
	Close() error
}

type DirItemsIter interface {
	Seq() iter.Seq[utils.Triple[common.RecordID, DirectoryItem, error]]
	Close() error
}

type Edge struct {
	EdgeSystemFields
	Data map[string]any
}

type EdgeFilter func(*Edge) bool

var AllowAllEdgesFilter EdgeFilter = func(_ *Edge) bool {
	return true
}

type VertexFilter func(*Vertex) bool

var AllowAllVerticesFilter VertexFilter = func(_ *Vertex) bool {
	return true
}

type SumNeighborAttributesFilter func(float64) bool

type AssociativeArray[K comparable, V any] interface {
	Get(k K) (V, bool)
	Set(k K, v V) error
	Seq(yield func(K, V) bool)
}

type Engine interface {
	// Data structures
	NewAggregationAssociativeArray(common.TxnID) (AssociativeArray[VertexID, float64], error)
	NewBitMap(common.TxnID) (BitMap, error)
	NewQueue(common.TxnID) (Queue, error)

	// Graph traversals
	GetAllVertices(txnID common.TxnID, vertTableToken *txns.FileLockToken) (VerticesIter, error)
	Neighbours(
		txnID common.TxnID,
		startVertSystemID VertexSystemID,
		startVertTableToken *txns.FileLockToken,
		startVertIndex Index,
		logger common.ITxnLoggerWithContext,
	) (NeighborIDIter, error)
	AllVerticesWithValue(
		txnID common.TxnID,
		vertTableToken *txns.FileLockToken,
		vertIndex Index,
		logger common.ITxnLoggerWithContext,
		field string,
		value []byte,
	) (VerticesIter, error)
	CountOfFilteredEdges(
		txnID common.TxnID,
		vertSystemID VertexSystemID,
		vertTableToken *txns.FileLockToken,
		vertIndex Index,
		logger common.ITxnLoggerWithContext,
		filter EdgeFilter,
	) (uint64, error)
	GetNeighborsWithEdgeFilter(
		txnID common.TxnID,
		v VertexSystemID,
		vertTableToken *txns.FileLockToken,
		vertIndex Index,
		edgeFilter EdgeFilter,
		logger common.ITxnLoggerWithContext,
	) (VerticesIter, error)

	// Schema management
	CreateEdgeTable(
		txnID common.TxnID,
		tableName string,
		schema Schema,
		srcVertexTableFileID common.FileID,
		dstVertexTableFileID common.FileID,
		cToken *txns.CatalogLockToken,
		logger common.ITxnLoggerWithContext,
	) error
	CreateEdgeTableIndex(
		txnID common.TxnID,
		indexName string,
		tableName string,
		columns []string,
		cToken *txns.CatalogLockToken,
		logger common.ITxnLoggerWithContext,
	) error
	CreateVertexTable(
		txnID common.TxnID,
		tableName string,
		schema Schema,
		cToken *txns.CatalogLockToken,
		logger common.ITxnLoggerWithContext,
	) error
	CreateVertexTableIndex(
		txnID common.TxnID,
		indexName string,
		tableName string,
		columns []string,
		cToken *txns.CatalogLockToken,
		logger common.ITxnLoggerWithContext,
	) error
	DropEdgeTable(
		txnID common.TxnID,
		name string,
		cToken *txns.CatalogLockToken,
		logger common.ITxnLoggerWithContext,
	) error
	DropEdgeTableIndex(
		txnID common.TxnID,
		indexName string,
		cToken *txns.CatalogLockToken,
		logger common.ITxnLoggerWithContext,
	) error
	DropVertexTable(
		txnID common.TxnID,
		vertTableName string,
		cToken *txns.CatalogLockToken,
		logger common.ITxnLoggerWithContext,
	) error
	DropVertexTableIndex(
		txnID common.TxnID,
		indexName string,
		cToken *txns.CatalogLockToken,
		logger common.ITxnLoggerWithContext,
	) error
	GetDirTableSystemIndex(
		txnID common.TxnID,
		dirTableFileID common.FileID,
		cToken *txns.CatalogLockToken,
		logger common.ITxnLoggerWithContext,
	) (Index, error)
	GetEdgeTableIndex(
		txnID common.TxnID,
		indexName string,
		cToken *txns.CatalogLockToken,
		logger common.ITxnLoggerWithContext,
	) (Index, error)
	GetEdgeTableSystemIndex(
		txnID common.TxnID,
		edgeTableFileID common.FileID,
		cToken *txns.CatalogLockToken,
		logger common.ITxnLoggerWithContext,
	) (Index, error)
	GetVertexTableIndex(
		txnID common.TxnID,
		indexName string,
		cToken *txns.CatalogLockToken,
		logger common.ITxnLoggerWithContext,
	) (Index, error)
	GetVertexTableSystemIndex(
		txnID common.TxnID,
		vertexTableFileID common.FileID,
		cToken *txns.CatalogLockToken,
		logger common.ITxnLoggerWithContext,
	) (Index, error)

	GetEdgeTableSystemIndexMeta(
		edgeTableFileID common.FileID,
		cToken *txns.CatalogLockToken,
	) (IndexMeta, error)
	GetEdgeTableMeta(name string, cToken *txns.CatalogLockToken) (EdgeTableMeta, error)
	GetEdgeTableMetaByFileID(
		edgeTableID common.FileID,
		cToken *txns.CatalogLockToken,
	) (EdgeTableMeta, error)
	GetDirTableSystemIndexMeta(
		dirTableFileID common.FileID,
		cToken *txns.CatalogLockToken,
	) (IndexMeta, error)
	GetDirTableMeta(
		cToken *txns.CatalogLockToken,
		vertexTableFileID common.FileID,
	) (DirTableMeta, error)
	GetEdgeIndexMeta(name string, cToken *txns.CatalogLockToken) (IndexMeta, error)
	GetVertexTableSystemIndexMeta(
		vertexTableFileID common.FileID,
		cToken *txns.CatalogLockToken,
	) (IndexMeta, error)
	GetVertexTableIndexMeta(name string, cToken *txns.CatalogLockToken) (IndexMeta, error)
	GetVertexTableMeta(name string, cToken *txns.CatalogLockToken) (VertexTableMeta, error)
	GetVertexTableMetaByFileID(
		vertexTableID common.FileID,
		cToken *txns.CatalogLockToken,
	) (VertexTableMeta, error)

	// DML
	InsertEdge(
		txnID common.TxnID,
		edgeSystemID EdgeSystemID,
		srcVertexID VertexSystemID,
		dstVertexID VertexSystemID,
		edgeFields map[string]any,
		schema Schema,
		srcVertToken *txns.FileLockToken,
		srcVertSystemIndex Index,
		srcVertDirToken *txns.FileLockToken,
		srcVertDirSystemIndex Index,
		edgesFileToken *txns.FileLockToken,
		edgeSystemIndex Index,
		ctxLogger common.ITxnLoggerWithContext,
	) error
	InsertVertex(
		txnID common.TxnID,
		vertexSystemID VertexSystemID,
		data map[string]any,
		schema Schema,
		vertexFileToken *txns.FileLockToken,
		vertexIndex Index,
		ctxLogger common.ITxnLoggerWithContext,
	) error
	SelectEdge(
		txnID common.TxnID,
		edgeID EdgeSystemID,
		edgeFileToken *txns.FileLockToken,
		edgeSystemIndex Index,
		schema Schema,
	) (EdgeSystemFields, map[string]any, error)
	SelectVertex(
		txnID common.TxnID,
		vertexID VertexSystemID,
		vertexIndex Index,
		schema Schema,
	) (VertexSystemFields, map[string]any, error)
	UpdateEdge(
		txnID common.TxnID,
		edgeID EdgeSystemID,
		edgeFields map[string]any,
		edgesFileToken *txns.FileLockToken,
		edgeSystemIndex Index,
		schema Schema,
		ctxLogger common.ITxnLoggerWithContext,
	) error
	UpdateVertex(
		txnID common.TxnID,
		vertexID VertexSystemID,
		newData map[string]any,
		schema Schema,
		vertexFileToken *txns.FileLockToken,
		vertexIndex Index,
		ctxLogger common.ITxnLoggerWithContext,
	) error
	DeleteEdge(
		txnID common.TxnID,
		edgeID EdgeSystemID,
		edgesFileToken *txns.FileLockToken,
		edgeSystemIndex Index,
		dirFileToken *txns.FileLockToken,
		dirSystemIndex Index,
		ctxLogger common.ITxnLoggerWithContext,
	) error
	DeleteVertex(
		txnID common.TxnID,
		vertexID VertexSystemID,
		vertexFileToken *txns.FileLockToken,
		vertexIndex Index,
		ctxLogger common.ITxnLoggerWithContext,
	) error
}

type SystemCatalog interface {
	CurrentVersion() uint64
	GetBasePath() string
	GetNewFileID() common.FileID

	Load() error
	CommitChanges(logger common.ITxnLoggerWithContext) (err error)

	AddDirIndex(index IndexMeta) error
	AddDirTable(req DirTableMeta) error
	AddEdgeIndex(index IndexMeta) error
	AddEdgeTable(req EdgeTableMeta) error
	AddVertexIndex(index IndexMeta) error
	AddVertexTable(req VertexTableMeta) error
	DropDirIndex(name string) error
	DropDirTable(vertexTableID common.FileID) error
	DropEdgeIndex(name string) error
	DropEdgeTable(name string) error
	DropVertexIndex(name string) error
	DropVertexTable(name string) error

	GetDirIndexMeta(name string) (IndexMeta, error)
	GetDirTableMeta(vertexTableID common.FileID) (DirTableMeta, error)
	GetEdgeIndexMeta(name string) (IndexMeta, error)
	GetEdgeTableIndexes(name string) ([]IndexMeta, error)
	GetEdgeTableMeta(name string) (EdgeTableMeta, error)
	GetEdgeTableNameByFileID(fileID common.FileID) (string, error)
	GetVertexTableIndexMeta(name string) (IndexMeta, error)
	GetVertexTableIndexes(name string) ([]IndexMeta, error)
	GetVertexTableMeta(name string) (VertexTableMeta, error)
	GetVertexTableNameByFileID(fileID common.FileID) (string, error)
	VertexIndexExists(name string) (bool, error)
	VertexTableExists(name string) (bool, error)
	EdgeIndexExists(name string) (bool, error)
	EdgeTableExists(name string) (bool, error)
	DirIndexExists(name string) (bool, error)
	DirTableExists(vertexTableID common.FileID) (bool, error)
}

var ErrKeyNotFound = errors.New("key not found")

type Index interface {
	Get(key []byte) (common.RecordID, error)
	Delete(key []byte) error
	Insert(key []byte, rid common.RecordID) error
	Close() error
}
