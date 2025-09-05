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

type VertexInternalID uuid.UUID
type EdgeInternalID uuid.UUID
type DirItemInternalID uuid.UUID

var NilVertexID = VertexInternalID(uuid.Nil)
var NilEdgeID = EdgeInternalID(uuid.Nil)
var NilDirItemID = DirItemInternalID(uuid.Nil)

const (
	VertexInternalIDSize  = uint64(unsafe.Sizeof(VertexInternalID{}))
	EdgeInternalIDSize    = uint64(unsafe.Sizeof(EdgeInternalID{}))
	DirItemInternalIDSize = uint64(unsafe.Sizeof(DirItemInternalID{}))
)

func (v VertexInternalID) IsNil() bool {
	return v == NilVertexID
}

func (v VertexInternalID) MarshalBinary() ([]byte, error) {
	return uuid.UUID(v).MarshalBinary()
}

func (v *VertexInternalID) UnmarshalBinary(data []byte) error {
	return (*uuid.UUID)(v).UnmarshalBinary(data)
}

func (e EdgeInternalID) IsNil() bool {
	return e == NilEdgeID
}

func (v EdgeInternalID) MarshalBinary() ([]byte, error) {
	return uuid.UUID(v).MarshalBinary()
}

func (v *EdgeInternalID) UnmarshalBinary(data []byte) error {
	return (*uuid.UUID)(v).UnmarshalBinary(data)
}

func (d DirItemInternalID) IsNil() bool {
	return d == NilDirItemID
}

func (v DirItemInternalID) MarshalBinary() ([]byte, error) {
	return uuid.UUID(v).MarshalBinary()
}

func (v *DirItemInternalID) UnmarshalBinary(data []byte) error {
	return (*uuid.UUID)(v).UnmarshalBinary(data)
}

type VertexID struct {
	ID      VertexInternalID
	TableID common.FileID
}

type EdgeID struct {
	ID      EdgeInternalID
	TableID common.FileID
}

type VertexInternalIDWithDepthAndRID struct {
	V VertexInternalID
	D uint32
	R common.RecordID
}

type VertexInternalIDWithRID struct {
	V VertexInternalID
	R common.RecordID
}

type EdgeInternalIDWithRID struct {
	E EdgeInternalID
	R common.RecordID
}

type DirItemInternalIDWithRID struct {
	D DirItemInternalID
	R common.RecordID
}

func (v *VertexInternalIDWithRID) MarshalBinary() ([]byte, error) {
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

func (v *VertexInternalIDWithRID) UnmarshalBinary(data []byte) error {
	if err := v.V.UnmarshalBinary(data[:VertexInternalIDSize]); err != nil {
		return err
	}

	rBytes := data[VertexInternalIDSize:]
	if err := (&v.R).UnmarshalBinary(rBytes); err != nil {
		return err
	}

	return nil
}

var ErrQueueEmpty = errors.New("queue is empty")

type NeighborIDIter interface {
	Seq() iter.Seq[utils.Pair[VertexInternalIDWithRID, error]]
	Close() error
}

type NeighborEdgesIter interface {
	Seq() iter.Seq[utils.Triple[common.RecordID, Edge, error]]
	Close() error
}

type Queue interface {
	Enqueue(v VertexInternalIDWithDepthAndRID) error
	Dequeue() (VertexInternalIDWithDepthAndRID, error)

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
	Seq() iter.Seq[utils.Triple[common.RecordID, Vertex, error]]
	Close() error
}

type Edge struct {
	EdgeInternalFields
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

type StorageEngine interface {
	// Data structures
	NewAggregationAssociativeArray(
		common.TxnID,
	) (AssociativeArray[VertexInternalID, float64], error)
	NewBitMap(common.TxnID) (BitMap, error)
	NewQueue(common.TxnID) (Queue, error)

	// Graph traversals
	GetAllVertices(t common.TxnID, vertTableToken *txns.FileLockToken) (VerticesIter, error)
	Neighbours(
		txnID common.TxnID,
		vID VertexInternalID,
		vertTableToken *txns.FileLockToken,
		vertIndex Index,
		logger common.ITxnLoggerWithContext,
	) (NeighborIDIter, error)
	AllVerticesWithValue(
		t common.TxnID,
		vertTableToken *txns.FileLockToken,
		vertIndex Index,
		logger common.ITxnLoggerWithContext,
		field string,
		value []byte,
	) (VerticesIter, error)
	CountOfFilteredEdges(
		t common.TxnID,
		v VertexInternalID,
		vertTableToken *txns.FileLockToken,
		vertIndex Index,
		logger common.ITxnLoggerWithContext,
		filter EdgeFilter,
	) (uint64, error)
	GetNeighborsWithEdgeFilter(
		t common.TxnID,
		v VertexInternalID,
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
		keyBytesCnt uint32,
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
		keyBytesCnt uint32,
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
	GetDirTableInternalIndex(
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
	GetEdgeTableInternalIndex(
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
	GetVertexTableInternalIndex(
		txnID common.TxnID,
		vertexTableFileID common.FileID,
		cToken *txns.CatalogLockToken,
		logger common.ITxnLoggerWithContext,
	) (Index, error)

	GetEdgeTableInternalIndexMeta(
		edgeTableFileID common.FileID,
		cToken *txns.CatalogLockToken,
	) (IndexMeta, error)
	GetEdgeTableMeta(name string, cToken *txns.CatalogLockToken) (EdgeTableMeta, error)
	GetEdgeTableMetaByFileID(
		edgeTableID common.FileID,
		cToken *txns.CatalogLockToken,
	) (EdgeTableMeta, error)
	GetDirTableInternalIndexMeta(
		dirTableFileID common.FileID,
		cToken *txns.CatalogLockToken,
	) (IndexMeta, error)
	GetDirTableMeta(
		cToken *txns.CatalogLockToken,
		vertexTableFileID common.FileID,
	) (DirTableMeta, error)
	GetEdgeIndexMeta(name string, cToken *txns.CatalogLockToken) (IndexMeta, error)
	GetVertexTableInternalIndexMeta(
		vertexTableFileID common.FileID,
		cToken *txns.CatalogLockToken,
	) (IndexMeta, error)
	GetVertexTableIndexMeta(name string, cToken *txns.CatalogLockToken) (IndexMeta, error)
	GetVertexTableMeta(name string, cToken *txns.CatalogLockToken) (VertexTableMeta, error)
	GetVertexTableMetaByFileID(
		vertexTableID common.FileID,
		cToken *txns.CatalogLockToken,
	) (EdgeTableMeta, error)

	// DML
	InsertEdge(
		txnID common.TxnID,
		srcVertexID VertexInternalID,
		dstVertexID VertexInternalID,
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
		data map[string]any,
		schema Schema,
		vertexFileToken *txns.FileLockToken,
		vertexIndex Index,
		ctxLogger common.ITxnLoggerWithContext,
	) error
	SelectEdge(
		txnID common.TxnID,
		edgeID EdgeInternalID,
		edgeFileToken *txns.FileLockToken,
		edgeSystemIndex Index,
		schema Schema,
	) (EdgeInternalFields, map[string]any, error)
	SelectVertex(
		txnID common.TxnID,
		vertexID VertexInternalID,
		vertexIndex Index,
		schema Schema,
	) (VertexInternalFields, map[string]any, error)
	UpdateEdge(
		txnID common.TxnID,
		edgeID EdgeInternalID,
		edgeFields map[string]any,
		edgesFileToken *txns.FileLockToken,
		edgeSystemIndex Index,
		schema Schema,
		ctxLogger common.ITxnLoggerWithContext,
	) error
	UpdateVertex(
		txnID common.TxnID,
		vertexID VertexInternalID,
		newData map[string]any,
		schema Schema,
		vertexFileToken *txns.FileLockToken,
		vertexIndex Index,
		ctxLogger common.ITxnLoggerWithContext,
	) error
	DeleteEdge(
		txnID common.TxnID,
		edgeID EdgeInternalID,
		edgesFileToken *txns.FileLockToken,
		edgeSystemIndex Index,
		dirFileToken *txns.FileLockToken,
		dirSystemIndex Index,
		ctxLogger common.ITxnLoggerWithContext,
	) error
	DeleteVertex(
		txnID common.TxnID,
		vertexID VertexInternalID,
		vertexFileToken *txns.FileLockToken,
		vertexIndex Index,
		ctxLogger common.ITxnLoggerWithContext,
	) error
}

type SystemCatalog interface {
	CurrentVersion() uint64
	GetBasePath() string
	GetNewFileID() common.FileID
	GetFileIDToPathMap() map[common.FileID]string

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

type Index interface {
	Get(key []byte) (common.RecordID, error)
	Delete(key []byte) error
	Insert(key []byte, rid common.RecordID) error
}
