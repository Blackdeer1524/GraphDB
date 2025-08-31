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
	Seq() iter.Seq[*VertexIDWithRID]
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
	ID   VertexID
	Data map[string]any
}

type VerticesIter interface {
	Seq() iter.Seq[*Vertex]
	Close() error
}

type Edge struct {
	Src  VertexID
	Dst  VertexID
	Data map[string]any
}

type EdgeFilter func(*Edge) bool

type SumNeighborAttributesFilter func(float64) bool

type AssociativeArray[K comparable, V any] interface {
	Get(k K) (V, bool)
	Set(k K, v V) error
	Seq(yield func(K, V) bool)
}
